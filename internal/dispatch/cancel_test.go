package dispatch

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
)

func newHarness(t *testing.T) (*Planner, *Publisher, *Canceller, *Buffer, *station.TargetService, *station.CommandService, *audit.Query) {
	t.Helper()
	stations := station.NewRegistry([]station.Station{{
		ID: "s1", Name: "Station 1", Online: true, TargetCelsius: 60, TargetRevision: 1, ControllerEpoch: 1,
		Exchangers: map[string]station.ExchangerPhase{},
	}})
	targets := station.NewTargetService(stations)
	planner := NewPlanner(targets)
	buffer := NewBuffer()
	commands := station.NewCommandService(stations)
	eventLog := store.NewEventLog(t.TempDir() + "/events.jsonl")
	recorder := audit.NewRecorder(eventLog)
	publisher := NewPublisher(planner, targets, buffer, recorder)
	canceller := NewCanceller(planner, buffer, commands, recorder)
	return planner, publisher, canceller, buffer, targets, commands, audit.NewQuery(eventLog)
}

func beginPlan(t *testing.T, planner *Planner, requested map[string]float64) Plan {
	t.Helper()
	plan, err := planner.Begin(t.Context(), requested, time.Millisecond)
	if err != nil {
		t.Fatalf("begin plan: %v", err)
	}
	return plan
}

func TestCancelRevokesTargetsAndDiscardsBufferedCommands(t *testing.T) {
	planner, _, canceller, buffer, targets, _, query := newHarness(t)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})

	if _, err := buffer.EnqueueForPlan("s1", plan.ID, 5, 1, 70); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	result, err := canceller.Cancel(plan.ID, "operator")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if result.Status != PlanCanceled {
		t.Fatalf("status = %s, want canceled", result.Status)
	}
	if got := result.RevokedTargets["s1"]; got != 70 {
		t.Fatalf("revoked target = %v, want 70 snapshot", got)
	}
	if len(result.DiscardedCommands) != 1 || result.DiscardedCommands[0].PlanID != plan.ID {
		t.Fatalf("discarded = %+v, want the single queued command", result.DiscardedCommands)
	}
	if len(result.RetainedCommands) != 0 {
		t.Fatalf("retained = %+v, want none", result.RetainedCommands)
	}

	stored, ok := planner.Plan(plan.ID)
	if !ok {
		t.Fatal("plan disappeared")
	}
	if stored.Status != PlanCanceled || len(stored.Targets) != 0 {
		t.Fatalf("stored plan = %+v, want canceled with detached targets", stored)
	}
	if pending := buffer.Pending("s1"); len(pending) != 0 {
		t.Fatalf("pending after cancel = %+v, want empty", pending)
	}
	if rev := targets.Revision(); rev != 1 {
		t.Fatalf("target revision = %d, want unchanged 1", rev)
	}

	entries, err := query.Recent(audit.Filter{EntityID: plan.ID, Action: "dispatch.cancel"})
	if err != nil {
		t.Fatalf("query audit: %v", err)
	}
	if len(entries) != 1 || entries[0].Outcome != audit.OutcomeCompensated {
		t.Fatalf("audit entries = %+v, want one compensated record", entries)
	}
}

func TestCancelKeepsExecutedStationCommandsWithoutRollback(t *testing.T) {
	planner, _, canceller, buffer, _, commands, _ := newHarness(t)
	plan := beginPlan(t, planner, map[string]float64{"s1": 72})

	buffered, err := buffer.EnqueueForPlan("s1", plan.ID, 5, 1, 72)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Simulate reconnect draining and applying the command to the station.
	ready, _ := buffer.Drain("s1", 5)
	if len(ready) != 1 {
		t.Fatalf("drained %d commands, want 1", len(ready))
	}
	applied, err := commands.Apply(station.Command{
		ID: buffered.CommandID, StationID: "s1", PlanID: plan.ID,
		Epoch: buffered.Epoch, Sequence: buffered.Sequence, Celsius: buffered.Celsius,
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	result, err := canceller.Cancel(plan.ID, "operator")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if len(result.DiscardedCommands) != 0 {
		t.Fatalf("discarded = %+v, executed command must not be removed retroactively", result.DiscardedCommands)
	}
	if len(result.RetainedCommands) != 1 || result.RetainedCommands[0].ID != applied.ID {
		t.Fatalf("retained = %+v, want the executed command", result.RetainedCommands)
	}

	// The station keeps the command's target: cancellation must not forge a
	// rollback back to the pre-command value.
	state, _ := commands.Last("s1")
	if state.Celsius != 72 || state.Status != "applied" {
		t.Fatalf("station command = %+v, want applied 72 left in effect", state)
	}
}

func TestCancelIsIdempotent(t *testing.T) {
	planner, _, canceller, buffer, _, _, query := newHarness(t)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})
	if _, err := buffer.EnqueueForPlan("s1", plan.ID, 5, 1, 70); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first, err := canceller.Cancel(plan.ID, "operator")
	if err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	second, err := canceller.Cancel(plan.ID, "operator")
	if err != nil {
		t.Fatalf("retry cancel: %v", err)
	}
	if first.CompensatedAt != second.CompensatedAt {
		t.Fatalf("compensated at differs: %v vs %v", first.CompensatedAt, second.CompensatedAt)
	}
	if len(second.DiscardedCommands) != 1 {
		t.Fatalf("retry reports %d discarded, want cached 1", len(second.DiscardedCommands))
	}

	entries, err := query.Recent(audit.Filter{EntityID: plan.ID, Action: "dispatch.cancel"})
	if err != nil {
		t.Fatalf("query audit: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("audit entries = %d, want exactly one on retry", len(entries))
	}
}

func TestCancelAndPublishAreMutuallyExclusive(t *testing.T) {
	planner, publisher, canceller, _, _, _, _ := newHarness(t)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})

	if _, err := canceller.Cancel(plan.ID, "operator"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := publisher.Publish(plan.ID, "operator"); err == nil {
		t.Fatal("publish after cancel succeeded, want conflict")
	}

	other := beginPlan(t, planner, map[string]float64{"s1": 71})
	if _, err := publisher.Publish(other.ID, "operator"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := canceller.Cancel(other.ID, "operator"); err == nil {
		t.Fatal("cancel after publish succeeded, want conflict")
	}
}

func TestEnqueueRejectsClosedPlan(t *testing.T) {
	planner, _, canceller, buffer, _, _, _ := newHarness(t)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})
	if _, err := canceller.Cancel(plan.ID, "operator"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := buffer.EnqueueForPlan("s1", plan.ID, 5, 1, 70); err == nil {
		t.Fatal("enqueue after cancel succeeded")
	}
	if pending := buffer.Pending("s1"); len(pending) != 0 {
		t.Fatalf("pending = %+v, want empty", pending)
	}
}

func TestCancelUnknownPlan(t *testing.T) {
	_, _, canceller, _, _, _, _ := newHarness(t)
	if _, err := canceller.Cancel("missing", "operator"); err != ErrPlanNotFound {
		t.Fatalf("err = %v, want ErrPlanNotFound", err)
	}
}

func TestCancelRollsBackWhenAuditWriteFails(t *testing.T) {
	// Point the event log at a path whose parent is a regular file, so the
	// audit Append (which MkdirAlls the directory) fails deterministically.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("create blocker: %v", err)
	}
	stations := station.NewRegistry([]station.Station{{
		ID: "s1", Exchangers: map[string]station.ExchangerPhase{}, TargetRevision: 1, ControllerEpoch: 1,
	}})
	targets := station.NewTargetService(stations)
	planner := NewPlanner(targets)
	buffer := NewBuffer()
	commands := station.NewCommandService(stations)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(blocker, "events.jsonl")))
	canceller := NewCanceller(planner, buffer, commands, recorder)

	plan := beginPlan(t, planner, map[string]float64{"s1": 70})
	if _, err := buffer.EnqueueForPlan("s1", plan.ID, 5, 1, 70); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := canceller.Cancel(plan.ID, "operator"); err == nil {
		t.Fatal("cancel with failing audit succeeded")
	}
	stored, _ := planner.Plan(plan.ID)
	if stored.Status != PlanCalculating {
		t.Fatalf("plan status after failed cancel = %s, want calculating", stored.Status)
	}
	if got := stored.Targets["s1"]; got != 70 {
		t.Fatalf("targets after rollback = %v, want 70 restored", stored.Targets)
	}
	if pending := buffer.Pending("s1"); len(pending) != 1 {
		t.Fatalf("pending after rollback = %d, want command restored", len(pending))
	}
	if _, err := buffer.EnqueueForPlan("s1", plan.ID, 5, 2, 71); err != nil {
		t.Fatalf("plan should be open again after rollback: %v", err)
	}
}

func TestConcurrentCancelAndPublish(t *testing.T) {
	planner, publisher, canceller, buffer, targets, _, query := newHarness(t)
	plan := beginPlan(t, planner, map[string]float64{"s1": 70})
	if _, err := buffer.EnqueueForPlan("s1", plan.ID, 5, 1, 70); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	const goroutines = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	var published, canceled, picker atomic.Int32
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cancel := picker.Add(1)%2 == 0
			<-start
			for range 20 {
				if cancel {
					if _, err := canceller.Cancel(plan.ID, "operator"); err == nil {
						canceled.Add(1)
					}
				} else {
					if _, err := publisher.Publish(plan.ID, "operator"); err == nil {
						published.Add(1)
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if published.Load() > 0 && canceled.Load() > 0 {
		t.Fatalf("both outcomes observed: published=%d canceled=%d", published.Load(), canceled.Load())
	}
	stored, _ := planner.Plan(plan.ID)
	if stored.Status == PlanPublished && targets.Revision() != 2 {
		t.Fatalf("published plan did not bump revision exactly once, got %d", targets.Revision())
	}
	entries, err := query.Recent(audit.Filter{EntityID: plan.ID})
	if err != nil {
		t.Fatalf("query audit: %v", err)
	}
	var cancels int
	for _, entry := range entries {
		if entry.Action == "dispatch.cancel" {
			cancels++
		}
	}
	if cancels > 1 {
		t.Fatalf("cancel audited %d times, want at most 1", cancels)
	}
}
