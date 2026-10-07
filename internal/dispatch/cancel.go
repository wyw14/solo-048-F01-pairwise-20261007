package dispatch

import (
	"fmt"
	"sync"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
)

// CancelResult is the compensation record for a canceled plan. It is
// immutable once produced: repeating the same cancel request returns the
// exact same result instead of performing compensation twice.
type CancelResult struct {
	PlanID string     `json:"plan_id"`
	Status PlanStatus `json:"status"`
	// RevokedTargets is the snapshot of the targets that had never been
	// published to stations; they are detached from the plan and never
	// enter service, while this record preserves the snapshot.
	RevokedTargets map[string]float64 `json:"revoked_targets"`
	// DiscardedCommands were still queued in the dispatch buffer and never
	// reached a station, so they can be safely removed.
	DiscardedCommands []BufferedCommand `json:"discarded_commands"`
	// RetainedCommands were already applied to stations before cancellation.
	// They stay in effect on purpose: executed site commands are never
	// fabricated into a rollback.
	RetainedCommands []station.Command `json:"retained_commands"`
	CompensatedAt    time.Time         `json:"compensated_at"`
}

type Canceller struct {
	planner  *Planner
	buffer   *Buffer
	commands *station.CommandService
	audit    *audit.Recorder

	// mu serializes cancellations so concurrent retries cannot compensate
	// twice; the resulting record is cached for idempotent replays.
	mu      sync.Mutex
	results map[string]CancelResult
}

func NewCanceller(planner *Planner, buffer *Buffer, commands *station.CommandService, recorder *audit.Recorder) *Canceller {
	return &Canceller{planner: planner, buffer: buffer, commands: commands, audit: recorder, results: make(map[string]CancelResult)}
}

func (c *Canceller) Cancel(planID, actor string) (CancelResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if result, ok := c.results[planID]; ok {
		return result, nil
	}

	// Atomically claim the plan against a concurrent publish: both
	// transitions serialize under the planner lock, so a published plan is
	// never compensated and a canceled plan is never published.
	compensatedAt := time.Now().UTC()
	revoked := map[string]float64{}
	plan, err := c.planner.commit(planID, func(plan *Plan) error {
		if plan.Status != PlanCalculating {
			return fmt.Errorf("plan %s is %s", plan.ID, plan.Status)
		}
		for stationID, celsius := range plan.Targets {
			revoked[stationID] = celsius
		}
		plan.Status = PlanCanceled
		plan.CanceledAt = compensatedAt
		// Unpublished targets exist only on the plan: revoke them by
		// detaching the set; the compensation record keeps the snapshot.
		plan.Targets = map[string]float64{}
		plan.Message = "canceled before publication; unpublished targets revoked"
		return nil
	})
	if err != nil {
		return CancelResult{}, err
	}

	// Remove queued commands under the buffer lock, which Drain takes too:
	// after this point no reconnect can pull a canceled command out and
	// apply it to a station. Commands already drained and applied are simply
	// absent here.
	discarded := c.buffer.Discard(plan.ID)
	// Report (never undo) commands that already took effect on stations.
	retained := c.commands.ExecutedForPlan(plan.ID)

	_, auditErr := c.audit.Record(audit.Entry{
		Action:   "dispatch.cancel",
		EntityID: plan.ID,
		Actor:    actor,
		Outcome:  audit.OutcomeCompensated,
		Message:  fmt.Sprintf("plan canceled: %d targets revoked, %d buffered commands discarded, %d executed commands retained", len(revoked), len(discarded), len(retained)),
		At:       compensatedAt,
		Details: map[string]any{
			"revoked_targets":          revoked,
			"discarded_command_ids":    commandIDs(discarded),
			"retained_command_ids":     retainedCommandIDs(retained),
			"retained_not_rolled_back": true,
		},
	})
	if auditErr != nil {
		// Compensation could not be durably recorded: restore every mutable
		// side effect so the same cancel request retries from a clean slate.
		// Planner lock is taken before the buffer lock here, matching the
		// forward path's lock order. Station state was never touched, so
		// there is nothing to undo there.
		_, _ = c.planner.commit(planID, func(plan *Plan) error {
			if plan.Status != PlanCanceled {
				return nil
			}
			plan.Status = PlanCalculating
			plan.Targets = revoked
			plan.CanceledAt = time.Time{}
			plan.Message = ""
			return nil
		})
		c.buffer.Restore(plan.ID, discarded)
		return CancelResult{}, auditErr
	}

	result := CancelResult{
		PlanID: plan.ID, Status: PlanCanceled, RevokedTargets: revoked,
		DiscardedCommands: discarded, RetainedCommands: retained, CompensatedAt: compensatedAt,
	}
	c.results[planID] = result
	return result, nil
}

func commandIDs(commands []BufferedCommand) []string {
	ids := make([]string, 0, len(commands))
	for _, command := range commands {
		ids = append(ids, command.CommandID)
	}
	return ids
}

func retainedCommandIDs(commands []station.Command) []string {
	ids := make([]string, 0, len(commands))
	for _, command := range commands {
		ids = append(ids, command.ID)
	}
	return ids
}
