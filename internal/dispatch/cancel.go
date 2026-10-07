package dispatch

import (
	"errors"
	"fmt"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
)

// CancelReport describes the compensation performed when a plan is canceled.
// It is stored on the plan, so retrying the same cancel request replays the
// exact same result without repeating any side effect.
type CancelReport struct {
	PlanID            string             `json:"plan_id"`
	Actor             string             `json:"actor"`
	CanceledAt        time.Time          `json:"canceled_at"`
	RevokedTargets    map[string]float64 `json:"revoked_targets"`
	DiscardedCommands []BufferedCommand  `json:"discarded_commands"`
	ExecutedCommands  []station.Command  `json:"executed_commands"`
}

func (r *CancelReport) Clone() *CancelReport {
	if r == nil {
		return nil
	}
	copy := *r
	copy.RevokedTargets = make(map[string]float64, len(r.RevokedTargets))
	for stationID, celsius := range r.RevokedTargets {
		copy.RevokedTargets[stationID] = celsius
	}
	copy.DiscardedCommands = append([]BufferedCommand{}, r.DiscardedCommands...)
	copy.ExecutedCommands = append([]station.Command{}, r.ExecutedCommands...)
	return &copy
}

// StatusError reports that a plan is in a state that does not allow the
// requested transition.
type StatusError struct {
	PlanID string
	Status PlanStatus
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("plan %s is %s", e.PlanID, e.Status)
}

type Canceler struct {
	planner  *Planner
	buffer   *Buffer
	commands *station.CommandService
	audit    *audit.Recorder
}

func NewCanceler(planner *Planner, buffer *Buffer, commands *station.CommandService, recorder *audit.Recorder) *Canceler {
	return &Canceler{planner: planner, buffer: buffer, commands: commands, audit: recorder}
}

// Cancel revokes a plan that was created but never published. Pending
// buffered commands queued for the plan are discarded; commands already
// executed at a station are reported as executed and deliberately left in
// effect — the cancel flow never fabricates a rollback of site state.
// Canceling an already-canceled plan replays the stored report unchanged.
func (c *Canceler) Cancel(planID, actor string) (CancelReport, error) {
	var report CancelReport
	replayed := false
	plan, err := c.planner.Update(planID, func(plan *Plan) error {
		if plan.Status == PlanCanceled {
			if plan.Cancellation == nil {
				return fmt.Errorf("plan %s cancellation is incomplete", plan.ID)
			}
			report = *plan.Cancellation.Clone()
			replayed = true
			return nil
		}
		if plan.Status != PlanCalculating {
			return &StatusError{PlanID: plan.ID, Status: plan.Status}
		}
		report = CancelReport{
			PlanID:            plan.ID,
			Actor:             actor,
			CanceledAt:        time.Now().UTC(),
			RevokedTargets:    make(map[string]float64, len(plan.Targets)),
			DiscardedCommands: c.buffer.PurgePlan(plan.ID),
			ExecutedCommands:  c.commands.AppliedByPlan(plan.ID),
		}
		for stationID, celsius := range plan.Targets {
			report.RevokedTargets[stationID] = celsius
		}
		plan.Status = PlanCanceled
		plan.Cancellation = report.Clone()
		return nil
	})
	if err != nil {
		var statusErr *StatusError
		if errors.As(err, &statusErr) {
			_ = c.audit.Rejected("dispatch.cancel", planID, actor, err.Error(), nil)
		}
		return CancelReport{}, err
	}
	if replayed {
		return report, nil
	}
	message := fmt.Sprintf("plan canceled: %d targets revoked, %d buffered commands discarded, %d executed commands left in effect",
		len(report.RevokedTargets), len(report.DiscardedCommands), len(report.ExecutedCommands))
	if err := c.audit.Accepted("dispatch.cancel", plan.ID, actor, message, map[string]any{
		"source_revision":    plan.SourceRevision,
		"revoked_targets":    report.RevokedTargets,
		"discarded_commands": bufferedCommandIDs(report.DiscardedCommands),
		"executed_commands":  executedCommandIDs(report.ExecutedCommands),
	}); err != nil {
		return CancelReport{}, err
	}
	return report, nil
}

func bufferedCommandIDs(commands []BufferedCommand) []string {
	ids := make([]string, 0, len(commands))
	for _, command := range commands {
		ids = append(ids, command.CommandID)
	}
	return ids
}

func executedCommandIDs(commands []station.Command) []string {
	ids := make([]string, 0, len(commands))
	for _, command := range commands {
		ids = append(ids, command.ID)
	}
	return ids
}
