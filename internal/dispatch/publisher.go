package dispatch

import (
	"errors"
	"fmt"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
)

type Publisher struct {
	planner *Planner
	targets *station.TargetService
	buffer  *Buffer
	audit   *audit.Recorder
}

func NewPublisher(planner *Planner, targets *station.TargetService, buffer *Buffer, recorder *audit.Recorder) *Publisher {
	return &Publisher{planner: planner, targets: targets, buffer: buffer, audit: recorder}
}

func (p *Publisher) Publish(planID, actor string) (Plan, error) {
	plan, err := p.planner.commit(planID, func(plan *Plan) error {
		if plan.Status != PlanCalculating {
			return fmt.Errorf("plan %s is %s", plan.ID, plan.Status)
		}
		if err := p.targets.CompareAndPublish(plan.SourceRevision, plan.Targets); err != nil {
			plan.Status = PlanStale
			plan.Message = err.Error()
			p.buffer.ClosePlan(plan.ID)
			return err
		}
		plan.Status = PlanPublished
		plan.PublishedAt = time.Now().UTC()
		// Terminal state: no further buffered commands may attach to the
		// plan; queued ones remain available for replay.
		p.buffer.ClosePlan(plan.ID)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrPlanNotFound) {
			return plan, fmt.Errorf("plan %s not found", planID)
		}
		if plan.Status == PlanStale {
			_ = p.audit.Rejected("dispatch.publish", plan.ID, actor, err.Error(), map[string]any{"source_revision": plan.SourceRevision})
		}
		return plan, err
	}
	if err := p.audit.Accepted("dispatch.publish", plan.ID, actor, "dispatch targets published", map[string]any{"source_revision": plan.SourceRevision}); err != nil {
		return plan, err
	}
	return plan, nil
}
