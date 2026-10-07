package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/station"
)

var ErrPlanNotFound = errors.New("plan not found")

type Planner struct {
	targets *station.TargetService
	mu      sync.RWMutex
	plans   map[string]Plan
}

func NewPlanner(targets *station.TargetService) *Planner {
	return &Planner{targets: targets, plans: make(map[string]Plan)}
}

func (p *Planner) Begin(ctx context.Context, requested map[string]float64, delay time.Duration) (Plan, error) {
	if len(requested) == 0 {
		return Plan{}, fmt.Errorf("at least one station target is required")
	}
	plan := Plan{
		ID: uuid.NewString(), SourceRevision: p.targets.Revision(), Targets: make(map[string]float64, len(requested)),
		Status: PlanCalculating, CreatedAt: time.Now().UTC(),
	}
	for stationID, celsius := range requested {
		plan.Targets[stationID] = celsius
	}
	p.save(plan)
	select {
	case <-ctx.Done():
		plan.Status = PlanFailed
		plan.Message = ctx.Err().Error()
		p.save(plan)
		return plan, ctx.Err()
	case <-time.After(delay):
	}
	return plan, nil
}

func (p *Planner) save(plan Plan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.plans[plan.ID] = plan.Clone()
}

// Update applies mutate to the stored plan while holding the planner lock, so
// status transitions are atomic against concurrent readers and writers. The
// mutated plan is persisted only when mutate returns nil. Callers may invoke
// the buffer and command services from inside mutate: no code path acquires
// the planner lock while holding one of theirs, so the lock order stays
// acyclic.
func (p *Planner) Update(planID string, mutate func(*Plan) error) (Plan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, ok := p.plans[planID]
	if !ok {
		return Plan{}, fmt.Errorf("%w: %s", ErrPlanNotFound, planID)
	}
	working := plan.Clone()
	if err := mutate(&working); err != nil {
		return Plan{}, err
	}
	p.plans[planID] = working.Clone()
	return working.Clone(), nil
}

func (p *Planner) Plan(planID string) (Plan, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	plan, ok := p.plans[planID]
	return plan.Clone(), ok
}

func (p *Planner) Plans() []Plan {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]Plan, 0, len(p.plans))
	for _, plan := range p.plans {
		result = append(result, plan.Clone())
	}
	return result
}
