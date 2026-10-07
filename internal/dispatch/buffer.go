package dispatch

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrPlanClosed is returned when a command is buffered against a plan that
// is no longer calculating (canceled or published).
var ErrPlanClosed = errors.New("dispatch plan is closed for new commands")

type Buffer struct {
	mu       sync.Mutex
	commands map[string][]BufferedCommand
	accepted map[string]uint64
	closed   map[string]struct{}
}

func NewBuffer() *Buffer {
	return &Buffer{
		commands: make(map[string][]BufferedCommand),
		accepted: make(map[string]uint64),
		closed:   make(map[string]struct{}),
	}
}

func (b *Buffer) Enqueue(stationID string, epoch, sequence uint64, celsius float64) (BufferedCommand, error) {
	return b.EnqueueForPlan(stationID, "", epoch, sequence, celsius)
}

func (b *Buffer) EnqueueForPlan(stationID, planID string, epoch, sequence uint64, celsius float64) (BufferedCommand, error) {
	if stationID == "" || epoch == 0 || sequence == 0 {
		return BufferedCommand{}, fmt.Errorf("buffered command identity is incomplete")
	}
	command := BufferedCommand{
		StationID: stationID, PlanID: planID, CommandID: uuid.NewString(), Epoch: epoch,
		Sequence: sequence, Celsius: celsius, QueuedAt: time.Now().UTC(),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Same lock as Discard: whichever of enqueue/cancel runs first wins, so
	// a command can never be queued for a plan after that plan was canceled.
	if planID != "" {
		if _, ok := b.closed[planID]; ok {
			return BufferedCommand{}, fmt.Errorf("%w: %s", ErrPlanClosed, planID)
		}
	}
	b.commands[stationID] = append(b.commands[stationID], command)
	return command, nil
}

func (b *Buffer) AcceptEpoch(stationID string, epoch uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if epoch > b.accepted[stationID] {
		b.accepted[stationID] = epoch
	}
}

func (b *Buffer) Drain(stationID string, currentEpoch uint64) (ready, retired []BufferedCommand) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if currentEpoch > b.accepted[stationID] {
		b.accepted[stationID] = currentEpoch
	}
	fence := b.accepted[stationID]
	for _, command := range b.commands[stationID] {
		if command.Epoch < fence {
			retired = append(retired, command)
			continue
		}
		ready = append(ready, command)
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Epoch == ready[j].Epoch {
			return ready[i].Sequence < ready[j].Sequence
		}
		return ready[i].Epoch < ready[j].Epoch
	})
	delete(b.commands, stationID)
	return ready, retired
}

func (b *Buffer) Pending(stationID string) []BufferedCommand {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]BufferedCommand(nil), b.commands[stationID]...)
}

// Restore re-queues commands previously removed by Discard and reopens the
// plan when the rest of a compensation transaction could not be committed.
func (b *Buffer) Restore(planID string, commands []BufferedCommand) {
	if len(commands) == 0 && planID == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, command := range commands {
		b.commands[command.StationID] = append(b.commands[command.StationID], command)
	}
	if planID != "" {
		delete(b.closed, planID)
	}
}

// ClosePlan marks a plan as terminal (published) so no further commands can
// be buffered against it. Queued commands are preserved for replay.
func (b *Buffer) ClosePlan(planID string) {
	if planID == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed[planID] = struct{}{}
}

// Discard removes every still-queued command that belongs to planID and
// closes the plan against new enqueues, all under one lock. Commands that
// were already drained and applied to stations are absent from the queue
// and therefore cannot be touched here; callers must report those
// separately instead of rolling station state back.
func (b *Buffer) Discard(planID string) []BufferedCommand {
	if planID == "" {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed[planID] = struct{}{}
	var removed []BufferedCommand
	for stationID, queue := range b.commands {
		kept := queue[:0]
		for _, command := range queue {
			if command.PlanID == planID {
				removed = append(removed, command)
				continue
			}
			kept = append(kept, command)
		}
		if len(kept) == 0 {
			delete(b.commands, stationID)
		} else {
			b.commands[stationID] = kept
		}
	}
	return removed
}
