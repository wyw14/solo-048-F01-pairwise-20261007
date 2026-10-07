package dispatch

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Buffer struct {
	mu       sync.Mutex
	commands map[string][]BufferedCommand
	accepted map[string]uint64
}

func NewBuffer() *Buffer {
	return &Buffer{commands: make(map[string][]BufferedCommand), accepted: make(map[string]uint64)}
}

func (b *Buffer) Enqueue(stationID, planID string, epoch, sequence uint64, celsius float64) (BufferedCommand, error) {
	if stationID == "" || epoch == 0 || sequence == 0 {
		return BufferedCommand{}, fmt.Errorf("buffered command identity is incomplete")
	}
	command := BufferedCommand{
		StationID: stationID, CommandID: uuid.NewString(), PlanID: planID, Epoch: epoch,
		Sequence: sequence, Celsius: celsius, QueuedAt: time.Now().UTC(),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
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

// PurgePlan removes every pending command queued for planID and returns what
// was removed. Commands already drained for replay are no longer held here
// and are never touched by a purge.
func (b *Buffer) PurgePlan(planID string) []BufferedCommand {
	b.mu.Lock()
	defer b.mu.Unlock()
	purged := make([]BufferedCommand, 0)
	if planID == "" {
		return purged
	}
	for stationID, commands := range b.commands {
		kept := commands[:0]
		for _, command := range commands {
			if command.PlanID == planID {
				purged = append(purged, command)
				continue
			}
			kept = append(kept, command)
		}
		if len(kept) == 0 {
			delete(b.commands, stationID)
			continue
		}
		b.commands[stationID] = kept
	}
	sort.Slice(purged, func(i, j int) bool {
		if purged[i].StationID != purged[j].StationID {
			return purged[i].StationID < purged[j].StationID
		}
		if purged[i].Epoch == purged[j].Epoch {
			return purged[i].Sequence < purged[j].Sequence
		}
		return purged[i].Epoch < purged[j].Epoch
	})
	return purged
}
