package station

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type CommandService struct {
	registry *Registry
	mu       sync.Mutex
	last     map[string]Command
	applied  map[string]Command
	rejected []Command
}

func NewCommandService(registry *Registry) *CommandService {
	return &CommandService{registry: registry, last: make(map[string]Command), applied: make(map[string]Command), rejected: make([]Command, 0)}
}

func (s *CommandService) Apply(command Command) (Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stationState, ok := s.registry.Get(command.StationID)
	if !ok {
		return Command{}, fmt.Errorf("station %s not found", command.StationID)
	}
	if command.At.IsZero() {
		command.At = time.Now().UTC()
	}
	previous := s.last[command.StationID]
	if command.Epoch < stationState.ControllerEpoch || command.Epoch < previous.Epoch {
		command.Status = "rejected_old_epoch"
		s.rejected = append(s.rejected, command)
		return command, fmt.Errorf("controller epoch %d retired", command.Epoch)
	}
	if command.Epoch == previous.Epoch && command.Sequence <= previous.Sequence {
		command.Status = "duplicate"
		return command, nil
	}
	_, err := s.registry.Update(command.StationID, func(item *Station) error {
		item.ControllerEpoch = command.Epoch
		item.TargetCelsius = command.Celsius
		return nil
	})
	if err != nil {
		return Command{}, err
	}
	command.Status = "applied"
	s.last[command.StationID] = command
	s.applied[command.ID] = command
	return command, nil
}

func (s *CommandService) Last(stationID string) (Command, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	command, ok := s.last[stationID]
	return command, ok
}

func (s *CommandService) Rejected() []Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Command(nil), s.rejected...)
}

// AppliedByPlan returns commands originating from planID that were executed
// at a station. The dispatch cancel flow uses this to report executed
// commands truthfully instead of pretending they can be rolled back.
func (s *CommandService) AppliedByPlan(planID string) []Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Command, 0)
	if planID == "" {
		return result
	}
	for _, command := range s.applied {
		if command.PlanID == planID {
			result = append(result, command)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].StationID != result[j].StationID {
			return result[i].StationID < result[j].StationID
		}
		if result[i].Epoch == result[j].Epoch {
			return result[i].Sequence < result[j].Sequence
		}
		return result[i].Epoch < result[j].Epoch
	})
	return result
}
