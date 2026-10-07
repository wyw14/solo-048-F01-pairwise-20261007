package station

import "time"

type ExchangerPhase string

const (
	ExchangerStandby  ExchangerPhase = "standby"
	ExchangerPriming  ExchangerPhase = "priming"
	ExchangerCarrying ExchangerPhase = "carrying"
	ExchangerDraining ExchangerPhase = "draining"
)

type Station struct {
	ID              string                    `json:"id"`
	Name            string                    `json:"name"`
	ZoneID          string                    `json:"zone_id"`
	Online          bool                      `json:"online"`
	Maintenance     bool                      `json:"maintenance"`
	TargetCelsius   float64                   `json:"target_celsius"`
	Hold            bool                      `json:"hold"`
	TargetRevision  uint64                    `json:"target_revision"`
	ControllerEpoch uint64                    `json:"controller_epoch"`
	Exchangers      map[string]ExchangerPhase `json:"exchangers"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

func (s Station) Clone() Station {
	copy := s
	copy.Exchangers = make(map[string]ExchangerPhase, len(s.Exchangers))
	for id, phase := range s.Exchangers {
		copy.Exchangers[id] = phase
	}
	return copy
}

type Target struct {
	StationID string    `json:"station_id"`
	Celsius   float64   `json:"celsius"`
	Revision  uint64    `json:"revision"`
	Hold      bool      `json:"hold"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Command struct {
	ID        string    `json:"id"`
	StationID string    `json:"station_id"`
	PlanID    string    `json:"plan_id,omitempty"`
	Epoch     uint64    `json:"epoch"`
	Sequence  uint64    `json:"sequence"`
	Celsius   float64   `json:"celsius"`
	Status    string    `json:"status"`
	At        time.Time `json:"at"`
}
