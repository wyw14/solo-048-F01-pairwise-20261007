package dispatch

import "time"

type PlanStatus string

const (
	PlanCalculating PlanStatus = "calculating"
	PlanPublished   PlanStatus = "published"
	PlanStale       PlanStatus = "stale"
	PlanFailed      PlanStatus = "failed"
	PlanCanceled    PlanStatus = "canceled"
)

type Plan struct {
	ID             string             `json:"id"`
	SourceRevision uint64             `json:"source_revision"`
	Targets        map[string]float64 `json:"targets"`
	Status         PlanStatus         `json:"status"`
	CreatedAt      time.Time          `json:"created_at"`
	PublishedAt    time.Time          `json:"published_at,omitempty"`
	Cancellation   *CancelReport      `json:"cancellation,omitempty"`
	Message        string             `json:"message,omitempty"`
}

func (p Plan) Clone() Plan {
	copy := p
	copy.Targets = make(map[string]float64, len(p.Targets))
	for stationID, value := range p.Targets {
		copy.Targets[stationID] = value
	}
	copy.Cancellation = p.Cancellation.Clone()
	return copy
}

type BufferedCommand struct {
	StationID string    `json:"station_id"`
	CommandID string    `json:"command_id"`
	PlanID    string    `json:"plan_id,omitempty"`
	Epoch     uint64    `json:"epoch"`
	Sequence  uint64    `json:"sequence"`
	Celsius   float64   `json:"celsius"`
	QueuedAt  time.Time `json:"queued_at"`
}
