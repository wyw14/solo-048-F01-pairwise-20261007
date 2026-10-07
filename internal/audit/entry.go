package audit

import "time"

type Outcome string

const (
	OutcomeAccepted    Outcome = "accepted"
	OutcomeRejected    Outcome = "rejected"
	OutcomeCompleted   Outcome = "completed"
	OutcomeFailed      Outcome = "failed"
	OutcomeCompensated Outcome = "compensated"
)

type Entry struct {
	ID       string         `json:"id"`
	Action   string         `json:"action"`
	EntityID string         `json:"entity_id"`
	Actor    string         `json:"actor"`
	Outcome  Outcome        `json:"outcome"`
	Message  string         `json:"message"`
	At       time.Time      `json:"at"`
	Details  map[string]any `json:"details,omitempty"`
}

type Filter struct {
	EntityID string
	Action   string
	Outcome  Outcome
	Limit    int
}

func (f Filter) Match(entry Entry) bool {
	if f.EntityID != "" && entry.EntityID != f.EntityID {
		return false
	}
	if f.Action != "" && entry.Action != f.Action {
		return false
	}
	if f.Outcome != "" && entry.Outcome != f.Outcome {
		return false
	}
	return true
}
