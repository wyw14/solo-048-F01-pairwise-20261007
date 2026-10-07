package recovery

import (
	"fmt"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/dispatch"
	"heatpulse/internal/station"
)

type ReplayResult struct {
	StationID string            `json:"station_id"`
	Epoch     uint64            `json:"epoch"`
	Applied   []station.Command `json:"applied"`
	Rejected  []station.Command `json:"rejected"`
	At        time.Time         `json:"at"`
}

type Reconnector struct {
	buffer   *dispatch.Buffer
	stations *station.Registry
	commands *station.CommandService
	audit    *audit.Recorder
}

func NewReconnector(buffer *dispatch.Buffer, stations *station.Registry, commands *station.CommandService, recorder *audit.Recorder) *Reconnector {
	return &Reconnector{buffer: buffer, stations: stations, commands: commands, audit: recorder}
}

func (r *Reconnector) Reconnect(stationID string, currentEpoch uint64) (ReplayResult, error) {
	if _, ok := r.stations.Get(stationID); !ok {
		return ReplayResult{}, fmt.Errorf("station %s not found", stationID)
	}
	r.buffer.AcceptEpoch(stationID, currentEpoch)
	ready, retired := r.buffer.Drain(stationID, currentEpoch)
	result := ReplayResult{StationID: stationID, Epoch: currentEpoch, At: time.Now().UTC()}
	for _, buffered := range retired {
		result.Rejected = append(result.Rejected, station.Command{
			ID: buffered.CommandID, StationID: buffered.StationID, PlanID: buffered.PlanID, Epoch: buffered.Epoch,
			Sequence: buffered.Sequence, Celsius: buffered.Celsius, Status: "rejected_old_epoch", At: buffered.QueuedAt,
		})
	}
	for _, buffered := range ready {
		command, err := r.commands.Apply(station.Command{
			ID: buffered.CommandID, StationID: stationID, PlanID: buffered.PlanID, Epoch: buffered.Epoch,
			Sequence: buffered.Sequence, Celsius: buffered.Celsius, At: buffered.QueuedAt,
		})
		if err != nil {
			result.Rejected = append(result.Rejected, command)
			continue
		}
		result.Applied = append(result.Applied, command)
	}
	if _, err := r.stations.SetOnline(stationID, true); err != nil {
		return result, err
	}
	if err := r.audit.Accepted("recovery.reconnect", stationID, "recovery", "station replay reconciled", map[string]any{"epoch": currentEpoch, "applied": len(result.Applied), "rejected": len(result.Rejected)}); err != nil {
		return result, err
	}
	return result, nil
}
