package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/dispatch"
)

func (a *App) listPlans(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.Planner.Plans())
}

func (a *App) createPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Targets     map[string]float64 `json:"targets"`
		DelayMillis int                `json:"delay_millis"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.Planner.Begin(r.Context(), input.Targets, time.Duration(input.DelayMillis)*time.Millisecond)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, plan)
}

func (a *App) publishPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	plan, err := a.Publisher.Publish(chi.URLParam(r, "planID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (a *App) cancelPlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	report, err := a.Canceler.Cancel(chi.URLParam(r, "planID"), input.Actor)
	if err != nil {
		var statusErr *dispatch.StatusError
		switch {
		case errors.Is(err, dispatch.ErrPlanNotFound):
			writeError(w, http.StatusNotFound, err)
		case errors.As(err, &statusErr):
			writeError(w, http.StatusConflict, err)
		default:
			writeError(w, http.StatusConflict, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *App) applyHold(w http.ResponseWriter, r *http.Request) {
	var input struct {
		StationIDs []string `json:"station_ids"`
		Actor      string   `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	revision, err := a.Targets.ManualHold(input.StationIDs, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": revision, "stations": input.StationIDs})
}

func (a *App) releaseHold(w http.ResponseWriter, r *http.Request) {
	revision, err := a.Targets.ReleaseHold(chi.URLParam(r, "stationID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": revision})
}

func (a *App) bufferCommand(w http.ResponseWriter, r *http.Request) {
	var input struct {
		StationID string  `json:"station_id"`
		PlanID    string  `json:"plan_id"`
		Epoch     uint64  `json:"epoch"`
		Sequence  uint64  `json:"sequence"`
		Celsius   float64 `json:"celsius"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	command, err := a.DispatchBuffer.Enqueue(input.StationID, input.PlanID, input.Epoch, input.Sequence, input.Celsius)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, command)
}
