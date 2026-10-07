package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func postJSON(t *testing.T, handler http.Handler, path string, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var envelope struct {
		Data  map[string]any `json:"data"`
		Error string         `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode %s: %v body=%s", path, err, rec.Body.String())
	}
	return rec.Code, envelope.Data
}

func TestCancelPlanEndpointIsIdempotent(t *testing.T) {
	app, err := NewApp(t.TempDir())
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close(t.Context()) })
	handler := app.Handler()

	status, plan := postJSON(t, handler, "/api/v1/dispatch/plans", `{"targets":{"xincheng":70},"delay_millis":0}`)
	if status != http.StatusAccepted {
		t.Fatalf("create status = %d", status)
	}
	planID, _ := plan["id"].(string)
	if planID == "" {
		t.Fatalf("missing plan id in %v", plan)
	}

	status, buffered := postJSON(t, handler, "/api/v1/dispatch/buffer",
		`{"station_id":"xincheng","plan_id":"`+planID+`","epoch":9,"sequence":1,"celsius":70}`)
	if status != http.StatusAccepted {
		t.Fatalf("buffer status = %d payload=%v", status, buffered)
	}

	cancelBody := func() (int, map[string]any) {
		return postJSON(t, handler, "/api/v1/dispatch/plans/"+planID+"/cancel", `{"actor":"operator"}`)
	}
	status1, result1 := cancelBody()
	if status1 != http.StatusOK {
		t.Fatalf("first cancel status = %d result=%v", status1, result1)
	}
	status2, result2 := cancelBody()
	if status2 != http.StatusOK {
		t.Fatalf("retry cancel status = %d", status2)
	}
	if result1["compensated_at"] != result2["compensated_at"] {
		t.Fatalf("retry changed result: %v vs %v", result1["compensated_at"], result2["compensated_at"])
	}
	discarded, _ := result2["discarded_commands"].([]any)
	if len(discarded) != 1 {
		t.Fatalf("retry discarded = %d, want cached 1", len(discarded))
	}
	retained, _ := result2["retained_commands"].([]any)
	if len(retained) != 0 {
		t.Fatalf("retained = %d, want 0 for an unapplied command", len(retained))
	}

	// Buffering after cancellation must be rejected.
	if code, _ := postJSON(t, handler, "/api/v1/dispatch/buffer",
		`{"station_id":"xincheng","plan_id":"`+planID+`","epoch":9,"sequence":2,"celsius":71}`); code != http.StatusConflict {
		t.Fatalf("post-cancel buffer status = %d, want 409", code)
	}
	// Publishing a canceled plan must fail.
	if code, _ := postJSON(t, handler, "/api/v1/dispatch/plans/"+planID+"/publish", `{"actor":"operator"}`); code != http.StatusConflict {
		t.Fatalf("post-cancel publish status = %d, want 409", code)
	}

	// The original station target was never touched by the canceled plan.
	if state, ok := app.Stations.Get("xincheng"); !ok || state.TargetCelsius != 68 {
		t.Fatalf("station target = %v, want untouched 68", state.TargetCelsius)
	}
}

func TestCancelUnknownPlanEndpointReturns404(t *testing.T) {
	app, err := NewApp(t.TempDir())
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close(t.Context()) })
	if code, _ := postJSON(t, app.Handler(), "/api/v1/dispatch/plans/nope/cancel", `{"actor":"operator"}`); code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
}
