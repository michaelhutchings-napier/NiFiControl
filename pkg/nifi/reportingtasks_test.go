package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPReportingTaskClientCreatePostsToController(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nifi-api/controller/reporting-tasks" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in ReportingTaskEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Component.Type != "org.apache.nifi.reporting.ambari.AmbariReportingTask" || in.Component.SchedulingPeriod != "60 sec" {
			t.Fatalf("create payload = %#v", in.Component)
		}
		out := in
		out.ID = "rt1"
		out.Component.ID = "rt1"
		out.Component.State = "STOPPED"
		out.Component.ValidationStatus = "VALID"
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	created, err := (HTTPReportingTaskClient{}).CreateReportingTask(t.Context(), server.URL, ReportingTaskEntity{
		Revision: Revision{Version: 0},
		Component: ReportingTaskComponent{
			Name:             "metrics",
			Type:             "org.apache.nifi.reporting.ambari.AmbariReportingTask",
			SchedulingPeriod: "60 sec",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ReportingTaskEntityID(*created) != "rt1" || created.Component.ValidationStatus != "VALID" {
		t.Fatalf("created = %#v", created)
	}
}

func TestHTTPReportingTaskClientRunStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/reporting-tasks/rt1/run-status" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in ReportingTaskRunStatusEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.State != "RUNNING" || in.Revision.Version != 2 {
			t.Fatalf("run-status payload = %#v", in)
		}
		_ = json.NewEncoder(w).Encode(ReportingTaskEntity{ID: "rt1", Revision: Revision{Version: 3}, Component: ReportingTaskComponent{ID: "rt1", State: "RUNNING"}})
	}))
	defer server.Close()

	updated, err := (HTTPReportingTaskClient{}).UpdateReportingTaskRunStatus(t.Context(), server.URL, "rt1", 2, "RUNNING")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Component.State != "RUNNING" || updated.Revision.Version != 3 {
		t.Fatalf("updated = %#v", updated)
	}
}

func TestHTTPReportingTaskClientDeleteSendsVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/nifi-api/reporting-tasks/rt1" || r.URL.Query().Get("version") != "5" {
			t.Fatalf("got %s %s ?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := (HTTPReportingTaskClient{}).DeleteReportingTask(t.Context(), server.URL, "rt1", 5); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPReportingTaskClientGetNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no task", http.StatusNotFound)
	}))
	defer server.Close()

	_, err := (HTTPReportingTaskClient{}).GetReportingTask(t.Context(), server.URL, "missing")
	if !IsNotFound(err) {
		t.Fatalf("expected IsNotFound, got %v", err)
	}
}
