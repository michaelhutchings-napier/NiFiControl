package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFlowAboutURL(t *testing.T) {
	got, err := flowAboutURL("https://nifi.example.com/nifi/")
	if err != nil {
		t.Fatal(err)
	}
	want := "https://nifi.example.com/nifi/nifi-api/flow/about"
	if got != want {
		t.Fatalf("flowAboutURL = %q, want %q", got, want)
	}
}

func TestFlowAboutURLRequiresAbsoluteURI(t *testing.T) {
	if _, err := flowAboutURL("nifi.example.com"); err == nil {
		t.Fatal("expected error for URI without scheme")
	}
}

func TestHTTPParameterContextClientListParameterContexts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-api/flow/parameter-contexts" {
			t.Fatalf("path = %s, want /nifi-api/flow/parameter-contexts", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ParameterContextsResponse{
			ParameterContexts: []ParameterContextEntity{
				{ID: "pc-1", Revision: Revision{Version: 7}, Component: ParameterContextComponent{Name: "payments"}},
			},
		})
	}))
	defer server.Close()

	contexts, err := (HTTPParameterContextClient{}).ListParameterContexts(t.Context(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(contexts) != 1 {
		t.Fatalf("contexts length = %d, want 1", len(contexts))
	}
	if contexts[0].ID != "pc-1" || contexts[0].Revision.Version != 7 {
		t.Fatalf("context = %#v, want id pc-1 version 7", contexts[0])
	}
}

func TestHTTPParameterContextClientCreateParameterContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/parameter-contexts" {
			t.Fatalf("path = %s, want /nifi-api/parameter-contexts", r.URL.Path)
		}
		var got ParameterContextEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Name != "payments" {
			t.Fatalf("name = %q, want payments", got.Component.Name)
		}
		_ = json.NewEncoder(w).Encode(ParameterContextEntity{
			ID:       "pc-1",
			Revision: Revision{Version: 0},
			Component: ParameterContextComponent{
				ID:   "pc-1",
				Name: got.Component.Name,
			},
		})
	}))
	defer server.Close()

	created, err := (HTTPParameterContextClient{}).CreateParameterContext(t.Context(), server.URL, ParameterContextEntity{
		Component: ParameterContextComponent{Name: "payments"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "pc-1" {
		t.Fatalf("created id = %q, want pc-1", created.ID)
	}
}
