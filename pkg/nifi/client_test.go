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

func TestHTTPParameterContextClientGetParameterContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-api/parameter-contexts/pc-1" {
			t.Fatalf("path = %s, want /nifi-api/parameter-contexts/pc-1", r.URL.Path)
		}
		if r.URL.Query().Get("includeInheritedParameters") != "false" {
			t.Fatalf("includeInheritedParameters = %q, want false", r.URL.Query().Get("includeInheritedParameters"))
		}
		_ = json.NewEncoder(w).Encode(ParameterContextEntity{ID: "pc-1"})
	}))
	defer server.Close()

	got, err := (HTTPParameterContextClient{}).GetParameterContext(t.Context(), server.URL, "pc-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "pc-1" {
		t.Fatalf("id = %q, want pc-1", got.ID)
	}
}

func TestHTTPParameterContextClientDeleteParameterContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/nifi-api/parameter-contexts/pc-1" {
			t.Fatalf("path = %s, want /nifi-api/parameter-contexts/pc-1", r.URL.Path)
		}
		if r.URL.Query().Get("version") != "12" {
			t.Fatalf("version = %q, want 12", r.URL.Query().Get("version"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := (HTTPParameterContextClient{}).DeleteParameterContext(t.Context(), server.URL, "pc-1", 12); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPParameterContextClientCreateParameterContextUpdateRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/parameter-contexts/pc-1/update-requests" {
			t.Fatalf("path = %s, want /nifi-api/parameter-contexts/pc-1/update-requests", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ParameterContextUpdateRequestEntity{
			Request: ParameterContextUpdateRequest{RequestID: "update-1", Complete: false},
		})
	}))
	defer server.Close()

	request, err := (HTTPParameterContextClient{}).CreateParameterContextUpdateRequest(t.Context(), server.URL, "pc-1", ParameterContextEntity{ID: "pc-1"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Request.RequestID != "update-1" {
		t.Fatalf("request id = %q, want update-1", request.Request.RequestID)
	}
}

func TestHTTPParameterContextClientGetParameterContextUpdateRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-api/parameter-contexts/pc-1/update-requests/update-1" {
			t.Fatalf("path = %s, want /nifi-api/parameter-contexts/pc-1/update-requests/update-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ParameterContextUpdateRequestEntity{
			Request: ParameterContextUpdateRequest{RequestID: "update-1", Complete: true},
		})
	}))
	defer server.Close()

	request, err := (HTTPParameterContextClient{}).GetParameterContextUpdateRequest(t.Context(), server.URL, "pc-1", "update-1")
	if err != nil {
		t.Fatal(err)
	}
	if request.Request.RequestID != "update-1" || !request.Request.Complete {
		t.Fatalf("request = %#v, want complete update-1", request.Request)
	}
}

func TestHTTPRegistryClientClientCreateRegistryClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller/registry-clients" {
			t.Fatalf("path = %s, want /nifi-api/controller/registry-clients", r.URL.Path)
		}
		var got RegistryClientEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Name != "platform-flows" {
			t.Fatalf("name = %q, want platform-flows", got.Component.Name)
		}
		_ = json.NewEncoder(w).Encode(RegistryClientEntity{
			ID:       "registry-1",
			Revision: Revision{Version: 0},
			Component: RegistryClientComponent{
				ID:   "registry-1",
				Name: got.Component.Name,
			},
		})
	}))
	defer server.Close()

	created, err := (HTTPRegistryClientClient{}).CreateRegistryClient(t.Context(), server.URL, RegistryClientEntity{
		Component: RegistryClientComponent{Name: "platform-flows"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "registry-1" {
		t.Fatalf("created id = %q, want registry-1", created.ID)
	}
}

func TestHTTPRegistryClientClientGetRegistryClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller/registry-clients/registry-1" {
			t.Fatalf("path = %s, want /nifi-api/controller/registry-clients/registry-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(RegistryClientEntity{ID: "registry-1"})
	}))
	defer server.Close()

	got, err := (HTTPRegistryClientClient{}).GetRegistryClient(t.Context(), server.URL, "registry-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "registry-1" {
		t.Fatalf("id = %q, want registry-1", got.ID)
	}
}

func TestHTTPRegistryClientClientUpdateRegistryClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller/registry-clients/registry-1" {
			t.Fatalf("path = %s, want /nifi-api/controller/registry-clients/registry-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(RegistryClientEntity{ID: "registry-1", Revision: Revision{Version: 2}})
	}))
	defer server.Close()

	got, err := (HTTPRegistryClientClient{}).UpdateRegistryClient(t.Context(), server.URL, RegistryClientEntity{ID: "registry-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision.Version != 2 {
		t.Fatalf("revision = %d, want 2", got.Revision.Version)
	}
}

func TestHTTPRegistryClientClientDeleteRegistryClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller/registry-clients/registry-1" {
			t.Fatalf("path = %s, want /nifi-api/controller/registry-clients/registry-1", r.URL.Path)
		}
		if r.URL.Query().Get("version") != "12" {
			t.Fatalf("version = %q, want 12", r.URL.Query().Get("version"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := (HTTPRegistryClientClient{}).DeleteRegistryClient(t.Context(), server.URL, "registry-1", 12); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPProcessGroupClientCreateProcessGroup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/root/process-groups" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/root/process-groups", r.URL.Path)
		}
		var got ProcessGroupEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Name != "payments" {
			t.Fatalf("name = %q, want payments", got.Component.Name)
		}
		_ = json.NewEncoder(w).Encode(ProcessGroupEntity{
			ID:       "pg-1",
			Revision: Revision{Version: 0},
			Component: ProcessGroupComponent{
				ID:   "pg-1",
				Name: got.Component.Name,
			},
		})
	}))
	defer server.Close()

	created, err := (HTTPProcessGroupClient{}).CreateProcessGroup(t.Context(), server.URL, "root", ProcessGroupEntity{
		Component: ProcessGroupComponent{Name: "payments"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "pg-1" {
		t.Fatalf("created id = %q, want pg-1", created.ID)
	}
}

func TestHTTPProcessGroupClientGetProcessGroup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ProcessGroupEntity{ID: "pg-1"})
	}))
	defer server.Close()

	got, err := (HTTPProcessGroupClient{}).GetProcessGroup(t.Context(), server.URL, "pg-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "pg-1" {
		t.Fatalf("id = %q, want pg-1", got.ID)
	}
}

func TestHTTPProcessGroupClientUpdateProcessGroup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ProcessGroupEntity{ID: "pg-1", Revision: Revision{Version: 2}})
	}))
	defer server.Close()

	got, err := (HTTPProcessGroupClient{}).UpdateProcessGroup(t.Context(), server.URL, ProcessGroupEntity{ID: "pg-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision.Version != 2 {
		t.Fatalf("revision = %d, want 2", got.Revision.Version)
	}
}

func TestHTTPProcessGroupClientDeleteProcessGroup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1", r.URL.Path)
		}
		if r.URL.Query().Get("version") != "12" {
			t.Fatalf("version = %q, want 12", r.URL.Query().Get("version"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := (HTTPProcessGroupClient{}).DeleteProcessGroup(t.Context(), server.URL, "pg-1", 12); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPProcessorClientCreateProcessor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1/processors" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1/processors", r.URL.Path)
		}
		var got ProcessorEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Type != "org.apache.nifi.processors.standard.GenerateFlowFile" {
			t.Fatalf("type = %q, want GenerateFlowFile", got.Component.Type)
		}
		_ = json.NewEncoder(w).Encode(ProcessorEntity{
			ID:       "processor-1",
			Revision: Revision{Version: 0},
			Component: ProcessorComponent{
				ID:   "processor-1",
				Name: got.Component.Name,
				Type: got.Component.Type,
			},
		})
	}))
	defer server.Close()

	created, err := (HTTPProcessorClient{}).CreateProcessor(t.Context(), server.URL, "pg-1", ProcessorEntity{
		Component: ProcessorComponent{Name: "generate", Type: "org.apache.nifi.processors.standard.GenerateFlowFile"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "processor-1" {
		t.Fatalf("created id = %q, want processor-1", created.ID)
	}
}

func TestHTTPProcessorClientGetProcessor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-api/processors/processor-1" {
			t.Fatalf("path = %s, want /nifi-api/processors/processor-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ProcessorEntity{ID: "processor-1"})
	}))
	defer server.Close()

	got, err := (HTTPProcessorClient{}).GetProcessor(t.Context(), server.URL, "processor-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "processor-1" {
		t.Fatalf("id = %q, want processor-1", got.ID)
	}
}

func TestHTTPProcessorClientUpdateProcessor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/nifi-api/processors/processor-1" {
			t.Fatalf("path = %s, want /nifi-api/processors/processor-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ProcessorEntity{ID: "processor-1", Revision: Revision{Version: 2}})
	}))
	defer server.Close()

	got, err := (HTTPProcessorClient{}).UpdateProcessor(t.Context(), server.URL, ProcessorEntity{ID: "processor-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision.Version != 2 {
		t.Fatalf("revision = %d, want 2", got.Revision.Version)
	}
}

func TestHTTPProcessorClientDeleteProcessor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/nifi-api/processors/processor-1" {
			t.Fatalf("path = %s, want /nifi-api/processors/processor-1", r.URL.Path)
		}
		if r.URL.Query().Get("version") != "12" {
			t.Fatalf("version = %q, want 12", r.URL.Query().Get("version"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := (HTTPProcessorClient{}).DeleteProcessor(t.Context(), server.URL, "processor-1", 12); err != nil {
		t.Fatal(err)
	}
}
