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

func TestHTTPFlowSnapshotClientImportProcessGroup(t *testing.T) {
	snapshot := json.RawMessage(`{"flowContents":{"name":"Payments","processors":[{"name":"Generate"}]}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/root/process-groups/import" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		var got ProcessGroupUploadEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if string(got.FlowSnapshot) != string(snapshot) {
			t.Fatalf("snapshot = %s, want %s", got.FlowSnapshot, snapshot)
		}
		if got.GroupName != "Payments" {
			t.Fatalf("group name = %q, want Payments", got.GroupName)
		}
		if got.Revision.Version != 0 {
			t.Fatalf("revision = %d, want 0", got.Revision.Version)
		}
		_ = json.NewEncoder(w).Encode(ProcessGroupEntity{ID: "pg-imported", Revision: Revision{Version: 3}})
	}))
	defer server.Close()

	imported, err := (HTTPFlowSnapshotClient{}).ImportProcessGroup(t.Context(), server.URL, "root", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if imported.ID != "pg-imported" || imported.Revision.Version != 3 {
		t.Fatalf("imported = %#v", imported)
	}
}

func TestHTTPFlowSnapshotClientReplaceRequestLifecycle(t *testing.T) {
	snapshot := json.RawMessage(`{"flowContents":{"name":"Payments"}}`)
	requestCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/nifi-api/process-groups/pg-1/replace-requests":
			requestCalls++
			var got ProcessGroupImportEntity
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got.ProcessGroupRevision == nil || got.ProcessGroupRevision.Version != 12 {
				t.Fatalf("revision = %#v, want 12", got.ProcessGroupRevision)
			}
			_ = json.NewEncoder(w).Encode(ProcessGroupReplaceRequestEntity{Request: ProcessGroupReplaceRequest{RequestID: "replace-1", State: "Running"}})
		case r.Method == http.MethodGet && r.URL.Path == "/nifi-api/process-groups/replace-requests/replace-1":
			requestCalls++
			_ = json.NewEncoder(w).Encode(ProcessGroupReplaceRequestEntity{Request: ProcessGroupReplaceRequest{RequestID: "replace-1", State: "Complete", Complete: true, PercentCompleted: 100}})
		case r.Method == http.MethodDelete && r.URL.Path == "/nifi-api/process-groups/replace-requests/replace-1":
			requestCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := HTTPFlowSnapshotClient{}
	created, err := client.CreateProcessGroupReplaceRequest(t.Context(), server.URL, "pg-1", 12, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := client.GetProcessGroupReplaceRequest(t.Context(), server.URL, created.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if !observed.Request.Complete || observed.Request.PercentCompleted != 100 {
		t.Fatalf("observed request = %#v", observed.Request)
	}
	if err := client.DeleteProcessGroupReplaceRequest(t.Context(), server.URL, created.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if requestCalls != 3 {
		t.Fatalf("request calls = %d, want 3", requestCalls)
	}
}

func TestHTTPFlowSnapshotClientDeleteMissingReplaceRequestIsIdempotent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "request no longer exists", http.StatusNotFound)
	}))
	defer server.Close()

	if err := (HTTPFlowSnapshotClient{}).DeleteProcessGroupReplaceRequest(t.Context(), server.URL, "replace-1"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPControllerServiceClientCreateControllerService(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1/controller-services" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1/controller-services", r.URL.Path)
		}
		var got ControllerServiceEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Type != "org.apache.nifi.dbcp.DBCPConnectionPool" {
			t.Fatalf("type = %q", got.Component.Type)
		}
		_ = json.NewEncoder(w).Encode(ControllerServiceEntity{
			ID:       "controller-service-1",
			Revision: Revision{Version: 0},
			Component: ControllerServiceComponent{
				ID:               "controller-service-1",
				Name:             got.Component.Name,
				ValidationStatus: "VALID",
			},
		})
	}))
	defer server.Close()

	created, err := (HTTPControllerServiceClient{}).CreateControllerService(t.Context(), server.URL, "pg-1", ControllerServiceEntity{
		Component: ControllerServiceComponent{
			Name: "dbcp",
			Type: "org.apache.nifi.dbcp.DBCPConnectionPool",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "controller-service-1" {
		t.Fatalf("created id = %q, want controller-service-1", created.ID)
	}
	if created.Component.ValidationStatus != "VALID" {
		t.Fatalf("validation status = %q, want VALID", created.Component.ValidationStatus)
	}
}

func TestHTTPControllerServiceClientUpdateControllerService(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller-services/controller-service-1" {
			t.Fatalf("path = %s, want /nifi-api/controller-services/controller-service-1", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ControllerServiceEntity{ID: "controller-service-1", Revision: Revision{Version: 2}})
	}))
	defer server.Close()

	got, err := (HTTPControllerServiceClient{}).UpdateControllerService(t.Context(), server.URL, ControllerServiceEntity{ID: "controller-service-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision.Version != 2 {
		t.Fatalf("revision = %d, want 2", got.Revision.Version)
	}
}

func TestHTTPControllerServiceClientDeleteControllerService(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller-services/controller-service-1" {
			t.Fatalf("path = %s, want /nifi-api/controller-services/controller-service-1", r.URL.Path)
		}
		if r.URL.Query().Get("version") != "12" {
			t.Fatalf("version = %q, want 12", r.URL.Query().Get("version"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := (HTTPControllerServiceClient{}).DeleteControllerService(t.Context(), server.URL, "controller-service-1", 12); err != nil {
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

func TestHTTPInputPortClientCreateInputPort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1/input-ports" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1/input-ports", r.URL.Path)
		}
		var got PortEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Name != "payments-in" {
			t.Fatalf("name = %q, want payments-in", got.Component.Name)
		}
		_ = json.NewEncoder(w).Encode(PortEntity{ID: "input-1", Revision: Revision{Version: 0}})
	}))
	defer server.Close()

	created, err := (HTTPInputPortClient{}).CreateInputPort(t.Context(), server.URL, "pg-1", PortEntity{
		Component: PortComponent{Name: "payments-in"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "input-1" {
		t.Fatalf("created id = %q, want input-1", created.ID)
	}
}

func TestHTTPOutputPortClientCreateOutputPort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1/output-ports" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1/output-ports", r.URL.Path)
		}
		var got PortEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Name != "payments-out" {
			t.Fatalf("name = %q, want payments-out", got.Component.Name)
		}
		_ = json.NewEncoder(w).Encode(PortEntity{ID: "output-1", Revision: Revision{Version: 0}})
	}))
	defer server.Close()

	created, err := (HTTPOutputPortClient{}).CreateOutputPort(t.Context(), server.URL, "pg-1", PortEntity{
		Component: PortComponent{Name: "payments-out"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "output-1" {
		t.Fatalf("created id = %q, want output-1", created.ID)
	}
}

func TestHTTPConnectionClientCreateConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/nifi-api/process-groups/pg-1/connections" {
			t.Fatalf("path = %s, want /nifi-api/process-groups/pg-1/connections", r.URL.Path)
		}
		var got ConnectionEntity
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.Component.Source.ID != "processor-1" || got.Component.Destination.ID != "output-1" {
			t.Fatalf("source/destination = %q/%q", got.Component.Source.ID, got.Component.Destination.ID)
		}
		_ = json.NewEncoder(w).Encode(ConnectionEntity{ID: "connection-1", Revision: Revision{Version: 0}})
	}))
	defer server.Close()

	created, err := (HTTPConnectionClient{}).CreateConnection(t.Context(), server.URL, "pg-1", ConnectionEntity{
		Component: ConnectionComponent{
			Source:      Connectable{ID: "processor-1", Type: "PROCESSOR"},
			Destination: Connectable{ID: "output-1", Type: "OUTPUT_PORT"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != "connection-1" {
		t.Fatalf("created id = %q, want connection-1", created.ID)
	}
}
