package nifi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// NiFi's LabelDTO models width/height as Double, so a real NiFi returns them with a decimal
// (e.g. 200.0). LabelComponent.Width/Height must be float64 or the response fails to unmarshal
// ("cannot unmarshal number 200.0 into Go struct field ... of type int32") and the label never
// becomes ready. This guards that regression.
func TestHTTPLabelClientCreateParsesDecimalWidthHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nifi-api/process-groups/root/labels" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		// Respond exactly as NiFi does: width/height as JSON numbers with a decimal.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"lbl1","revision":{"version":0},"component":{"id":"lbl1","label":"hi","width":200.0,"height":80.0,"position":{"x":100.0,"y":100.0}}}`))
	}))
	defer server.Close()

	created, err := (HTTPLabelClient{}).CreateLabel(t.Context(), server.URL, "root", LabelEntity{
		Component: LabelComponent{Label: "hi", Width: 200, Height: 80},
	})
	if err != nil {
		t.Fatalf("CreateLabel with a decimal width/height response: %v", err)
	}
	if created.Component.Width != 200 || created.Component.Height != 80 {
		t.Fatalf("width/height = %v/%v, want 200/80", created.Component.Width, created.Component.Height)
	}
}
