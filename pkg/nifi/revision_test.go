package nifi

import (
	"encoding/json"
	"strings"
	"testing"
)

// NiFi requires an explicit "version": 0 when creating components, so Revision must not
// drop a zero version via omitempty.
func TestRevisionSerializesZeroVersion(t *testing.T) {
	data, err := json.Marshal(ConnectionEntity{Revision: Revision{Version: 0}, Component: ConnectionComponent{}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"version":0`) {
		t.Fatalf("create payload must include version 0, got %s", string(data))
	}
}
