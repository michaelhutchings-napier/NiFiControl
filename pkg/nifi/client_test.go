package nifi

import "testing"

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
