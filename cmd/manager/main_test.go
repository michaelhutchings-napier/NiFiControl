package main

import "testing"

func TestNamespaceCacheConfig(t *testing.T) {
	// Empty (and whitespace/comma-only) means watch all namespaces -> nil (no cache restriction).
	for _, empty := range []string{"", "   ", ",", " , "} {
		if got := namespaceCacheConfig(empty); got != nil {
			t.Errorf("namespaceCacheConfig(%q) = %v, want nil (watch all)", empty, got)
		}
	}
	// A comma-separated list, tolerant of surrounding whitespace, restricts to those namespaces.
	got := namespaceCacheConfig(" team-a , team-b ")
	if len(got) != 2 {
		t.Fatalf("want 2 namespaces, got %d: %v", len(got), got)
	}
	for _, ns := range []string{"team-a", "team-b"} {
		if _, ok := got[ns]; !ok {
			t.Errorf("expected namespace %q to be watched, got %v", ns, got)
		}
	}
}
