package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func loggingTestCluster(logging *nifiv1alpha1.NiFiClusterLoggingSpec) *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "edge", Namespace: "default"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:    nifiv1alpha1.ClusterModeInternal,
			Logging: logging,
		},
	}
}

// countRootLevel counts <root level="X"> occurrences; the baseline has two root blocks.
func countRootLevel(doc, level string) int {
	return strings.Count(doc, `<root level="`+level+`">`)
}

func TestRenderLogbackDefaultsEqualBaseline(t *testing.T) {
	// spec.logging with only the defaulted level renders back to the shipped baseline, so an
	// otherwise-empty section does not silently diverge from the image.
	if got := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{Level: "INFO"}); got != logbackBaseline {
		t.Fatalf("default logging should equal the baseline; diff present")
	}
}

func TestRenderLogbackRootLevel(t *testing.T) {
	doc := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{Level: "DEBUG"})
	if countRootLevel(doc, "DEBUG") != 2 {
		t.Fatalf("expected both root blocks at DEBUG, got %d", countRootLevel(doc, "DEBUG"))
	}
	if countRootLevel(doc, "INFO") != 0 {
		t.Fatalf("expected no INFO root blocks left")
	}
	// The library noise-suppression loggers must survive so DEBUG stays useful.
	if !strings.Contains(doc, `<logger name="org.springframework" level="ERROR"/>`) {
		t.Fatal("expected the Spring noise-suppression logger to be preserved")
	}
	if !strings.Contains(doc, `<appender name="DEPRECATION_FILE"`) {
		t.Fatal("expected the deprecation appender to be preserved")
	}
}

func TestRenderLogbackLoggerLevels(t *testing.T) {
	doc := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{
		Level: "INFO",
		Loggers: map[string]string{
			"org.apache.nifi.web.security": "DEBUG", // existing multi-line logger: level swapped in place
			"com.example.custom":           "TRACE", // unknown: injected
		},
	})
	// Existing logger keeps its additivity/appender body, only the level changes.
	if !strings.Contains(doc, `<logger name="org.apache.nifi.web.security" level="DEBUG" additivity="false">`) {
		t.Fatalf("expected in-place level change for org.apache.nifi.web.security")
	}
	if strings.Contains(doc, `<logger name="org.apache.nifi.web.security" level="INFO"`) {
		t.Fatal("old INFO level for org.apache.nifi.web.security should be gone")
	}
	// Prefix disambiguation: the shorter org.apache.nifi logger is untouched.
	if !strings.Contains(doc, `<logger name="org.apache.nifi" level="INFO"/>`) {
		t.Fatal("expected org.apache.nifi to stay INFO (prefix must not match)")
	}
	// Unknown logger injected once.
	if n := strings.Count(doc, `<logger name="com.example.custom" level="TRACE"/>`); n != 1 {
		t.Fatalf("expected the unknown logger injected exactly once, got %d", n)
	}
}

func TestRenderLogbackInjectsLoggerWithNonInfoRoot(t *testing.T) {
	// Regression: injecting a new logger must not depend on the root staying at INFO — the root
	// level is rewritten before loggers are processed, so the injection anchor has to survive it.
	doc := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{
		Level:   "DEBUG",
		Loggers: map[string]string{"com.example.custom": "TRACE"},
	})
	if n := strings.Count(doc, `<logger name="com.example.custom" level="TRACE"/>`); n != 1 {
		t.Fatalf("expected the unknown logger injected once even with a non-INFO root, got %d", n)
	}
	if countRootLevel(doc, "DEBUG") != 2 {
		t.Fatalf("expected both roots at DEBUG alongside the injected logger")
	}
}

func TestRenderLogbackConsole(t *testing.T) {
	off := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{Level: "INFO"})
	if strings.Contains(off, "<appender-ref ref=\"CONSOLE\" />\n    </root>") {
		t.Fatal("console should not be attached to root by default")
	}
	on := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{Level: "INFO", Console: ptr.To(true)})
	appRoot := "    <root level=\"INFO\">\n        <appender-ref ref=\"APP_FILE\" />\n        <appender-ref ref=\"CONSOLE\" />\n    </root>"
	if !strings.Contains(on, appRoot) {
		t.Fatal("expected CONSOLE appended to the APP_FILE root block")
	}
	// The DEDICATED_LOGGING root block must not gain a console ref.
	if strings.Contains(on, "ref=\"DEDICATED_LOGGING\" />\n        <appender-ref ref=\"CONSOLE\"") {
		t.Fatal("console should only attach to the APP_FILE root")
	}
}

func TestRenderLogbackRetentionScopedToAppFile(t *testing.T) {
	doc := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{
		Level: "INFO",
		Retention: &nifiv1alpha1.NiFiClusterLogRetentionSpec{
			MaxFileSize:  "250MB",
			MaxHistory:   ptr.To[int32](7),
			TotalSizeCap: "10GB",
		},
	})
	appFile := doc[strings.Index(doc, `<appender name="APP_FILE"`):]
	appFile = appFile[:strings.Index(appFile, "</appender>")]
	if !strings.Contains(appFile, "<maxFileSize>250MB</maxFileSize>") ||
		!strings.Contains(appFile, "<maxHistory>7</maxHistory>") ||
		!strings.Contains(appFile, "<totalSizeCap>10GB</totalSizeCap>") {
		t.Fatalf("APP_FILE retention not applied: %s", appFile)
	}
	// The deprecation appender keeps its own, smaller defaults — retention is scoped.
	depr := doc[strings.Index(doc, `<appender name="DEPRECATION_FILE"`):]
	depr = depr[:strings.Index(depr, "</appender>")]
	if !strings.Contains(depr, "<maxFileSize>10MB</maxFileSize>") || !strings.Contains(depr, "<maxHistory>10</maxHistory>") {
		t.Fatalf("DEPRECATION_FILE retention should be untouched: %s", depr)
	}
}

func TestRenderLogbackRejectsUnsafeInput(t *testing.T) {
	// Defense in depth: even if a malformed logger name reaches the operator (an older CRD
	// whose CEL predates this field), it is skipped rather than injected into the XML.
	doc := renderManagedClusterLogback(&nifiv1alpha1.NiFiClusterLoggingSpec{
		Level:   "INFO",
		Loggers: map[string]string{`evil"/><script`: "DEBUG"},
	})
	if strings.Contains(doc, "script") {
		t.Fatal("unsafe logger name must not be rendered into logback.xml")
	}
}

func TestLoggingFlowsThroughConfigOverrides(t *testing.T) {
	cluster := loggingTestCluster(&nifiv1alpha1.NiFiClusterLoggingSpec{Level: "WARN"})
	if !hasConfigOverrides(cluster) {
		t.Fatal("spec.logging should count as config overrides so the payload volume mounts")
	}
	resolved, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	logback, ok := resolved.data[overridesLogbackKey]
	if !ok {
		t.Fatal("expected spec.logging to render a logback.xml override entry")
	}
	if countRootLevel(logback, "WARN") != 2 {
		t.Fatalf("expected rendered logback at WARN, got %q root levels", logback)
	}
	if resolved.checksum == "" {
		t.Fatal("expected a checksum for a logging-only override")
	}

	// Changing a logging knob must roll the pods (checksum changes).
	cluster.Spec.Logging.Level = "ERROR"
	changed, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster)
	if err != nil || changed.checksum == resolved.checksum {
		t.Fatalf("checksum should change when the log level changes: %v", err)
	}
}

func TestLoggingAndPropertiesCoexist(t *testing.T) {
	// spec.logging renders logback.xml while spec.configOverrides still contributes
	// nifi.properties — both land in the same payload.
	cluster := loggingTestCluster(&nifiv1alpha1.NiFiClusterLoggingSpec{Level: "INFO"})
	cluster.Spec.ConfigOverrides = &nifiv1alpha1.NiFiClusterConfigOverrides{
		NiFiProperties: map[string]nifiv1alpha1.ConfigOverrideValue{"nifi.queue.swap.threshold": "40000"},
	}
	resolved, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := resolved.data[overridesLogbackKey]; !ok {
		t.Fatal("expected logback.xml from spec.logging")
	}
	if _, ok := resolved.data[overridesNiFiPropertiesKey]; !ok {
		t.Fatal("expected nifi.properties from spec.configOverrides")
	}
}
