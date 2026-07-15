package controller

import (
	"testing"

	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

// These tests pin the fix for the perpetual-update bug: NiFi's read view of a component always
// carries descriptor defaults for keys we never set, returns sensitive values masked (null), and
// fills in the resolved NAR bundle we left unset, so comparing the full property maps (or a nil
// desired bundle against NiFi's resolved one) registered drift on every reconcile and updated the
// component forever (observed as an exploding revision.version). The comparators must instead check
// only what NiFiControl manages: desired properties (skipping sensitive keys), tolerating NiFi's
// extra defaults, and a bundle only when the CR actually pinned one.

// resolvedBundle is what NiFi returns for a component whose CR left the bundle unset.
var resolvedBundle = &nifi.Bundle{Group: "org.apache.nifi", Artifact: "nifi-standard-nar", Version: "2.10.0"}

func TestBundleDiffersIgnoresResolvedBundleWhenUnset(t *testing.T) {
	if bundleDiffers(nil, resolvedBundle) {
		t.Fatal("an unset desired bundle must not differ from NiFi's resolved bundle")
	}
	if bundleDiffers(nil, nil) {
		t.Fatal("nil/nil must not differ")
	}
	pinned := &nifi.Bundle{Group: "org.apache.nifi", Artifact: "nifi-standard-nar", Version: "2.10.0"}
	if bundleDiffers(pinned, resolvedBundle) {
		t.Fatal("a pinned bundle equal to NiFi's must not differ")
	}
	if !bundleDiffers(&nifi.Bundle{Group: "com.example", Artifact: "custom-nar", Version: "1.0.0"}, resolvedBundle) {
		t.Fatal("a pinned bundle that differs from NiFi's must be detected")
	}
}

func TestManagedPropertiesDifferIgnoresDefaultsAndSensitive(t *testing.T) {
	desired := map[string]string{"Region": "eu-west-2", "Secret Key": "s3cr3t"}
	// What NiFi returns on read: our managed non-sensitive value, the sensitive value masked (null ->
	// absent/empty), plus descriptor defaults we never set.
	existing := map[string]string{"Region": "eu-west-2", "Secret Key": "", "Timeout": "30 secs", "Batch Size": "100"}
	sensitive := map[string]bool{"Secret Key": true}

	if managedPropertiesDiffer(desired, existing, sensitive) {
		t.Fatal("should not differ: only the masked sensitive value and NiFi defaults differ")
	}
	// A genuine change to a managed non-sensitive property must be detected.
	existing["Region"] = "us-east-1"
	if !managedPropertiesDiffer(desired, existing, sensitive) {
		t.Fatal("should differ when a managed non-sensitive property differs")
	}
}

func TestReportingTaskNeedsUpdateIgnoresSensitiveAndDefaults(t *testing.T) {
	desired := nifi.ReportingTaskEntity{Component: nifi.ReportingTaskComponent{
		Name: "mem", Type: "T", Properties: map[string]string{"Threshold": "90%", "Password": "p"},
	}}
	existing := nifi.ReportingTaskEntity{Component: nifi.ReportingTaskComponent{
		Name: "mem", Type: "T", Bundle: resolvedBundle, Properties: map[string]string{"Threshold": "90%", "Password": "", "Frequency": "5 mins"},
	}}
	sensitive := map[string]bool{"Password": true}

	if reportingTaskNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("reporting task should not need update from masked sensitive + NiFi defaults alone")
	}
	existing.Component.Properties["Threshold"] = "50%"
	if !reportingTaskNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("reporting task should need update when a managed property differs")
	}
}

func TestControllerServiceNeedsUpdateIgnoresSensitiveAndDefaults(t *testing.T) {
	desired := nifi.ControllerServiceEntity{Component: nifi.ControllerServiceComponent{
		Name: "cs", Type: "T", Properties: map[string]string{"Database URL": "jdbc:...", "Password": "p"},
	}}
	existing := nifi.ControllerServiceEntity{Component: nifi.ControllerServiceComponent{
		Name: "cs", Type: "T", Bundle: resolvedBundle, Properties: map[string]string{"Database URL": "jdbc:...", "Password": "", "Max Connections": "8"},
	}}
	sensitive := map[string]bool{"Password": true}

	if controllerServiceNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("controller service should not need update from masked sensitive + NiFi defaults alone")
	}
	existing.Component.Properties["Database URL"] = "jdbc:other"
	if !controllerServiceNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("controller service should need update when a managed property differs")
	}
}

func TestParameterProviderNeedsUpdateIgnoresSensitiveAndDefaults(t *testing.T) {
	desired := nifi.ParameterProviderEntity{Component: nifi.ParameterProviderComponent{
		Name: "aws", Type: "T", Properties: map[string]string{"region": "eu-west-2", "secret-key": "s"},
	}}
	existing := nifi.ParameterProviderEntity{Component: nifi.ParameterProviderComponent{
		Name: "aws", Type: "T", Bundle: resolvedBundle, Properties: map[string]string{"region": "eu-west-2", "secret-key": "", "verification-level": "None"},
	}}
	sensitive := map[string]bool{"secret-key": true}

	if parameterProviderNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("parameter provider should not need update from masked sensitive + NiFi defaults alone")
	}
	existing.Component.Properties["region"] = "us-east-1"
	if !parameterProviderNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("parameter provider should need update when a managed property differs")
	}
}

func TestProcessorNeedsUpdateIgnoresSensitiveAndDefaults(t *testing.T) {
	desired := nifi.ProcessorEntity{Component: nifi.ProcessorComponent{
		Name: "p", Type: "T", Config: nifi.ProcessorConfig{Properties: map[string]string{"URL": "http://x", "Token": "t"}},
	}}
	existing := nifi.ProcessorEntity{Component: nifi.ProcessorComponent{
		Name: "p", Type: "T", Bundle: resolvedBundle, Config: nifi.ProcessorConfig{Properties: map[string]string{"URL": "http://x", "Token": "", "Connect Timeout": "5 secs", "Read Timeout": "15 secs"}},
	}}
	sensitive := map[string]bool{"Token": true}

	if processorNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("processor should not need update from masked sensitive + NiFi defaults alone")
	}
	existing.Component.Config.Properties["URL"] = "http://y"
	if !processorNeedsUpdate(desired, existing, sensitive) {
		t.Fatal("processor should need update when a managed property differs")
	}
}
