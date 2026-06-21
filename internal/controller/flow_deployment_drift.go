package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
)

const maxDriftDifferences = 20

func compareFlowSnapshots(desired []byte, live []byte, ignoredFields []string) (string, string, []string, error) {
	desiredValue, desiredDigest, err := normalizeFlowSnapshot(desired, ignoredFields)
	if err != nil {
		return "", "", nil, fmt.Errorf("normalize desired flow snapshot: %w", err)
	}
	liveValue, liveDigest, err := normalizeFlowSnapshot(live, ignoredFields)
	if err != nil {
		return "", "", nil, fmt.Errorf("normalize live flow snapshot: %w", err)
	}
	differences := []string{}
	collectFlowDifferences(desiredValue, liveValue, "", &differences)
	return desiredDigest, liveDigest, differences, nil
}

func normalizeFlowSnapshot(snapshot []byte, ignoredFields []string) (any, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(snapshot))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, "", err
	}
	normalized, keep := normalizeFlowValue(value, "", ignoredFields)
	if !keep {
		normalized = map[string]any{}
	}
	payload, err := json.Marshal(normalized)
	if err != nil {
		return nil, "", err
	}
	return normalized, fmt.Sprintf("sha256:%x", sha256.Sum256(payload)), nil
}

func normalizeFlowValue(value any, path string, ignoredFields []string) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for key, child := range typed {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			if isVolatileFlowField(path, key) || isDefaultedFlowField(key, child) || flowFieldIgnored(childPath, ignoredFields) {
				continue
			}
			childValue, keep := normalizeFlowValue(child, childPath, ignoredFields)
			if keep {
				normalized[key] = childValue
			}
		}
		return normalized, len(normalized) > 0
	case []any:
		normalized := make([]any, 0, len(typed))
		for _, child := range typed {
			childValue, keep := normalizeFlowValue(child, path, ignoredFields)
			if keep {
				normalized = append(normalized, childValue)
			}
		}
		if identifiableFlowObjects(normalized) {
			sort.SliceStable(normalized, func(i, j int) bool {
				return flowObjectIdentity(normalized[i]) < flowObjectIdentity(normalized[j])
			})
		}
		return normalized, len(normalized) > 0
	case nil:
		return nil, false
	case string:
		return typed, typed != ""
	case json.Number:
		number, err := typed.Float64()
		if err != nil {
			return typed.String(), true
		}
		return number, true
	default:
		return typed, true
	}
}

func isVolatileFlowField(parentPath string, key string) bool {
	if parentPath == "" {
		switch key {
		case "snapshotMetadata", "flow", "bucket":
			return true
		}
	}
	if parentPath == "flowContents" && key == "name" {
		return true
	}
	switch key {
	case "identifier", "instanceIdentifier", "versionedFlowCoordinates", "componentState":
		return true
	default:
		return false
	}
}

func isDefaultedFlowField(key string, value any) bool {
	defaults := map[string]any{
		"defaultFlowFileExpiration":            "0 sec",
		"defaultBackPressureObjectThreshold":   json.Number("10000"),
		"defaultBackPressureDataSizeThreshold": "1 GB",
		"scheduledState":                       "ENABLED",
		"executionEngine":                      "INHERITED",
		"maxConcurrentTasks":                   json.Number("1"),
		"statelessFlowTimeout":                 "1 min",
		"flowFileConcurrency":                  "UNBOUNDED",
		"flowFileOutboundPolicy":               "STREAM_WHEN_AVAILABLE",
		"flowEncodingVersion":                  "1.0",
		"latest":                               false,
	}
	defaultValue, ok := defaults[key]
	return ok && reflect.DeepEqual(value, defaultValue)
}

func flowFieldIgnored(path string, ignoredFields []string) bool {
	for _, configured := range ignoredFields {
		configured = strings.Trim(strings.TrimSpace(configured), ".")
		if configured == "" {
			continue
		}
		configured = strings.TrimPrefix(configured, "component.")
		if path == configured || strings.HasSuffix(path, "."+configured) {
			return true
		}
	}
	return false
}

func identifiableFlowObjects(values []any) bool {
	if len(values) < 2 {
		return false
	}
	for _, value := range values {
		if flowObjectIdentity(value) == "" {
			return false
		}
	}
	return true
}

func flowObjectIdentity(value any) string {
	object, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"identifier", "id", "name"} {
		if identity, ok := object[key].(string); ok && identity != "" {
			return key + ":" + identity
		}
	}
	return ""
}

func collectFlowDifferences(desired any, live any, path string, differences *[]string) {
	if len(*differences) >= maxDriftDifferences || reflect.DeepEqual(desired, live) {
		return
	}
	desiredMap, desiredIsMap := desired.(map[string]any)
	liveMap, liveIsMap := live.(map[string]any)
	if desiredIsMap && liveIsMap {
		keys := map[string]struct{}{}
		for key := range desiredMap {
			keys[key] = struct{}{}
		}
		for key := range liveMap {
			keys[key] = struct{}{}
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		for _, key := range ordered {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			collectFlowDifferences(desiredMap[key], liveMap[key], childPath, differences)
			if len(*differences) >= maxDriftDifferences {
				return
			}
		}
		return
	}
	desiredList, desiredIsList := desired.([]any)
	liveList, liveIsList := live.([]any)
	if desiredIsList && liveIsList {
		if len(desiredList) != len(liveList) {
			*differences = append(*differences, displayFlowDifferencePath(path))
			return
		}
		for index := range desiredList {
			collectFlowDifferences(desiredList[index], liveList[index], fmt.Sprintf("%s[%d]", path, index), differences)
			if len(*differences) >= maxDriftDifferences {
				return
			}
		}
		return
	}
	*differences = append(*differences, displayFlowDifferencePath(path))
}

func displayFlowDifferencePath(path string) string {
	if path == "" {
		return "$"
	}
	return "$." + path
}

func resolvedDriftPolicy(deployment *nifiv1alpha1.NiFiFlowDeployment) nifiv1alpha1.DriftPolicyMode {
	if deployment.Spec.DriftPolicy.Mode == "" {
		return nifiv1alpha1.DriftPolicyWarn
	}
	return deployment.Spec.DriftPolicy.Mode
}
