package controller

import (
	_ "embed"
	"fmt"
	"regexp"
	"sort"
	"strings"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
)

// logbackBaseline is the conf/logback.xml that apache/nifi 2.x ships. spec.logging renders
// the node logging configuration by overlaying its settings onto this baseline, rather than
// synthesizing a document from scratch, so NiFi's carefully tuned defaults survive — the
// deprecation log, the user-vs-app log separation, and the noise suppression for ZooKeeper,
// Spring, Jetty, and friends. Raising the root level to DEBUG therefore stays useful instead
// of unleashing a firehose from those libraries. The baseline tracks the NiFi 2.x default; a
// future image whose shipped logback.xml differs will lag it (spec.configOverrides.logbackXml
// remains the full-control escape hatch). Keep this file byte-for-byte the image default so
// spec.logging with only defaults renders back to it.
//
//go:embed logback_baseline.xml
var logbackBaseline string

// The primary root block writes application logging to nifi-app.log. spec.logging.console
// adds a stdout appender here so those lines also reach `kubectl logs`; the level attribute
// is rewritten to spec.logging.level. The second root block drives NiFi's per-context
// SiftingAppender and only needs its level tracked.
const (
	logbackRootAppFileBlock = `    <root level="INFO">
        <appender-ref ref="APP_FILE" />
    </root>`
	logbackRootDedicatedBlock = `    <root level="INFO">
        <appender-ref ref="DEDICATED_LOGGING" />
    </root>`
)

// logbackLoggerNamePattern and logbackSizePattern mirror the CRD's CEL validation. spec.logging
// carries no Secret-sourced values, so admission already covers valid input; the render still
// filters against these so a logger name or retention size can never inject XML even when the
// operator runs against an older CRD whose CEL rules predate this field.
var (
	logbackLoggerNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.$]+$`)
	logbackSizePattern       = regexp.MustCompile(`^[0-9]+(|KB|MB|GB)$`)
)

// renderManagedClusterLogback overlays spec.logging onto the NiFi 2.x logback baseline and
// returns the resulting conf/logback.xml. The result flows through the same override plumbing
// as spec.configOverrides.logbackXml (which the CRD makes mutually exclusive with spec.logging),
// so removing spec.logging restores the image default on the next rollout. With only defaults
// set (level INFO, no logger overrides, console off, no retention) it returns the baseline
// unchanged.
func renderManagedClusterLogback(logging *nifiv1alpha1.NiFiClusterLoggingSpec) string {
	if logging == nil {
		return logbackBaseline
	}
	level := logging.Level
	if level == "" {
		level = "INFO"
	}

	rendered := logbackBaseline

	// Primary root block: set the level and, when requested, mirror app logging to stdout.
	appRoot := fmt.Sprintf("    <root level=%q>\n        <appender-ref ref=\"APP_FILE\" />\n", level)
	if logging.Console != nil && *logging.Console {
		appRoot += "        <appender-ref ref=\"CONSOLE\" />\n"
	}
	appRoot += "    </root>"
	rendered = strings.Replace(rendered, logbackRootAppFileBlock, appRoot, 1)

	// Second root block (SiftingAppender): level only.
	dedicatedRoot := fmt.Sprintf("    <root level=%q>\n        <appender-ref ref=\"DEDICATED_LOGGING\" />\n    </root>", level)
	rendered = strings.Replace(rendered, logbackRootDedicatedBlock, dedicatedRoot, 1)

	// Per-logger levels: rewrite the level attribute of a logger the baseline already declares,
	// or append a new <logger> before the root blocks when the name is unknown. Iterated in a
	// stable order so the rendered document (and its override checksum) is deterministic.
	names := make([]string, 0, len(logging.Loggers))
	for name := range logging.Loggers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		lvl := logging.Loggers[name]
		if !logbackLoggerNamePattern.MatchString(name) || !isLogbackLevel(lvl) {
			continue
		}
		rendered = setLogbackLoggerLevel(rendered, name, lvl)
	}

	// Retention: scope replacements to the APP_FILE appender so the other log files keep their
	// defaults.
	if logging.Retention != nil {
		rendered = applyLogbackRetention(rendered, logging.Retention)
	}

	return rendered
}

// setLogbackLoggerLevel replaces the level attribute of an existing <logger name="..."> or, when
// the name is not declared in the baseline, inserts a new logger element. Only the level
// attribute on the opening tag is touched, so multi-line loggers with additivity and appender
// references keep their bodies.
func setLogbackLoggerLevel(doc, name, level string) string {
	quoted := regexp.QuoteMeta(name)
	// The closing quote after the name disambiguates prefixes: name="org.apache.nifi" never
	// matches name="org.apache.nifi.processors".
	existing := regexp.MustCompile(`(<logger name="` + quoted + `"[^>]*\blevel=")[^"]*(")`)
	if existing.MatchString(doc) {
		return existing.ReplaceAllString(doc, `${1}`+level+`${2}`)
	}
	logger := fmt.Sprintf("    <logger name=%q level=%q/>\n\n", name, level)
	// Insert before the first root block so loggers stay grouped ahead of the roots. Anchor on
	// the root opening tag rather than a fixed level, since the level has already been rewritten
	// by the time loggers are processed; fall back to </configuration>.
	if idx := strings.Index(doc, `    <root level="`); idx >= 0 {
		return doc[:idx] + logger + doc[idx:]
	}
	if idx := strings.Index(doc, "</configuration>"); idx >= 0 {
		return doc[:idx] + logger + doc[idx:]
	}
	return doc
}

// applyLogbackRetention rewrites the rolling-policy sizes inside the APP_FILE appender only.
// maxFileSize and totalSizeCap appear in several appenders; scoping to the APP_FILE block keeps
// the deprecation, user, and bootstrap logs on their own defaults.
func applyLogbackRetention(doc string, retention *nifiv1alpha1.NiFiClusterLogRetentionSpec) string {
	start := strings.Index(doc, `<appender name="APP_FILE"`)
	if start < 0 {
		return doc
	}
	end := strings.Index(doc[start:], "</appender>")
	if end < 0 {
		return doc
	}
	end += start
	block := doc[start:end]
	if retention.MaxFileSize != "" && logbackSizePattern.MatchString(retention.MaxFileSize) {
		block = replaceLogbackTag(block, "maxFileSize", retention.MaxFileSize)
	}
	if retention.MaxHistory != nil {
		block = replaceLogbackTag(block, "maxHistory", fmt.Sprintf("%d", *retention.MaxHistory))
	}
	if retention.TotalSizeCap != "" && logbackSizePattern.MatchString(retention.TotalSizeCap) {
		block = replaceLogbackTag(block, "totalSizeCap", retention.TotalSizeCap)
	}
	return doc[:start] + block + doc[end:]
}

func replaceLogbackTag(block, tag, value string) string {
	re := regexp.MustCompile(`(<` + tag + `>)[^<]*(</` + tag + `>)`)
	return re.ReplaceAllString(block, `${1}`+value+`${2}`)
}

func isLogbackLevel(level string) bool {
	switch level {
	case "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "OFF":
		return true
	default:
		return false
	}
}
