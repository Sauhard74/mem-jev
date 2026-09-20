package ingest

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"google.golang.org/protobuf/proto"
)

const redactionMarker = "[REDACTED]"

var (
	privateKeyPattern         = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----`)
	privateKeyMaterialPattern = regexp.MustCompile(`\bMII[A-Za-z0-9+/=]{20,}\b`)
	jwtPattern                = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)
	authorizationPattern      = regexp.MustCompile(`(?i)(authorization\s*:\s*(?:bearer|basic)\s+)[^\s'";]+`)
	assignmentPattern         = regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password|passwd|access[_-]?key)(\s*[:=]\s*["']?)([A-Za-z0-9_./+=-]{8,})`)
	providerTokenPattern      = regexp.MustCompile(`\b(?:sk[-_](?:live[-_]|test[-_])?|api[-_]|tok[-_]|key[-_])[A-Za-z0-9_-]{12,}\b`)
	awsAccessKeyPattern       = regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`)
	entropyTokenPattern       = regexp.MustCompile(`[A-Za-z0-9_+/=-]{32,}`)
)

type SanitizationReport struct {
	Redactions       int            `json:"redactions"`
	ReasonCounts     map[string]int `json:"reason_counts"`
	StrippedControls int            `json:"stripped_controls"`
}

func Sanitize(request *memjevv1.IngestTraceRequest, policy SanitizerPolicy) (*memjevv1.IngestTraceRequest, SanitizationReport, error) {
	report := SanitizationReport{ReasonCounts: make(map[string]int)}
	if request == nil {
		return nil, report, &SanitizationError{Reason: ErrInvalidTrace, Location: "request"}
	}
	if policy.MaxValueBytes <= 0 || policy.MaxTaskBytes <= 0 || policy.MaxTotalBytes <= 0 {
		return nil, report, &SanitizationError{Reason: ErrInvalidTrace, Location: "policy"}
	}
	if proto.Size(request) > policy.MaxTotalBytes {
		return nil, report, &SanitizationError{Reason: ErrTraceTooLarge, Location: "request"}
	}
	if len(request.GetTask()) > policy.MaxTaskBytes {
		return nil, report, &SanitizationError{Reason: ErrValueTooLarge, Location: "task"}
	}
	if err := validateFieldLimits(request, policy); err != nil {
		return nil, report, err
	}

	clean, ok := proto.Clone(request).(*memjevv1.IngestTraceRequest)
	if !ok {
		return nil, report, &SanitizationError{Reason: ErrInvalidTrace, Location: "request"}
	}
	clean.Task = sanitizeValue(clean.GetTask(), &report)
	for _, item := range clean.GetEnvironment() {
		item.StringValue = sanitizeValue(item.GetStringValue(), &report)
	}
	for _, event := range clean.GetEvents() {
		for _, item := range event.GetFields() {
			item.StringValue = sanitizeValue(item.GetStringValue(), &report)
		}
		if event.GetResult() != nil {
			for _, item := range event.GetResult().GetEvidence() {
				item.StringValue = sanitizeValue(item.GetStringValue(), &report)
			}
		}
	}
	return clean, report, nil
}

func validateFieldLimits(request *memjevv1.IngestTraceRequest, policy SanitizerPolicy) error {
	if err := validateFields("environment", request.GetEnvironment(), policy); err != nil {
		return err
	}
	for eventIndex, event := range request.GetEvents() {
		if event == nil {
			return &SanitizationError{Reason: ErrInvalidTrace, Location: fmt.Sprintf("events[%d]", eventIndex)}
		}
		if err := validateFields(fmt.Sprintf("events[%d].fields", eventIndex), event.GetFields(), policy); err != nil {
			return err
		}
		if event.GetResult() != nil {
			if err := validateFields(fmt.Sprintf("events[%d].result.evidence", eventIndex), event.GetResult().GetEvidence(), policy); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateFields(location string, fields []*memjevv1.Field, policy SanitizerPolicy) error {
	for index, item := range fields {
		itemLocation := fmt.Sprintf("%s[%d]", location, index)
		if item == nil {
			return &SanitizationError{Reason: ErrInvalidTrace, Location: itemLocation}
		}
		name := strings.TrimSpace(item.GetName())
		if _, allowed := policy.AllowedFields[name]; !allowed {
			return &SanitizationError{Reason: ErrForbiddenField, Location: itemLocation + ".name"}
		}
		if len(item.GetStringValue()) > policy.MaxValueBytes {
			return &SanitizationError{Reason: ErrValueTooLarge, Location: itemLocation + ".value"}
		}
	}
	return nil
}

func sanitizeValue(value string, report *SanitizationReport) string {
	clean, stripped := stripControls(value)
	report.StrippedControls += stripped
	clean = redactRegexp(clean, privateKeyPattern, "private_key", "[REDACTED:PRIVATE_KEY]", report)
	clean = redactRegexp(clean, privateKeyMaterialPattern, "private_key_material", "[REDACTED:PRIVATE_KEY]", report)
	clean = redactRegexp(clean, jwtPattern, "jwt", "[REDACTED:JWT]", report)
	clean = redactRegexp(clean, authorizationPattern, "authorization", "${1}"+redactionMarker, report)
	clean = redactRegexp(clean, assignmentPattern, "assignment", "${1}${2}"+redactionMarker, report)
	clean = redactRegexp(clean, providerTokenPattern, "provider_token", redactionMarker, report)
	clean = redactRegexp(clean, awsAccessKeyPattern, "aws_access_key", redactionMarker, report)
	clean = entropyTokenPattern.ReplaceAllStringFunc(clean, func(candidate string) string {
		if shannonEntropy(candidate) < 4.2 || !mixedToken(candidate) {
			return candidate
		}
		report.Redactions++
		report.ReasonCounts["high_entropy"]++
		return redactionMarker
	})
	return clean
}

func redactRegexp(value string, pattern *regexp.Regexp, reason, replacement string, report *SanitizationReport) string {
	matches := pattern.FindAllStringIndex(value, -1)
	if len(matches) == 0 {
		return value
	}
	report.Redactions += len(matches)
	report.ReasonCounts[reason] += len(matches)
	return pattern.ReplaceAllString(value, replacement)
}

func stripControls(value string) (string, int) {
	value = strings.ReplaceAll(strings.ReplaceAll(value, "\r\n", "\n"), "\r", "\n")
	var builder strings.Builder
	builder.Grow(len(value))
	stripped := 0
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		value = value[size:]
		if (r < 0x20 && r != '\t' && r != '\n') || (r >= 0x7f && r <= 0x9f) {
			stripped++
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String(), stripped
}

func shannonEntropy(value string) float64 {
	counts := make(map[byte]int)
	for index := 0; index < len(value); index++ {
		counts[value[index]]++
	}
	length := float64(len(value))
	entropy := 0.0
	for _, count := range counts {
		probability := float64(count) / length
		entropy -= probability * math.Log2(probability)
	}
	return entropy
}

func mixedToken(value string) bool {
	var lower, upper, digit bool
	for index := 0; index < len(value); index++ {
		switch {
		case value[index] >= 'a' && value[index] <= 'z':
			lower = true
		case value[index] >= 'A' && value[index] <= 'Z':
			upper = true
		case value[index] >= '0' && value[index] <= '9':
			digit = true
		}
	}
	return digit && (lower || upper)
}
