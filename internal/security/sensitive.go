package security

import (
	"regexp"
	"strings"
)

var credentialPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----`),
	regexp.MustCompile(`\bMII[A-Za-z0-9+/=]{20,}\b`),
	regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`),
	regexp.MustCompile(`(?i)authorization\s*:\s*(?:bearer|basic)\s+[^\s'";]+`),
	regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password|passwd|access[_-]?key)\s*[:=]\s*["']?[A-Za-z0-9_./+=-]{8,}`),
	regexp.MustCompile(`\b(?:sk[-_](?:live[-_]|test[-_])?|api[-_]|tok[-_]|key[-_])[A-Za-z0-9_-]{12,}\b`),
	regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`),
}

// ContainsCredentialMaterial is intentionally conservative and does not use a
// generic entropy heuristic: verifier evidence legitimately contains hashes.
func ContainsCredentialMaterial(value string) bool {
	for _, pattern := range credentialPatterns {
		if pattern.MatchString(value) {
			return true
		}
	}
	return strings.IndexFunc(value, func(r rune) bool {
		return (r < 0x20 && r != '\t' && r != '\n') || (r >= 0x7f && r <= 0x9f)
	}) >= 0
}
