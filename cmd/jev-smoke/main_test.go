package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRejectsMissingSecretWithoutPrintingArguments(t *testing.T) {
	var output bytes.Buffer
	secret := "must-not-be-printed"
	code := run([]string{"-model", "jev-1.13.0", "-key-file", secret}, &output)
	if code == 0 || strings.Contains(output.String(), secret) {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestRunRejectsUnpinnedModelBeforeProviderCall(t *testing.T) {
	var output bytes.Buffer
	code := run([]string{"-model", "latest", "-key-file", "/does/not/exist"}, &output)
	if code == 0 || !strings.Contains(output.String(), "configuration rejected") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}
