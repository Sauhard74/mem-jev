//go:build integration

package testinfra

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

func StartSurreal(t *testing.T, image string) *surrealdb.DB {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm",
		"--publish", "127.0.0.1::8000", image,
		"start", "--log", "warn", "--user", "root", "--pass", "root", "memory")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("start SurrealDB container: %v: %s", err, strings.TrimSpace(string(output)))
	}
	containerID := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		_ = exec.CommandContext(stopCtx, "docker", "stop", "--time", "2", containerID).Run()
	})

	var portOutput []byte
	deadline := time.Now().Add(90 * time.Second)
	for {
		portOutput, err = exec.CommandContext(ctx, "docker", "port", containerID, "8000/tcp").CombinedOutput()
		if err == nil && strings.TrimSpace(string(portOutput)) != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("resolve SurrealDB port: %v: %s", err, strings.TrimSpace(string(portOutput)))
		}
		time.Sleep(50 * time.Millisecond)
	}
	address := strings.TrimSpace(string(portOutput))
	if newline := strings.IndexByte(address, '\n'); newline >= 0 {
		address = address[:newline]
	}

	healthURL := "http://" + address + "/health"
	client := &http.Client{Timeout: time.Second}
	deadline = time.Now().Add(90 * time.Second)
	for {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			logs, _ := exec.CommandContext(ctx, "docker", "logs", containerID).CombinedOutput()
			t.Fatalf("SurrealDB did not become healthy: %v: %s", requestErr, strings.TrimSpace(string(logs)))
		}
		time.Sleep(100 * time.Millisecond)
	}

	db, err := surrealdb.FromEndpointURLString(ctx, "ws://"+address)
	if err != nil {
		t.Fatalf("connect to SurrealDB: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = db.Close(closeCtx)
	})
	if _, err = db.SignIn(ctx, map[string]any{"user": "root", "pass": "root"}); err != nil {
		t.Fatalf("sign in to SurrealDB: %v", err)
	}
	namespace := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if err = db.Use(ctx, namespace, "memjev"); err != nil {
		t.Fatalf("select SurrealDB namespace/database: %v", err)
	}
	return db
}
