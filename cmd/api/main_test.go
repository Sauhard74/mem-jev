package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
)

func TestAPIProcess(t *testing.T) {
	if os.Getenv("GO_WANT_MEMJEV_API_PROCESS") != "1" {
		return
	}
	os.Exit(run())
}

func TestMemoryAPIStartsReadyAndShutsDownOnSignal(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	tokenHash := sha256.Sum256([]byte("token-aaaaaaaaaaaaaaaa"))
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	credentials := fmt.Sprintf(`{"credentials":[{"credential_sha256":"%s","tenant_id":"tenant_a","region":"local","scopes":["ingest:write"],"consent":"learn_and_recall"}]}`, hex.EncodeToString(tokenHash[:]))
	if err := os.WriteFile(credentialsPath, []byte(credentials), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestAPIProcess$")
	environment := make([]string, 0, len(os.Environ())+6)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "MEMJEV_") && !strings.HasPrefix(entry, "GO_WANT_MEMJEV_API_PROCESS=") {
			environment = append(environment, entry)
		}
	}
	command.Env = append(environment,
		"GO_WANT_MEMJEV_API_PROCESS=1",
		"MEMJEV_ENVIRONMENT=development",
		"MEMJEV_ADAPTER_MODE=memory",
		"MEMJEV_LISTEN_ADDR="+address,
		"MEMJEV_CREDENTIALS_FILE="+credentialsPath,
		"MEMJEV_SHUTDOWN_TIMEOUT=2s",
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
		}
	})

	client := memjevv1connect.NewHealthServiceClient(http.DefaultClient, "http://"+address)
	deadline := time.Now().Add(8 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		response, checkErr := client.Check(ctx, connect.NewRequest(&memjevv1.CheckRequest{}))
		cancel()
		if checkErr == nil && response.Msg.GetStatus() == memjevv1.CheckResponse_STATUS_SERVING {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			t.Fatalf("API did not become ready: %v", checkErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("API shutdown: %v", err)
	}
}
