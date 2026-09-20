package jev

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var ErrSecretUnavailable = errors.New("Jev secret unavailable")

type SecretSource interface {
	Token() (string, error)
}

type FileSecret struct {
	path         string
	maximumBytes int64
}

func NewFileSecret(path string, maximumBytes int64) (*FileSecret, error) {
	path = strings.TrimSpace(path)
	if path == "" || maximumBytes < 8 || maximumBytes > 64<<10 {
		return nil, ErrSecretUnavailable
	}
	return &FileSecret{path: path, maximumBytes: maximumBytes}, nil
}

func (secret *FileSecret) Token() (string, error) {
	if secret == nil {
		return "", ErrSecretUnavailable
	}
	info, err := os.Lstat(secret.path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() <= 0 || info.Size() > secret.maximumBytes {
		return "", ErrSecretUnavailable
	}
	file, err := os.Open(secret.path)
	if err != nil {
		return "", ErrSecretUnavailable
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(io.LimitReader(file, secret.maximumBytes+1))
	if err != nil || int64(len(body)) > secret.maximumBytes {
		return "", ErrSecretUnavailable
	}
	token := strings.TrimSpace(string(body))
	if len(token) < 8 || strings.IndexFunc(token, func(r rune) bool { return r == '\x00' || r == '\r' || r == '\n' || r == '\t' || r == ' ' }) >= 0 {
		return "", ErrSecretUnavailable
	}
	return token, nil
}

func (secret *FileSecret) String() string { return "FileSecret(redacted)" }

func (secret *FileSecret) GoString() string { return fmt.Sprintf("%T(redacted)", secret) }
