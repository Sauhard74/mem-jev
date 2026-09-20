package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var (
	ErrMigrationChecksum = errors.New("database migration checksum mismatch")
	ErrMigrationOrder    = errors.New("database migration order is invalid")
)

type Migrator interface {
	Apply(context.Context) error
}

type Migration struct {
	Version    int
	Name       string
	Checksum   string
	Statements string
}

func NewMigration(version int, name, statements string) Migration {
	sum := sha256.Sum256([]byte(statements))
	return Migration{
		Version:    version,
		Name:       name,
		Checksum:   hex.EncodeToString(sum[:]),
		Statements: statements,
	}
}

func (m Migration) ValidChecksum() bool {
	sum := sha256.Sum256([]byte(m.Statements))
	return m.Checksum == hex.EncodeToString(sum[:])
}
