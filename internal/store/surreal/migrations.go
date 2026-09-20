package surreal

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	dbmigrations "github.com/sauhard74/mem-jev/db/migrations"
	"github.com/sauhard74/mem-jev/internal/store"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

type Migrator struct {
	db         *surrealdb.DB
	migrations []store.Migration
	loadErr    error
}

func NewMigrator(db *surrealdb.DB) *Migrator {
	migrations, err := loadMigrations()
	if err != nil {
		return &Migrator{db: db, loadErr: err}
	}
	return newMigrator(db, migrations)
}

func newMigrator(db *surrealdb.DB, migrations []store.Migration) *Migrator {
	copyOfMigrations := append([]store.Migration(nil), migrations...)
	return &Migrator{db: db, migrations: copyOfMigrations}
}

func (m *Migrator) Apply(ctx context.Context) error {
	if m.db == nil {
		return errors.New("migration database is nil")
	}
	if m.loadErr != nil {
		return m.loadErr
	}
	if err := validateMigrationSequence(m.migrations); err != nil {
		return err
	}
	applied, err := m.applied(ctx)
	if err != nil {
		return fmt.Errorf("read applied migrations: %w", err)
	}
	if err := verifyAppliedMigrations(m.migrations, applied); err != nil {
		return err
	}
	for _, migration := range m.migrations {
		if _, ok := applied[migration.Version]; ok {
			continue
		}
		if err := m.applyWithConflictRetry(ctx, migration); err != nil {
			return err
		}
	}
	return nil
}

func verifyAppliedMigrations(migrations []store.Migration, applied map[int]string) error {
	for version, checksum := range applied {
		if version <= 0 || version > len(migrations) || migrations[version-1].Version != version {
			return fmt.Errorf("%w: database has unknown version %d", store.ErrMigrationOrder, version)
		}
		if migrations[version-1].Checksum != checksum {
			return fmt.Errorf("%w: version %d", store.ErrMigrationChecksum, version)
		}
	}
	return nil
}

func (m *Migrator) applyWithConflictRetry(ctx context.Context, migration store.Migration) error {
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		applied, err := m.applied(ctx)
		if err != nil {
			return fmt.Errorf("refresh migration state: %w", err)
		}
		if checksum, ok := applied[migration.Version]; ok {
			if checksum != migration.Checksum {
				return fmt.Errorf("%w: version %d", store.ErrMigrationChecksum, migration.Version)
			}
			return nil
		}
		lastErr = m.applyOne(ctx, migration)
		if lastErr == nil {
			return nil
		}
		if !surrealdb.IsTransactionConflict(lastErr) {
			return lastErr
		}
		delay := min(10*time.Millisecond<<attempt, 250*time.Millisecond)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("migration %d exceeded conflict retries: %w", migration.Version, lastErr)
}

type migrationRecord struct {
	Version  int    `json:"version"`
	Checksum string `json:"checksum"`
}

func (m *Migrator) applied(ctx context.Context) (map[int]string, error) {
	exists, err := m.migrationTableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return make(map[int]string), nil
	}
	results, err := surrealdb.Query[[]migrationRecord](ctx, m.db,
		"SELECT version, checksum FROM schema_migration ORDER BY version ASC", nil)
	if err != nil {
		return nil, err
	}
	applied := make(map[int]string)
	if results == nil {
		return applied, nil
	}
	for _, result := range *results {
		for _, record := range result.Result {
			applied[record.Version] = record.Checksum
		}
	}
	return applied, nil
}

func (m *Migrator) migrationTableExists(ctx context.Context) (bool, error) {
	results, err := surrealdb.Query[map[string]any](ctx, m.db, "INFO FOR DB", nil)
	if err != nil {
		return false, err
	}
	if results == nil || len(*results) == 0 {
		return false, nil
	}
	tables, ok := (*results)[0].Result["tables"].(map[string]any)
	if !ok {
		return false, nil
	}
	_, ok = tables["schema_migration"]
	return ok, nil
}

func (m *Migrator) applyOne(ctx context.Context, migration store.Migration) (err error) {
	tx, err := m.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", migration.Version, err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if _, err = surrealdb.Query[any](ctx, tx, migration.Statements, nil); err != nil {
		return fmt.Errorf("execute migration %d: %w", migration.Version, err)
	}
	_, err = surrealdb.Query[any](ctx, tx, `
		CREATE ONLY schema_migration CONTENT {
			version: $version,
			name: $name,
			checksum: $checksum,
			applied_at: $applied_at
		}`, map[string]any{
		"version":    migration.Version,
		"name":       migration.Name,
		"checksum":   migration.Checksum,
		"applied_at": time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("record migration %d: %w", migration.Version, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d: %w", migration.Version, err)
	}
	return nil
}

func loadMigrations() ([]store.Migration, error) {
	entries, err := fs.ReadDir(dbmigrations.Files, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	migrations := make([]store.Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".surql") {
			continue
		}
		separator := strings.IndexByte(entry.Name(), '_')
		if separator <= 0 {
			return nil, fmt.Errorf("%w: invalid filename %q", store.ErrMigrationOrder, entry.Name())
		}
		version, parseErr := strconv.Atoi(entry.Name()[:separator])
		if parseErr != nil || version <= 0 {
			return nil, fmt.Errorf("%w: invalid filename %q", store.ErrMigrationOrder, entry.Name())
		}
		body, readErr := dbmigrations.Files.ReadFile(entry.Name())
		if readErr != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), readErr)
		}
		migrations = append(migrations, store.NewMigration(version, entry.Name(), string(body)))
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	return migrations, validateMigrationSequence(migrations)
}

func validateMigrationSequence(migrations []store.Migration) error {
	if len(migrations) == 0 {
		return fmt.Errorf("%w: no migrations", store.ErrMigrationOrder)
	}
	for index, migration := range migrations {
		if migration.Version != index+1 {
			return fmt.Errorf("%w: version %d follows position %d", store.ErrMigrationOrder, migration.Version, index)
		}
		if !migration.ValidChecksum() {
			return fmt.Errorf("%w: version %d", store.ErrMigrationChecksum, migration.Version)
		}
	}
	return nil
}
