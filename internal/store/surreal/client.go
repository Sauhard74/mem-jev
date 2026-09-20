package surreal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

var ErrInvalidConfig = errors.New("invalid SurrealDB configuration")

type Config struct {
	Endpoint  string
	Namespace string
	Database  string
	Username  string
	Password  string
}

func Open(ctx context.Context, config Config) (*surrealdb.DB, error) {
	if strings.TrimSpace(config.Endpoint) == "" ||
		strings.TrimSpace(config.Namespace) == "" ||
		strings.TrimSpace(config.Database) == "" ||
		strings.TrimSpace(config.Username) == "" ||
		config.Password == "" {
		return nil, ErrInvalidConfig
	}
	db, err := surrealdb.FromEndpointURLString(ctx, config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("connect SurrealDB: %w", err)
	}
	closeOnError := func(cause error) (*surrealdb.DB, error) {
		_ = db.Close(context.Background())
		return nil, cause
	}
	if _, err = db.SignIn(ctx, map[string]any{
		"user": config.Username,
		"pass": config.Password,
	}); err != nil {
		return closeOnError(fmt.Errorf("authenticate SurrealDB: %w", err))
	}
	if err = db.Use(ctx, config.Namespace, config.Database); err != nil {
		return closeOnError(fmt.Errorf("select SurrealDB namespace/database: %w", err))
	}
	return db, nil
}
