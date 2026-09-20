package surreal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

var ErrInvalidConfig = errors.New("invalid SurrealDB configuration")

type AuthScope string

const (
	AuthScopeRoot      AuthScope = "root"
	AuthScopeNamespace AuthScope = "namespace"
	AuthScopeDatabase  AuthScope = "database"
)

type Config struct {
	Endpoint  string
	Namespace string
	Database  string
	Username  string
	Password  string
	AuthScope AuthScope
}

func Open(ctx context.Context, config Config) (*surrealdb.DB, error) {
	if strings.TrimSpace(config.Endpoint) == "" ||
		strings.TrimSpace(config.Namespace) == "" ||
		strings.TrimSpace(config.Database) == "" ||
		strings.TrimSpace(config.Username) == "" ||
		config.Password == "" ||
		(config.AuthScope != AuthScopeRoot && config.AuthScope != AuthScopeNamespace && config.AuthScope != AuthScopeDatabase) {
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
	auth, err := authenticationData(config)
	if err != nil {
		return closeOnError(err)
	}
	if _, err = db.SignIn(ctx, auth); err != nil {
		return closeOnError(fmt.Errorf("authenticate SurrealDB: %w", err))
	}
	if err = db.Use(ctx, config.Namespace, config.Database); err != nil {
		return closeOnError(fmt.Errorf("select SurrealDB namespace/database: %w", err))
	}
	return db, nil
}

func authenticationData(config Config) (surrealdb.Auth, error) {
	auth := surrealdb.Auth{Username: config.Username, Password: config.Password}
	switch config.AuthScope {
	case AuthScopeRoot:
	case AuthScopeNamespace:
		auth.Namespace = config.Namespace
	case AuthScopeDatabase:
		auth.Namespace = config.Namespace
		auth.Database = config.Database
	default:
		return surrealdb.Auth{}, ErrInvalidConfig
	}
	return auth, nil
}
