package migrations

import "embed"

// Files contains the immutable, forward-only database migrations.
//
//go:embed *.surql
var Files embed.FS
