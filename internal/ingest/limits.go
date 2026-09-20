package ingest

type SanitizerPolicy struct {
	AllowedFields map[string]struct{}
	MaxValueBytes int
	MaxTaskBytes  int
	MaxTotalBytes int
}

func DefaultPolicy() SanitizerPolicy {
	return SanitizerPolicy{
		AllowedFields: map[string]struct{}{
			"command":       {},
			"path":          {},
			"url_host":      {},
			"query_shape":   {},
			"resource_type": {},
			"resource_id":   {},
			"assertion":     {},
		},
		MaxValueBytes: 64 << 10,
		MaxTaskBytes:  256 << 10,
		MaxTotalBytes: 1 << 20,
	}
}
