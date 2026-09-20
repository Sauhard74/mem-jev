package buildinfo

type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	BuiltAt string `json:"built_at"`
}

var (
	version = "dev"
	commit  = "unknown"
	builtAt = "unknown"
)

func Current() Info {
	return Info{Version: version, Commit: commit, BuiltAt: builtAt}
}
