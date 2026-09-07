// Package webui embeds the static bootstrap files served by the gateway.
package webui

import (
	"embed"
	"io/fs"
)

//go:embed all:files
var raw embed.FS

// FS returns the embedded filesystem rooted at the project root so paths
// start with "files/..." (preserved from the embedding).
func FS() fs.FS {
	return raw
}

// StripPrefix returns an fs.FS rooted at the directory containing the
// embedded files.  We use this so callers can read "index.html" instead
// of "files/index.html".
func StripPrefix() fs.FS {
	sub, err := fs.Sub(raw, "files")
	if err != nil {
		panic(err)
	}
	return sub
}