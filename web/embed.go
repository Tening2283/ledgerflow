// Package web embeds the dashboard into the binary: one artifact to deploy, no separate file server.
package web

import (
	"embed"
	"io/fs"
)

//go:embed static
var files embed.FS

func Static() fs.FS {
	sub, err := fs.Sub(files, "static")
	if err != nil {
		panic(err)
	}
	return sub
}
