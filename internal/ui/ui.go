// Package ui embeds and serves the web frontend dashboard for logengine.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed assets/*
var assets embed.FS

// Handler returns an http.Handler that serves the embedded web dashboard.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, "assets")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
