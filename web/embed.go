package web

import _ "embed"

//go:embed viewer.html
var ViewerHTML []byte

//go:embed admin.html
var AdminHTML []byte
