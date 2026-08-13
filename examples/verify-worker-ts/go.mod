// This module contains no Go code. It exists purely as a module boundary so
// `go build ./...` / `go vet ./...` / `go test ./...` run from examples/ (a
// real Go module) don't recurse into node_modules and trip over vendored .go
// files bundled inside npm dependencies (e.g. @temporalio/core-bridge vendors
// a full checkout of go.temporal.io/api, including its own Go tooling).
module github.com/02strich/temporal-untrusted-workers/examples/verify-worker-ts-boundary

go 1.25.4
