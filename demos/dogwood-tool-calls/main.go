// Command dogwood-tool-calls drives the Dogwood tool-call verification demo
// (see demo.tape; built by up.sh as run/demo). The workers, workflows and
// cases are in internal/toolcalls, shared with cedar-tool-calls.
package main

import (
	"github.com/02strich/temporal-untrusted-workers/demos/internal/demokit"
	"github.com/02strich/temporal-untrusted-workers/demos/internal/toolcalls"
)

func main() {
	toolcalls.Main(demokit.Logs{Target: "dogwood_request", Engine: "Dogwood"})
}
