# Demos

Scripted, recordable walkthroughs of temporal-proxy. Each demo runs a complete local stack (a
Temporal dev server, the proxy, and whatever workers it needs), and can render itself to a video
with [VHS](https://github.com/charmbracelet/vhs).

| Demo | Shows |
|------|-------|
| [cedar-tool-calls](cedar-tool-calls) | Untrusted workflows calling trusted tools (Nexus operations, activities on other task queues), allowed or blocked by Cedar policies; the proxy's own same-queue rule; a live policy edit. |

## Running a demo

Each demo folder has the same entry points:

```bash
cd demos/<demo>
./up.sh                # build and start the stack in the background
source env.sh          # addresses, helpers and the `demo` command, in your shell
./down.sh              # stop everything
vhs demo.tape          # or render the scripted walkthrough to out/
```

Requirements: Go and the [Temporal CLI](https://docs.temporal.io/cli). Some demos need more, such
as Rust for the Cedar verifier. Rendering needs VHS (`brew install vhs`). Logs, PIDs and built
binaries go to `<demo>/run/` and recordings to `<demo>/out/`; both are git-ignored.

## Layout

```
demos/
  go.mod                  one Go module for every demo harness
  internal/demokit/       shared Go helpers: quiet clients, proxy client, allowed/blocked case runner
  lib/stack.sh            shared shell helpers: dev server, Nexus endpoints, proxy, background processes
  <demo>/
    main.go               the demo's harness (package main), built by up.sh as run/demo
    env.sh                the demo's settings and helpers; sources lib/stack.sh
    up.sh, down.sh        start and stop the stack
    demo.tape             the VHS recording script
    README.md             what the demo shows
```

## Adding a demo

1. Create `demos/<name>/` with an `env.sh` that sets `DEMO_DIR`, picks an unused `DEMO_PORT_BASE`
   (taken so far: 47000), and sources `../lib/stack.sh`.
2. Write `up.sh` from the `lib/stack.sh` building blocks: `stack_init`, `build_harness`,
   `build_proxy`, `start_temporal`, `create_endpoint`, `start_proxy`, `bg`, and `step` for progress
   output. `down.sh` is just `stack_down`.
3. Put the demo's workers and workflows in `main.go`. Use `demokit.RunCase` to show whether each
   step was allowed or blocked by the proxy, together with the proxy's reason.
4. Script the walkthrough in `demo.tape`, write the `README.md`, and add the demo to the table
   above.
