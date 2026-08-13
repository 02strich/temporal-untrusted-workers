// EchoActivity returns msg unchanged. Named to match the activity name Go's
// examples/verify-worker registers (the SDK registers activities under their
// exported function name), so proxy/Temporal UI history reads the same way
// regardless of which worker processed the task.
export async function EchoActivity(msg: string): Promise<string> {
  return msg;
}
