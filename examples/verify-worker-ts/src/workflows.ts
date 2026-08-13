import { proxyActivities } from '@temporalio/workflow';
// Only import the activity types - workflow code runs in a separate
// sandboxed context and must not import the activity implementations.
import type * as activities from './activities';

const { EchoActivity } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
});

// EchoActivity scheduled on "forbidden-queue" - a task queue no configured
// identity is authorized for. The proxy denies the RespondWorkflowTaskCompleted
// call carrying this command (see internal/scope.ValidateCommands), so this
// workflow never actually completes; it exists purely to make that denial
// observable when running the example.
const { EchoActivity: EchoActivityOnForbiddenQueue } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  taskQueue: 'forbidden-queue',
});

// EchoWorkflow runs EchoActivity and returns its result unchanged. It exists
// purely to exercise the proxy's Poll/Respond path for both workflow and
// activity task queues.
export async function EchoWorkflow(msg: string): Promise<string> {
  return await EchoActivity(msg);
}

export async function CrossQueueWorkflow(msg: string): Promise<string> {
  return await EchoActivityOnForbiddenQueue(msg);
}
