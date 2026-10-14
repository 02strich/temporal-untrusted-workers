// Example tool-call policy. The proxy already keeps child workflows and
// continue-as-new on the worker's own task queue; these policies decide which
// calls leaving it - Nexus operations and activities on other task queues -
// are allowed. Cedar denies anything no policy permits.

// The echo Nexus operation is harmless: allow it for every worker.
@id("allow-echo")
permit (principal, action == Action::"echo/echo/Echo", resource);

// Charges are allowed in USD and EUR...
@id("charge-in-usd-or-eur")
permit (principal, action == Action::"payments/billing/Charge", resource)
when {
  context.parameters has arg0 &&
  context.parameters.arg0.data has currency &&
  ["USD", "EUR"].contains(context.parameters.arg0.data.currency)
};

// ...charges above 1000 need a human's approval (a forbid annotated
// @decision("escalate") answers DECISION_ESCALATE instead of DECISION_DENIED;
// @reason tells the worker why)...
@id("charge-over-1000-needs-review")
@reason("Charges above 1000 need a human's approval")
@decision("escalate")
forbid (principal, action == Action::"payments/billing/Charge", resource)
unless {
  context.parameters has arg0 &&
  context.parameters.arg0.data has amount &&
  context.parameters.arg0.data.amount <= 1000
};

// ...and charges above 10000, or without an amount, are never allowed.
@id("charge-limit-10000")
@reason("Charges above 10000, or without an amount, are never allowed")
forbid (principal, action == Action::"payments/billing/Charge", resource)
unless {
  context.parameters has arg0 &&
  context.parameters.arg0.data has amount &&
  context.parameters.arg0.data.amount <= 10000
};

// The SendEmail activity, hosted by trusted workers on the trusted-tools task
// queue, may only email the customer the workflow was started for - not an
// address an injected prompt made up. Requires the run's full context.
@id("send-email-to-customer")
permit (principal, action == Action::"trusted-tools/SendEmail", resource)
when {
  context.inputEventsComplete &&
  context.workflowInput.parameters has arg0 &&
  context.workflowInput.parameters.arg0.data has customerEmail &&
  context.parameters has arg0 &&
  context.parameters.arg0.data has value &&
  context.parameters.arg0.data.value == context.workflowInput.parameters.arg0.data.customerEmail
};

// IssueRefund needs a human's approval, given to the workflow as an
// `approval` signal naming the refund (e.g. {"refundId": "R-1001",
// "approvedBy": "manager@example.com"}). The workflow can't approve itself:
// signals are sent from outside it.
@id("refund-needs-approval")
permit (principal, action == Action::"trusted-tools/IssueRefund", resource)
when {
  context.events.signal has approval &&
  context.events.signal.approval.parameters has arg0 &&
  context.events.signal.approval.parameters.arg0.data has refundId &&
  context.parameters has arg0 &&
  context.parameters.arg0.data has value &&
  context.parameters.arg0.data.value == context.events.signal.approval.parameters.arg0.data.refundId
};
