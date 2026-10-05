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

// ...but never above 1000, and never without an amount.
@id("charge-limit-1000")
forbid (principal, action == Action::"payments/billing/Charge", resource)
unless {
  context.parameters has arg0 &&
  context.parameters.arg0.data has amount &&
  context.parameters.arg0.data.amount <= 1000
};

// The SendEmail activity, hosted by trusted workers on the trusted-tools task
// queue, may only email example.com addresses.
@id("send-email-to-example-com")
permit (principal, action == Action::"trusted-tools/SendEmail", resource)
when {
  context.parameters has arg0 &&
  context.parameters.arg0.data has value &&
  context.parameters.arg0.data.value like "*@example.com"
};
