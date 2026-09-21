# Cursor integration research

Cursor's current dashboard usage endpoint is the Connect-RPC method
`POST https://api2.cursor.sh/aiserver.v1.DashboardService/GetCurrentPeriodUsage`.
It accepts `{}` and returns the current billing cycle in `billingCycleStart` and
`billingCycleEnd` (Unix milliseconds, commonly encoded as strings). The two
provider usage bars are `planUsage.autoPercentUsed` and
`planUsage.apiPercentUsed`.

The adapter uses either a bearer access token or an already-issued
`WorkosCursorSessionToken` cookie. It does not attempt login, token exchange, or
browser automation. Configure one of `UW_CURSOR_ACCESS_TOKEN` or
`UW_CURSOR_SESSION_COOKIE`; setting both disables the daemon's Cursor adapter.

Cursor's bars are monthly billing-pool percentages, not duration-keyed rolling
windows. They are stored as `WindowKind::Custom("auto")` and
`WindowKind::Custom("api")`, both using `billingCycleEnd` as their reset. Cursor
does not expose a supported compaction, resume, or trustworthy context-token
channel through this integration, so those adapter capabilities remain false.

The usage pace estimate uses the last 24 hours of samples. This is deliberately
different from the default short lookback used by rolling-window providers.

Sources verified 2026-09-20:

- Cursor dashboard usage documentation: `GetCurrentPeriodUsage` and the
  `planUsage` fields are documented in the public Cursor integration research
  used for this adapter.
- Cursor's usage-limits documentation confirms the two current monthly pools and
  billing-cycle reset behavior.
