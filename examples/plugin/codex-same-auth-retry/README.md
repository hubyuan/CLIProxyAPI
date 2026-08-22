# Codex Same-Auth Retry

This dynamic plugin opts eligible transient Codex `429` failures into a retry on the currently selected auth. It is intentionally request-local: CPA does not rotate the current request to another account, and the host keeps the auth available for later requests.

The host owns classification, the five-retry limit, and the 30-second total budget. The plugin supplies only the policy decision and delay. `usage_limit_reached`, quota windows, authentication, permission, and entitlement failures are not eligible. Streaming retries are accepted only before the first non-empty payload.

Example configuration:

```yaml
plugins:
  configs:
    codex-same-auth-retry:
      enabled: true
      priority: 100
      delay_ms: 250
```
