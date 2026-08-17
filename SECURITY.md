# Security Policy

## Supported versions

Security fixes are applied to the current default branch and released in the
next owner-approved patch, minor, or major version appropriate to compatibility
risk. Older releases are supported only when the repository owner explicitly
announces a maintained line.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use
[GitHub private vulnerability reporting](https://github.com/clemensv/avrotize/security/advisories/new)
and include:

- affected Avrotize, Structurize, MCP, or VS Code version and environment;
- affected command, schema/model behavior, or generated runtime;
- minimal reproducer and impact;
- whether the report involves untrusted schema/data, generated code, secrets,
  package publication, or dependency compromise;
- suggested mitigation, if known.

The repository owner coordinates acknowledgement, triage, embargo, fix,
disclosure, and release. A private report is security intake, not automatic
authorization to implement or disclose a fix.

Security changes require exact-head evidence, security/risk review, and
compatibility classification. Emergency authority is limited to the minimum
safe restoration and requires recorded risk, rollback, and ranked permanent
follow-up as defined in [GOVERNANCE.md](GOVERNANCE.md).
