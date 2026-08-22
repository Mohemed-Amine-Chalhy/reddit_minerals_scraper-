# Security policy

## Supported versions

Until the first tagged release, security fixes are applied only to the latest
commit on the default branch. After releases begin, this section must be updated
with an explicit support table.

## Report a vulnerability

Do not open a public issue for a vulnerability, exposed credential, personal
data, or provider-policy incident. Use the repository host's private security
reporting channel if one is configured; otherwise contact the repository owner
through a private channel already published on their profile. Include:

- affected version or commit;
- impact and prerequisites;
- minimal reproduction using synthetic data;
- suggested remediation, if known.

Do not include working credentials, private datasets, or unnecessary Reddit
content in the report.

## Credential incident response

Treat any committed, logged, displayed, or shared credential as compromised even
after the file is edited:

1. Revoke or rotate it at Reddit, Google, and any affected provider immediately.
2. Stop scheduled jobs that may still use it.
3. Search branches, tags, CI artifacts, releases, logs, notebooks, stashes, and
   local exports for the value or known prefixes.
4. Purge repository history with an appropriate history-rewrite tool.
5. Force-update protected references under a reviewed incident plan and require
   collaborators to re-clone; old clones still contain the secret.
6. Review provider audit and billing data and document scope and dates.
7. Add a regression secret-scanning rule without committing the secret itself.

Never assume deleting a line from the latest commit invalidates the credential.

## Security design expectations

- Secrets come only from environment variables or the deployment secret manager.
- Reddit uses read-only application authentication; user passwords are not
  accepted.
- Logs and exceptions must never serialize `SecretStr` values or content bodies.
- SQLite and exports require least-privilege filesystem access and must not be
  served from a public directory.
- Reddit and Gemini content is untrusted. Prompt construction must clearly
  delimit it and structured responses must be validated before persistence.
- Provider requests use bounded retries, timeouts, content limits, and explicit
  failure states.
- Dependencies and container images are scanned and updated through reviewed
  changes.

## Historical-secret warning

Earlier revisions of this project contained live-looking provider credentials in
source files. Repository owners must rotate those values and purge them from all
reachable history before treating the repository as safely distributable.
