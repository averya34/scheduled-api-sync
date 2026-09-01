# Security policy

## Reporting a vulnerability

Please report security issues privately through GitHub's private vulnerability
reporting: go to the [Security tab](https://github.com/averya34/scheduled-api-sync/security)
of this repository and choose **Report a vulnerability**. That opens a private
advisory visible only to you and the maintainer.

Do not open a public issue or pull request for a security problem, and do not
include real credentials in a report — a redacted example is always enough to
reproduce.

Please include:

- what an attacker gains, not only what the code does wrong
- the affected module and version
- a minimal reproduction, ideally as a failing test
- any mitigation you have already found

I aim to acknowledge a report within three working days and to have either a
fix or a concrete plan within fourteen days. Credit is given in the advisory
unless you would rather remain anonymous.

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.3.x   | Yes       |
| < 0.3   | No        |

## Threat model

This library is a set of building blocks for jobs that hold credentials for
third-party systems and run unattended on shared CI infrastructure. That
combination — long-lived secrets, no human watching, world-readable logs on
public repositories — defines what is in scope.

### In scope

| Threat | Why it matters here | Mitigation in this repository |
| --- | --- | --- |
| Credential leakage through logs | A sync job logs requests, responses and errors in a loop. GitHub Actions logs on a public repo are world-readable, and GitHub's secret masking only covers exact matches of values registered as repository secrets — not tokens minted at runtime, session cookies, or customer data. | `synckit.logging.RedactionFilter` scrubs values for keys matching `token`, `secret`, `password`, `api_key`, `authorization`, `bearer`, `credential`, `cookie` and friends at any nesting depth, plus inline `Bearer`/`Basic` values and email addresses in free text. It is a filter, not a formatter, so it applies regardless of which formatter is attached. `JsonFormatter` re-applies redaction to the assembled payload as a second pass. |
| Personal data in logs | CRM and directory records contain email addresses. Those are personal data under GDPR whether or not anyone thinks of them as secrets, and a log shipper will happily retain them for a year. | Email redaction is on by default and must be switched off explicitly. |
| Secrets committed to workflow files | The most common real-world leak is not a clever attack; it is a token pasted into a YAML file during debugging and never removed. | Workflows in this repo read every credential from `${{ secrets.* }}` and pass it as an environment variable. `.gitignore` excludes `.env`, `.env.*`, `*.pem`, `*.key`, `credentials.json` and `secrets.json`. |
| Secrets in process arguments | Anything passed as a command line argument is visible in `ps` output to every other process on the runner, and is echoed into the step log by any shell running with tracing. | `example-sync.yml` passes `API_TOKEN` via `env:`, never via argv, and the point is called out in a comment so a copy-paste user does not undo it. |
| Over-permissive `GITHUB_TOKEN` | The default token permissions are broad. A compromised third-party action inherits them and can push commits, publish releases, or open pull requests. | Every workflow sets `permissions: contents: read` at the top level. Nothing in this repository needs write access, and no job requests it. |
| Overlapping scheduled runs | Two concurrent runs of the same sync race on the checkpoint file and can double-write batches to a live CRM. | `concurrency` groups in `example-sync.yml`, with `cancel-in-progress: false` so an in-flight batch is never killed halfway. |
| Dependency confusion and supply-chain compromise | Every installed package is code that runs with the job's credentials. A typosquatted or hijacked transitive dependency is an established attack path. | **Zero runtime dependencies.** `synckit` imports only the standard library, so the runtime dependency graph has nothing to confuse. Dev dependencies are limited to `pytest` and `ruff`, are never installed in the example sync workflow, and third-party actions are pinned to major versions from verified publishers. |
| Corrupt state causing unbounded re-processing | A truncated checkpoint that silently reads as "start from zero" makes the next run re-write the entire dataset to production. | `JsonCheckpointStore` writes atomically (temp file, `fsync`, `os.replace`) so a crash cannot truncate the file, quarantines a corrupt file rather than overwriting the evidence, and offers `strict=True` for jobs where an accidental full re-sync is worse than a failed run. |
| Unreviewed writes to a production system | A widened filter or a wrong field mapping can overwrite thousands of live records with no undo. | `SyncRunner(dry_run=True)` runs the identical code path with the sink and store swapped out, and reports the exact counts and planned batches. |

### Out of scope

| Not covered | Reason |
| --- | --- |
| Transport security (TLS, certificate pinning) | This library ships no HTTP client. You bring your own transport and are responsible for verifying certificates. |
| Authentication and token minting | `synckit` never obtains, refreshes or stores credentials. It only avoids logging them. |
| Authorisation in the target system | Whether the sync's account should be allowed to write a given record is the target system's decision, enforced by the scopes you grant it. |
| Encryption of checkpoint files at rest | Checkpoints hold cursors and counters, not payloads. If your cursor is itself sensitive, encrypt the volume or use a store you control. |
| Malicious code with local execution | Anything already running as the job user can read the environment. Redaction defends against accidental disclosure, not against an attacker on the box. |
| Denial of service against the upstream API | The rate limiter is a good-citizen control, not a security boundary. A caller can construct a bucket with any rate it likes. |
| Secret scanning of your repository history | Use GitHub secret scanning and push protection. This library cannot see your commits. |

## Operational guidance

**Rotate credentials on a schedule, and mean it.** Pick a rotation interval you
can actually meet — 90 days is a reasonable default for a machine token — and
rotate immediately on any of: a maintainer leaving, a suspected log leak, a
public fork of a private repo, or a workflow run whose logs were downloaded by
someone unexpected. Rotation is only credible if you have tested it, so rotate
once deliberately before you need to do it under pressure.

**Use separate credentials per environment and per job.** A single token shared
between staging and production means a staging leak is a production incident,
and it makes the audit log useless for working out which job did what.

**Grant the narrowest API scopes the job needs.** A sync that reads contacts and
writes a reporting table does not need permission to delete deals. Most CRM and
accounting APIs support scoped tokens; the five minutes spent configuring them
is the cheapest control available.

**Keep `permissions: contents: read` as the default in every workflow** and add
a specific permission to a specific job only when a step genuinely fails without
it. Grant it at the job level, not the workflow level.

**Prefer environments with required reviewers for anything that writes to
production.** It turns an accidental `workflow_dispatch` into an approval
request, and it scopes production secrets so they are unavailable to any other
workflow in the repository.

**Run new or changed syncs with `dry_run=True` first**, in CI, and read the
counts. If a job that should touch two hundred records reports two hundred
thousand, the filter is wrong and you have found out for free.

**Treat CI logs as public even on a private repository.** Fork pull requests,
artifact downloads and screenshots in chat all move logs outside the repository's
access controls. The redaction filter exists because that assumption is safer
than the alternative.
