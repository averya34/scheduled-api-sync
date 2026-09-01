"""Structured JSON logging with secret redaction.

Why an integration job needs this more than most services
---------------------------------------------------------
A sync job is, by definition, a process that holds credentials for someone
else's system and talks to it in a loop. Its logs are the single richest
place for those credentials to escape:

* The obvious one -- somebody adds ``log.debug("request: %s", request)`` while
  chasing a bug, and the request object stringifies its ``Authorization``
  header.
* Error paths. An exception from an HTTP client frequently embeds the full
  URL, and plenty of APIs still accept ``?api_key=`` in the query string.
* Payload echoes. "Failed to write record: {...}" is a natural thing to log,
  and the record from a CRM contains email addresses, which are personal
  data under GDPR whether or not anybody thinks of them as secrets.
* Retry logging. The most tempting object to dump when a call fails four
  times is the thing you sent -- headers included.

And CI logs are *shared*. On a public repository, GitHub Actions logs are
world-readable. GitHub masks values registered as repository secrets, but
that masking only covers exact string matches of the configured secrets: a
token minted at runtime by an OAuth exchange, a session cookie, or a
customer's email address is not masked by anything.

So redaction happens at the logging layer, not at the call sites. Call sites
are written by humans under time pressure; a filter is applied to every
record unconditionally. Defence in depth: keep secrets out of log calls
*and* have the pipeline scrub what slips through.

Why JSON
--------
Structured output means a failed nightly run can be answered with a query
(``jq 'select(.level=="ERROR")'``) instead of a regex over prose. It also
keeps multi-line content -- tracebacks, payload fragments -- inside a single
log event, so a log shipper does not split one error into fourteen
unrelated lines.

Redaction is recursive and type-preserving: dicts, lists, tuples and sets
are walked to arbitrary depth, because the interesting secret is never at
the top level. It is at ``record.extra.request.headers.Authorization``.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging as _stdlib_logging
import re
from collections.abc import Iterable, Mapping
from typing import Any

__all__ = [
    "DEFAULT_SECRET_KEY_PATTERN",
    "REDACTED",
    "JsonFormatter",
    "RedactionFilter",
    "configure_logging",
    "redact",
]

#: Placeholder substituted for anything sensitive. A fixed sentinel (rather
#: than deleting the key) keeps the log shape stable, so a dashboard that
#: expects a field does not break, and a reader can tell the difference
#: between "absent" and "withheld".
REDACTED = "***REDACTED***"

#: Keys whose *values* are replaced wholesale. Substring matching, case
#: insensitive, so ``X-Api-Key``, ``refresh_token`` and ``client_secret`` are
#: all covered without enumerating every vendor's naming convention.
DEFAULT_SECRET_KEY_PATTERN = re.compile(
    r"(token|secret|password|passwd|api[_-]?key|apikey|authorization|auth|bearer|credential"
    r"|private[_-]?key|session[_-]?id|cookie|signature)",
    re.IGNORECASE,
)

#: Deliberately pragmatic rather than RFC 5322 complete. The full grammar
#: allows quoted local parts with spaces, matching which produces false
#: positives on ordinary prose. This covers the shape of addresses that
#: actually appear in CRM records.
_EMAIL_PATTERN = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
)

#: Bearer tokens and inline credentials embedded in free text -- the case a
#: key-based rule cannot catch, because there is no key, just a sentence.
_INLINE_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9\-._~+/]+=*"),
    re.compile(r"(?i)\b(basic)\s+[A-Za-z0-9+/]{8,}=*"),
    re.compile(r"(?i)\b(x-api-key|api[_-]?key|access[_-]?token|token)\s*[:=]\s*[^\s,;&\"']+"),
)

#: Attributes the stdlib puts on every LogRecord. Anything *not* in here was
#: supplied by the caller via ``extra=`` and therefore belongs in the JSON
#: payload. Hard-coding the list is more robust than diffing against a
#: freshly constructed record, which allocates on every single log call.
_STANDARD_RECORD_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


def _redact_text(text: str, *, redact_emails: bool) -> str:
    """Scrub inline secrets and, optionally, email addresses from a string."""
    for pattern in _INLINE_SECRET_PATTERNS:
        text = pattern.sub(lambda m: f"{m.group(1)} {REDACTED}", text)
    if redact_emails:
        text = _EMAIL_PATTERN.sub(REDACTED, text)
    return text


def redact(
    value: Any,
    *,
    key_pattern: re.Pattern[str] | None = None,
    redact_emails: bool = True,
    _depth: int = 0,
    _max_depth: int = 24,
) -> Any:
    """Return a copy of ``value`` with sensitive content replaced.

    The input is never mutated: a logging filter that edited the caller's
    dict in place would corrupt application state, and that bug is
    excruciating to find because it only manifests when logging is enabled.

    ``_max_depth`` guards against pathological or self-referential
    structures. Recursion errors inside a logging filter are especially
    nasty -- the handler for the failure tries to log, which recurses again.
    """
    pattern = key_pattern if key_pattern is not None else DEFAULT_SECRET_KEY_PATTERN

    if _depth > _max_depth:
        return REDACTED

    if isinstance(value, Mapping):
        cleaned: dict[Any, Any] = {}
        for key, item in value.items():
            if isinstance(key, str) and pattern.search(key):
                # The whole value goes, whatever its type. A nested dict
                # under a key called "credentials" is not made safe by
                # scrubbing its leaves individually.
                cleaned[key] = REDACTED
            else:
                cleaned[key] = redact(
                    item,
                    key_pattern=pattern,
                    redact_emails=redact_emails,
                    _depth=_depth + 1,
                    _max_depth=_max_depth,
                )
        return cleaned

    if isinstance(value, str):
        return _redact_text(value, redact_emails=redact_emails)

    if isinstance(value, (list, tuple, set, frozenset)):
        items = [
            redact(
                item,
                key_pattern=pattern,
                redact_emails=redact_emails,
                _depth=_depth + 1,
                _max_depth=_max_depth,
            )
            for item in value
        ]
        # Preserve container type so downstream consumers and assertions
        # see the same shape they passed in.
        if isinstance(value, tuple):
            return tuple(items)
        if isinstance(value, (set, frozenset)):
            try:
                return type(value)(items)
            except TypeError:
                # Redaction can make elements unhashable (a dict inside a
                # set is impossible, but a subclass may allow odd things).
                return items
        return items

    if isinstance(value, bytes):
        # Bytes are almost always a raw response body; decoding to inspect
        # them costs more than it is worth, and logging them verbatim is how
        # a whole payload lands in CI output.
        return f"<{len(value)} bytes>"

    return value


class RedactionFilter(_stdlib_logging.Filter):
    """Logging filter that scrubs the message and every ``extra`` field.

    Implemented as a Filter rather than a Formatter so that redaction
    applies no matter which formatter is attached -- including the plain
    text formatter somebody adds locally while debugging, which is exactly
    when a secret is most likely to be printed.
    """

    def __init__(
        self,
        *,
        key_pattern: re.Pattern[str] | str | None = None,
        redact_emails: bool = True,
        name: str = "",
    ) -> None:
        super().__init__(name)
        if isinstance(key_pattern, str):
            key_pattern = re.compile(key_pattern, re.IGNORECASE)
        self.key_pattern = key_pattern or DEFAULT_SECRET_KEY_PATTERN
        self.redact_emails = redact_emails

    def filter(self, record: _stdlib_logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _redact_text(record.msg, redact_emails=self.redact_emails)
        elif record.msg is not None:
            record.msg = redact(
                record.msg,
                key_pattern=self.key_pattern,
                redact_emails=self.redact_emails,
            )

        if record.args:
            # ``args`` may be a tuple of positional values or a single dict
            # for %(name)s style formatting; both need scrubbing before the
            # formatter interpolates them into the message.
            record.args = redact(
                record.args,
                key_pattern=self.key_pattern,
                redact_emails=self.redact_emails,
            )

        for attr, value in list(record.__dict__.items()):
            if attr in _STANDARD_RECORD_ATTRS or attr.startswith("_"):
                continue
            record.__dict__[attr] = redact(
                value,
                key_pattern=self.key_pattern,
                redact_emails=self.redact_emails,
            )

        # Always emit: this filter sanitises, it does not suppress.
        return True


class JsonFormatter(_stdlib_logging.Formatter):
    """Format a record as a single line of JSON.

    Parameters
    ----------
    static_fields:
        Merged into every record. The usual contents are ``service``,
        ``environment`` and the CI run id, which is what makes it possible
        to find every log line from one failed nightly run.
    include_source:
        Adds module, function and line number. Cheap and worth it for a job
        whose failures you will read days later, off by default for anyone
        shipping high volume to a metered log backend.
    redact_output:
        Applies :func:`redact` to the assembled payload as a final pass.
        Belt and braces: it catches fields injected by a handler or filter
        that ran after :class:`RedactionFilter`.
    """

    def __init__(
        self,
        *,
        static_fields: Mapping[str, Any] | None = None,
        include_source: bool = False,
        redact_output: bool = True,
        key_pattern: re.Pattern[str] | None = None,
        redact_emails: bool = True,
        ensure_ascii: bool = False,
    ) -> None:
        super().__init__()
        self.static_fields = dict(static_fields or {})
        self.include_source = include_source
        self.redact_output = redact_output
        self.key_pattern = key_pattern or DEFAULT_SECRET_KEY_PATTERN
        self.redact_emails = redact_emails
        self.ensure_ascii = ensure_ascii

    def format(self, record: _stdlib_logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            # ISO-8601 in UTC. Local time in a log written by a runner in an
            # unknown region is actively misleading.
            "timestamp": _dt.datetime.fromtimestamp(
                record.created, tz=_dt.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(self.static_fields)

        if self.include_source:
            payload["source"] = {
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            }

        if record.exc_info:
            # formatException gives the full traceback as one string, which
            # keeps the event atomic for log shippers.
            payload["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exception"] = record.exc_text
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_ATTRS and not key.startswith("_")
        }
        if extras:
            payload["extra"] = extras

        if self.redact_output:
            payload = redact(
                payload,
                key_pattern=self.key_pattern,
                redact_emails=self.redact_emails,
            )

        # ``default=str`` so a datetime, Decimal or dataclass in an extra
        # field degrades to its string form instead of raising inside the
        # logging machinery, where the exception would be swallowed and the
        # log line lost.
        return json.dumps(payload, default=str, ensure_ascii=self.ensure_ascii, sort_keys=False)


def configure_logging(
    level: int | str = _stdlib_logging.INFO,
    *,
    logger_name: str | None = None,
    static_fields: Mapping[str, Any] | None = None,
    include_source: bool = False,
    redact_emails: bool = True,
    key_pattern: re.Pattern[str] | str | None = None,
    stream: Any = None,
    handlers: Iterable[_stdlib_logging.Handler] | None = None,
) -> _stdlib_logging.Logger:
    """Install a JSON handler with redaction and return the logger.

    Existing handlers on the target logger are removed first. Calling this
    twice -- which happens whenever a job module is imported by both the CLI
    entry point and a test -- would otherwise duplicate every line.
    """
    logger = _stdlib_logging.getLogger(logger_name)
    logger.setLevel(level)

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = JsonFormatter(
        static_fields=static_fields,
        include_source=include_source,
        redact_emails=redact_emails,
        key_pattern=re.compile(key_pattern, re.IGNORECASE)
        if isinstance(key_pattern, str)
        else key_pattern,
    )
    redaction = RedactionFilter(key_pattern=key_pattern, redact_emails=redact_emails)

    chosen = list(handlers) if handlers is not None else [_stdlib_logging.StreamHandler(stream)]
    for handler in chosen:
        handler.setFormatter(formatter)
        handler.addFilter(redaction)
        logger.addHandler(handler)

    # Stop records also reaching the root logger's handlers, which in CI is
    # usually an unredacted default StreamHandler installed by something else.
    logger.propagate = False
    return logger
