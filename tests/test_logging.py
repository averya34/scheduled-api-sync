"""Tests for synckit.logging."""

from __future__ import annotations

import io
import json
import logging
import re

import pytest

from synckit.logging import (
    REDACTED,
    JsonFormatter,
    RedactionFilter,
    configure_logging,
    redact,
)


@pytest.fixture
def stream():
    return io.StringIO()


@pytest.fixture
def logger(stream, request):
    log = configure_logging(logging.DEBUG, logger_name=f"test.{request.node.name}", stream=stream)
    yield log
    for handler in list(log.handlers):
        log.removeHandler(handler)
        handler.close()


def emitted(stream) -> list[dict]:
    return [json.loads(line) for line in stream.getvalue().strip().splitlines() if line]


# -- redact(): keys -------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "token",
        "TOKEN",
        "api_key",
        "apiKey",
        "API-KEY",
        "secret",
        "client_secret",
        "password",
        "authorization",
        "Authorization",
        "bearer",
        "refresh_token",
        "private_key",
        "session_id",
        "Cookie",
    ],
)
def test_sensitive_keys_are_redacted(key):
    assert redact({key: "hunter2"})[key] == REDACTED


@pytest.mark.parametrize("key", ["id", "name", "count", "status", "cursor", "job"])
def test_ordinary_keys_are_left_alone(key):
    assert redact({key: "value"})[key] == "value"


def test_a_sensitive_key_hides_its_whole_subtree():
    data = {"credentials": {"user": "avery", "password": "p"}}
    assert redact(data)["credentials"] == REDACTED


def test_redaction_reaches_arbitrary_nesting_depth():
    data = {"a": {"b": {"c": {"d": {"e": {"api_key": "sk-live-123"}}}}}}
    assert redact(data)["a"]["b"]["c"]["d"]["e"]["api_key"] == REDACTED


def test_redaction_inside_a_list_of_dicts():
    data = {"requests": [{"url": "/v1/x", "token": "t1"}, {"url": "/v1/y", "token": "t2"}]}
    cleaned = redact(data)
    assert [item["token"] for item in cleaned["requests"]] == [REDACTED, REDACTED]
    assert cleaned["requests"][0]["url"] == "/v1/x"


def test_redaction_inside_nested_lists():
    data = {"batches": [[{"secret": "s"}], [[{"secret": "t"}]]]}
    cleaned = redact(data)
    assert cleaned["batches"][0][0]["secret"] == REDACTED
    assert cleaned["batches"][1][0][0]["secret"] == REDACTED


def test_tuples_stay_tuples():
    cleaned = redact(("a", {"token": "x"}))
    assert isinstance(cleaned, tuple)
    assert cleaned[1]["token"] == REDACTED


def test_sets_stay_sets():
    cleaned = redact({"alice@example.com", "plain"})
    assert isinstance(cleaned, set)
    assert REDACTED in cleaned


def test_input_is_never_mutated():
    original = {"token": "abc", "nested": {"password": "p"}}
    snapshot = json.dumps(original, sort_keys=True)
    redact(original)
    assert json.dumps(original, sort_keys=True) == snapshot


def test_non_string_values_pass_through():
    data = {"count": 5, "ratio": 1.5, "ok": True, "nothing": None}
    assert redact(data) == data


def test_non_string_keys_are_handled():
    assert redact({1: "value", None: "other"}) == {1: "value", None: "other"}


def test_bytes_are_summarised_not_logged():
    assert redact({"body": b"0123456789"})["body"] == "<10 bytes>"


def test_depth_limit_prevents_runaway_recursion():
    data: dict = {}
    node = data
    for _ in range(60):
        node["next"] = {}
        node = node["next"]
    node["token"] = "deep"
    cleaned = redact(data)
    text = json.dumps(cleaned)
    assert "deep" not in text


def test_custom_key_pattern():
    pattern = re.compile(r"internal", re.IGNORECASE)
    cleaned = redact({"internal_id": "x", "token": "t"}, key_pattern=pattern)
    assert cleaned["internal_id"] == REDACTED
    # The custom pattern replaces the default, so "token" is now allowed.
    assert cleaned["token"] == "t"


# -- redact(): free text --------------------------------------------------


def test_email_addresses_are_redacted():
    assert redact("contact avery.mcqueen@example.com now") == f"contact {REDACTED} now"


def test_multiple_emails_in_one_string():
    cleaned = redact("a@x.com, b@y.co.uk")
    assert cleaned.count(REDACTED) == 2


def test_emails_inside_a_list_are_redacted():
    cleaned = redact(["a@x.com", "no address here"])
    assert cleaned == [REDACTED, "no address here"]


def test_email_redaction_can_be_disabled():
    text = "avery@example.com"
    assert redact(text, redact_emails=False) == text


def test_bearer_tokens_in_free_text_are_redacted():
    cleaned = redact("Authorization: Bearer eyJhbGciOi.JIUzI1NiJ9.abc-_123")
    assert "eyJhbGciOi" not in cleaned
    assert REDACTED in cleaned


def test_basic_auth_in_free_text_is_redacted():
    cleaned = redact("header Basic YXZlcnk6c2VjcmV0Cg==")
    assert "YXZlcnk6" not in cleaned


def test_inline_api_key_assignment_is_redacted():
    cleaned = redact("calling https://api.example.com/v1?api_key=sk_live_9f8a7b")
    assert "sk_live_9f8a7b" not in cleaned


def test_ordinary_text_is_untouched():
    text = "wrote 250 records to the ledger in 3.2s"
    assert redact(text) == text


# -- RedactionFilter ------------------------------------------------------


def test_filter_scrubs_the_message():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "user avery@example.com", None, None)
    RedactionFilter().filter(record)
    assert "avery@example.com" not in record.getMessage()


def test_filter_scrubs_positional_args():
    record = logging.LogRecord(
        "t", logging.INFO, __file__, 1, "token is %s", ("Bearer abc123def",), None
    )
    RedactionFilter().filter(record)
    assert "abc123def" not in record.getMessage()


def test_filter_scrubs_extra_fields():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "call", None, None)
    record.headers = {"Authorization": "Bearer secret-value"}
    RedactionFilter().filter(record)
    assert record.headers["Authorization"] == REDACTED


def test_filter_leaves_standard_attributes_alone():
    record = logging.LogRecord("t", logging.INFO, "/path/x.py", 42, "hello", None, None)
    RedactionFilter().filter(record)
    assert record.pathname == "/path/x.py"
    assert record.lineno == 42


def test_filter_always_returns_true():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "anything", None, None)
    assert RedactionFilter().filter(record) is True


def test_filter_accepts_a_string_pattern():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "x", None, None)
    record.custom_field = {"widget": "v"}
    RedactionFilter(key_pattern="widget").filter(record)
    assert record.custom_field["widget"] == REDACTED


# -- JsonFormatter --------------------------------------------------------


def test_output_is_one_line_of_json(logger, stream):
    logger.info("sync finished")
    lines = stream.getvalue().strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["message"] == "sync finished"


def test_core_fields_are_present(logger, stream):
    logger.warning("careful")
    entry = emitted(stream)[0]
    assert entry["level"] == "WARNING"
    assert entry["message"] == "careful"
    assert entry["timestamp"].endswith("+00:00")
    assert "logger" in entry


def test_extra_fields_land_under_extra(logger, stream):
    logger.info("batch written", extra={"job": "crm", "written": 100})
    entry = emitted(stream)[0]
    assert entry["extra"]["job"] == "crm"
    assert entry["extra"]["written"] == 100


def test_secrets_in_extra_never_reach_the_stream(logger, stream):
    logger.info("calling api", extra={"headers": {"Authorization": "Bearer sk-live-xyz"}})
    assert "sk-live-xyz" not in stream.getvalue()
    assert REDACTED in stream.getvalue()


def test_emails_in_a_payload_never_reach_the_stream(logger, stream):
    logger.info("record failed", extra={"record": {"email": "buyer@acme.com", "id": 7}})
    text = stream.getvalue()
    assert "buyer@acme.com" not in text
    assert '"id": 7' in text or "'id': 7" in text or json.loads(text)["extra"]["record"]["id"] == 7


def test_static_fields_are_merged_into_every_record(stream):
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter(static_fields={"service": "crm-sync", "env": "prod"}))
    log = logging.getLogger("test.static")
    log.handlers = [handler]
    log.setLevel(logging.INFO)
    log.propagate = False
    log.info("hello")
    entry = emitted(stream)[0]
    assert entry["service"] == "crm-sync"
    assert entry["env"] == "prod"


def test_source_location_is_optional(stream):
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter(include_source=True))
    log = logging.getLogger("test.source")
    log.handlers = [handler]
    log.setLevel(logging.INFO)
    log.propagate = False
    log.info("hello")
    entry = emitted(stream)[0]
    assert entry["source"]["function"] == "test_source_location_is_optional"
    assert entry["source"]["line"] > 0


def test_exceptions_are_serialised_into_one_event(logger, stream):
    try:
        raise ValueError("bad row")
    except ValueError:
        logger.exception("record failed")
    entry = emitted(stream)[0]
    assert "ValueError: bad row" in entry["exception"]
    assert "Traceback" in entry["exception"]


def test_unserialisable_extra_values_degrade_to_strings(logger, stream):
    class Widget:
        def __repr__(self):
            return "<Widget>"

    logger.info("odd payload", extra={"widget": Widget()})
    assert emitted(stream)[0]["extra"]["widget"] == "<Widget>"


def test_percent_style_message_is_formatted(logger, stream):
    logger.info("wrote %d records in %s", 42, "crm")
    assert emitted(stream)[0]["message"] == "wrote 42 records in crm"


def test_non_ascii_is_preserved_by_default(logger, stream):
    logger.info("naïve résumé")
    assert emitted(stream)[0]["message"] == "naïve résumé"


# -- configure_logging ----------------------------------------------------


def test_configure_logging_returns_a_configured_logger(stream):
    log = configure_logging(logging.INFO, logger_name="test.configure", stream=stream)
    assert log.level == logging.INFO
    assert len(log.handlers) == 1
    assert log.propagate is False


def test_repeated_configuration_does_not_duplicate_handlers(stream):
    for _ in range(3):
        log = configure_logging(logging.INFO, logger_name="test.repeat", stream=stream)
    log.info("once")
    assert len(emitted(stream)) == 1


def test_configure_logging_accepts_explicit_handlers(stream):
    handler = logging.StreamHandler(stream)
    log = configure_logging(logging.INFO, logger_name="test.handlers", handlers=[handler])
    log.info("hi")
    assert emitted(stream)[0]["message"] == "hi"


def test_configure_logging_honours_a_custom_key_pattern(stream):
    log = configure_logging(
        logging.INFO,
        logger_name="test.pattern",
        stream=stream,
        key_pattern="widget",
    )
    log.info("x", extra={"widget": "hidden-value"})
    assert "hidden-value" not in stream.getvalue()


def test_level_filtering_still_works(stream):
    log = configure_logging(logging.WARNING, logger_name="test.level", stream=stream)
    log.debug("invisible")
    log.warning("visible")
    entries = emitted(stream)
    assert len(entries) == 1
    assert entries[0]["message"] == "visible"
