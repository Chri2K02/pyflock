"""Retry policy: backoff math and the retry decision."""

from __future__ import annotations

from pyflock.core.retry import RetryPolicy


def test_should_retry_respects_max_attempts():
    policy = RetryPolicy(max_attempts=3)
    assert policy.should_retry(0) is True
    assert policy.should_retry(1) is True
    assert policy.should_retry(2) is True
    assert policy.should_retry(3) is False
    assert policy.should_retry(4) is False


def test_backoff_is_exponential():
    policy = RetryPolicy(base=2.0, cap=60.0)
    assert policy.backoff_seconds(1) == 1.0  # 2**0
    assert policy.backoff_seconds(2) == 2.0  # 2**1
    assert policy.backoff_seconds(3) == 4.0  # 2**2
    assert policy.backoff_seconds(4) == 8.0  # 2**3


def test_backoff_is_capped():
    policy = RetryPolicy(base=2.0, cap=10.0)
    assert policy.backoff_seconds(10) == 10.0


def test_backoff_zero_for_no_attempts():
    assert RetryPolicy().backoff_seconds(0) == 0.0
