"""Exceptions a handler raises to steer the job's settlement."""

from __future__ import annotations


class TerminalJobError(Exception):
    """A job no retry can fix: publish a failure envelope and term it at once."""


class ParserTooOldError(Exception):
    """The source was parsed by a newer parser; another pool instance takes it."""

    code = "parser_too_old"


class RetryLaterError(Exception):
    """NAK the job with a delay (rate limit, upstream window); not a failure."""

    def __init__(self, delay_s: float, reason: str = "retry_later") -> None:
        super().__init__(reason)
        self.delay_s = delay_s
        self.reason = reason


class IncompatibleCoreError(RuntimeError):
    """Core runs another release major.minor than this worker."""


class CredentialsError(RuntimeError):
    """`cairn.creds.<provider>.get` answered an error or did not answer."""

    def __init__(self, account_id: str, reason: str) -> None:
        super().__init__(f"creds for {account_id}: {reason}")
        self.account_id = account_id
        self.reason = reason


class TokenError(RuntimeError):
    """`cairn.tokens.*` or the refresh refused; `terminal` reasons need re-authorization."""

    TERMINAL = frozenset({"account_gone", "needs_reauth", "invalid_grant"})

    def __init__(self, reason: str, message: str = "", retry_after_s: float = 0) -> None:
        super().__init__(message or reason)
        self.reason = reason
        self.retry_after_s = retry_after_s

    @property
    def terminal(self) -> bool:
        return self.reason in self.TERMINAL
