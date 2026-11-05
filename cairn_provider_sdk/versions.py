"""Release and parser version rules (cairn-core ADR 0005)."""

from __future__ import annotations


def _major_minor(v: str) -> tuple[int, int] | None:
    parts = v.removeprefix("v").split("-", 1)[0].split(".")
    if not v.startswith("v") or len(parts) < 2 or not all(p.isdigit() for p in parts[:2]):
        return None
    return int(parts[0]), int(parts[1])


def release_compatible(core: str, worker: str) -> bool:
    """Before 1.0 both share major.minor; from 1.0 on, same major and the
    worker's minor not above core's. Untagged builds are exempt.
    """
    c, wv = _major_minor(core), _major_minor(worker)
    if c is None or wv is None:
        return True
    if c[0] != wv[0]:
        return False
    if c[0] == 0:
        return c[1] == wv[1]
    return wv[1] <= c[1]


def parse_parser_version(v: str) -> tuple[int, int] | None:
    """ "N.M" as (N, M); a legacy bare "N" reads as N.0."""
    major, _, minor = v.strip().partition(".")
    if not major.isdigit() or (minor and not minor.isdigit()):
        return None
    return int(major), int(minor or 0)


def parser_allows(own: str, min_version: str) -> bool:
    """Parser own may re-parse a source last parsed by min_version: same
    dataset, own not older. A non-N.M min_version only matches itself.
    """
    if not min_version:
        return True
    m = parse_parser_version(min_version)
    if m is None:
        return own == min_version
    o = parse_parser_version(own)
    return o is not None and o[0] == m[0] and o[1] >= m[1]
