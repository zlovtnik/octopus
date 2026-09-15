#!/usr/bin/env python3
"""Fail when Octopus JaCoCo line coverage drops below its committed policy."""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as etree
from pathlib import Path


def line_counter(element: etree.Element) -> tuple[int, int]:
    counter = next((child for child in element.findall("counter") if child.get("type") == "LINE"), None)
    if counter is None:
        return (0, 0)
    return (int(counter.get("missed", "0")), int(counter.get("covered", "0")))


def percentage(missed: int, covered: int) -> float:
    total = missed + covered
    return 0.0 if total == 0 else covered * 100.0 / total


def report_coverage(report: etree.Element, package_prefix: str) -> tuple[int, int]:
    prefix = package_prefix.replace(".", "/")
    counters = [
        line_counter(package)
        for package in report.findall("package")
        if package.get("name", "").startswith(prefix)
    ]
    return (
        sum(missed for missed, _ in counters),
        sum(covered for _, covered in counters),
    ) if counters else (0, 0)


def load_policy(path: Path) -> tuple[float, dict[str, float]]:
    with path.open(encoding="utf-8") as policy_file:
        policy = json.load(policy_file)
    overall = float(policy["overall_minimum_line_percentage"])
    packages = {name: float(floor) for name, floor in policy["packages"].items()}
    return overall, packages


def check(report_path: Path, policy_path: Path) -> int:
    if not report_path.is_file():
        print(f"coverage report does not exist: {report_path}", file=sys.stderr)
        return 2

    try:
        report = etree.parse(report_path).getroot()
        overall_floor, package_floors = load_policy(policy_path)
    except (etree.ParseError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"invalid coverage input: {error}", file=sys.stderr)
        return 2

    failures = []
    overall_missed, overall_covered = line_counter(report)
    checks = [("overall", overall_missed, overall_covered, overall_floor)]
    checks.extend(
        (prefix, *report_coverage(report, prefix), floor)
        for prefix, floor in sorted(package_floors.items())
    )

    for name, missed, covered, floor in checks:
        actual = percentage(missed, covered)
        status = "OK" if actual >= floor else "FAILED"
        print(f"{name}: {actual:.2f}% line coverage (minimum {floor:.2f}%) {status}")
        if status == "FAILED":
            failures.append(name)

    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="path to jacoco.xml")
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("coverage-policy.json"),
        help="path to the committed coverage policy",
    )
    arguments = parser.parse_args()
    return check(arguments.report, arguments.policy)


if __name__ == "__main__":
    raise SystemExit(main())
