#!/usr/bin/env python3
"""Verdict of phase-meta-backend.sh, from the facts it prints to backend.txt.

Every requirement is checked explicitly; a missing fact fails like a wrong one, so a run that skipped a
step (no second Meta, no acknowledged sample, no verification) cannot pass. Exit 0 only when all hold.
Usage: meta_backend_judge.py backend.txt
"""
import re
import sys

EBUSY_TEXT = "Device or resource busy"


def judge(text):
    failures = []

    def need(condition, label):
        if not condition:
            failures.append(label)

    def line(prefix):
        found = [entry for entry in text.splitlines() if entry.startswith(prefix)]
        return found[-1] if found else None

    def number(pattern):
        match = re.search(pattern, text, re.MULTILINE)
        return int(match.group(1)) if match else None

    # 1. Meta kill -9 and recovery from the backend.
    need(line("[1] write after restart: ok") is not None, "[1] no write after the Meta restart")
    need(text.count("content identical: yes") == 2, "content not identical after both restarts")
    need("content identical: NO" not in text, "content changed across a restart")

    # 2. A second Meta on the same log fences the first one.
    opened = line("[2] second Meta opened the same log:")
    need(opened is not None and opened.startswith("[2] second Meta opened the same log: yes")
         and "same backend and prefix as the first: yes" in opened,
         "[2] the second Meta did not open the same backend and prefix")
    probe = line("[2] write through the first Meta:")
    rc = re.search(r"rc=(\d+)", probe or "")
    rc = int(rc.group(1)) if rc else None
    need(rc is not None and rc != 0, "[2] the probe write through the fenced Meta was not refused")
    need(rc != 124, "[2] the probe write timed out instead of being refused")
    need(probe is not None and EBUSY_TEXT in probe,
         "[2] the probe was not refused with EBUSY (META_LOG_FENCED)")
    fenced = number(r"^\[2\] fenced replies to the probe seen by the Node: (\d+)$")
    need(fenced is not None and fenced >= 1, "[2] the Node saw no fencing reply to the probe")
    need(line("[2] node exited after its Meta was fenced: yes") is not None,
         "[2] the Node served by the fenced Meta did not exit")
    need(line("[2] first Meta restarted, write: ok") is not None, "[2] no write after the first Meta restarted")
    need(line("[2] fenced probe absent after restart: yes") is not None,
         "[2] the refused probe was written anyway")

    # 3. Backend kill -9 under acknowledged (fsync'd) writes.
    before = number(r"^\[3\] backend killed; acknowledged before the kill: (\d+)$")
    need(before is not None and before >= 1, "[3] no acknowledged sample before the backend was killed")
    checked = re.search(r"^\[3\] acknowledged files: (\d+), checked: (\d+), missing or wrong: (\d+)$",
                        text, re.MULTILINE)
    if checked is None:
        failures.append("[3] acknowledged files were not verified")
    else:
        total, verified, bad = (int(value) for value in checked.groups())
        need(total >= 1, "[3] zero acknowledged samples")
        need(before is None or total >= before, "[3] fewer acknowledged files than before the kill")
        need(verified == total, "[3] not every acknowledged file was verified")
        need(bad == 0, "[3] an acknowledged file is missing or its SHA-256 differs")
    need(line("[3] write after backend restart: ok") is not None, "[3] no write after the backend restart")
    need(line("[3] new write after backend restart reads back: yes") is not None,
         "[3] the new write after the backend restart did not read back")

    need("FAILED" not in text, "a step reported FAILED")
    return failures


def main():
    if len(sys.argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    with open(sys.argv[1], errors="replace") as handle:
        text = handle.read()
    # The verdict of an earlier judge run in the same file is not a fact.
    text = "\n".join(entry for entry in text.splitlines() if not entry.startswith("[verdict]"))
    failures = judge(text)
    for failure in failures:
        print(f"[verdict] FAIL {failure}")
    print("[verdict] " + ("PASS" if not failures else f"FAIL ({len(failures)})"))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
