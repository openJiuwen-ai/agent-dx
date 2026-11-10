#!/usr/bin/env python3
"""Create fresh task-only mTLS material. Never overwrite existing identities."""

from pathlib import Path
import subprocess
import argparse
import logging
import secrets
import shutil
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("output", type=Path)
p.add_argument("--four-workers", action="store_true")
options = p.parse_args()
r = options.output
if r.exists() and any(r.iterdir()):
    p.error("TLS directory exists; preserve identities until the owning run is closed and cleaned")
r.mkdir(mode=0o700, parents=True, exist_ok=True)
token = r / "nydus-token"
token.touch(mode=0o600, exist_ok=False)
token.write_text(secrets.token_hex(32) + "\n")


OPENSSL = shutil.which("openssl")
if OPENSSL is None:
    p.error("openssl is required")


def cmd(*args):
    subprocess.run([OPENSSL, *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


cmd(
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-days",
    "3",
    "-subj",
    "/CN=afs-connectors-ca",
    "-keyout",
    r / "ca.key",
    "-out",
    r / "ca.pem",
)
(r / "extensions").write_text(
    "subjectAltName=DNS:afs-meta,IP:10.203.0.10,IP:10.203.0.20,IP:10.203.0.21,IP:10.203.0.22,IP:10.203.0.23\n"
    "extendedKeyUsage=serverAuth,clientAuth\n"
)
for n in ["meta", "node-a", "node-b", "node-c"] + (["node-d"] if options.four_workers else []):
    cmd("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={n}", "-keyout", r / f"{n}.key", "-out", r / f"{n}.csr")
    cmd(
        "x509",
        "-req",
        "-in",
        r / f"{n}.csr",
        "-CA",
        r / "ca.pem",
        "-CAkey",
        r / "ca.key",
        "-CAcreateserial",
        "-days",
        "3",
        "-extfile",
        r / "extensions",
        "-out",
        r / f"{n}.pem",
    )
for key in r.glob("*.key"):
    key.chmod(0o600)
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logging.info("Created task-only identities; key material not logged")
