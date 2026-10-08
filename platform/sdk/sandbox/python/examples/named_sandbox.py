"""Named Sandbox Example

Demonstrates creating a sandbox with a custom name and using its real ID with
the lightweight ``adx-sandbox`` CLI.

Prerequisites:
  - ADX_SERVER_ADDRESS and ADX_TOKEN environment variables must be set.
  - Install the SDK: pip install -e sdk/python/

Usage:
  export ADX_SERVER_ADDRESS=your-server.example.com
  export ADX_TOKEN=your-token
  python named_sandbox.py
"""

import subprocess
import sys

from adx_sandbox import Sandbox


def main():
    name = "my-test-sandbox"

    with Sandbox(cpu=2000, memory=4096, name=name) as sb:
        print(f"Sandbox created with name: {name}")
        print(f"  id: {sb.id}")

        # Verify sandbox is working
        result = sb.commands.run("echo hello from named sandbox")
        print(f"  exec result: {result.stdout.strip()}")

        # Run ``adx-sandbox ls`` in a subprocess to show CLI consistency
        print("\n--- adx-sandbox ls / output ---")
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "adx_sandbox.cli", "ls", sb.id, "/"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            print(proc.stdout)
            if proc.returncode == 0:
                print("[OK] adx-sandbox CLI can access the named sandbox")
            else:
                print(f"[WARN] adx-sandbox CLI returned rc={proc.returncode}")
        except Exception as e:
            print(f"  (could not run adx-sandbox ls: {e})")

    print("\nSandbox terminated.")


if __name__ == "__main__":
    main()
