"""Persistent Shell Example

Demonstrates the persistent shell feature: commands share the same bash
session so cwd, environment variables, and shell functions are preserved
across calls.

Prerequisites:
  - ADX_SERVER_ADDRESS and ADX_TOKEN environment variables must be set.

Usage:
  export ADX_SERVER_ADDRESS=your-server.example.com
  export ADX_TOKEN=your-token
  python persistent_bash.py
"""

import asyncio

from adx_sandbox import Sandbox


async def main():
    with Sandbox(cpu=2000, memory=4096) as sb:
        print(f"Sandbox created: {sb.id}")

        # --- Create a persistent shell ---
        sh = await sb.shells.create(cwd="/tmp")
        print("Shell created")

        # --- cwd is preserved ---
        result = await sh.run("pwd")
        print(f"pwd: {result.stdout.strip()}")  # /tmp

        result = await sh.run("cd /")
        result = await sh.run("pwd")
        print(f"pwd after cd: {result.stdout.strip()}")  # /

        # --- Environment variables are preserved ---
        await sh.run("export MY_VAR='hello from persistent shell'")
        result = await sh.run("echo $MY_VAR")
        print(f"echo $MY_VAR: {result.stdout.strip()}")

        # --- Shell functions are preserved ---
        await sh.run('greet() { echo "Hello, $1!"; }')
        result = await sh.run("greet World")
        print(f"greet World: {result.stdout.strip()}")

        # --- Multi-line commands ---
        result = await sh.run('for i in 1 2 3; do echo "item $i"; done')
        print(f"loop output:\n{result.stdout}")

        # --- Return code is captured ---
        result = await sh.run("ls /nonexistent 2>&1")
        print(f"ls /nonexistent: exit_code={result.exit_code}")

        result = await sh.run("true")
        print(f"true: exit_code={result.exit_code}")

        # --- One-shot cwd/envs (does not persist) ---
        result = await sh.run(
            "pwd && echo $TEMP_VAR", cwd="/tmp", envs={"TEMP_VAR": "temporary"}
        )
        print(f"one-shot cwd+env: {result.stdout.strip()}")

        result = await sh.run("pwd")
        print(f"pwd after one-shot: {result.stdout.strip()}")  # back to /

        # --- Kill and recreate ---
        await sh.kill()
        print("\nShell killed, creating new one...")

        sh2 = await sb.shells.create()
        result = await sh2.run("echo $MY_VAR")
        print(f"$MY_VAR in new shell: '{result.stdout.strip()}'")  # empty

        # --- Multiple shells ---
        sh_a = await sb.shells.create(cwd="/tmp")
        sh_b = await sb.shells.create(cwd="/")
        result_a = await sh_a.run("pwd")
        result_b = await sh_b.run("pwd")
        print(f"\nshell A pwd: {result_a.stdout.strip()}")  # /tmp
        print(f"shell B pwd: {result_b.stdout.strip()}")  # /

        await sh2.kill()
        await sh_a.kill()
        await sh_b.kill()

        # --- Stateless commands still work ---
        result = sb.commands.run("echo 'stateless command'")
        print(f"\nstateless: {result.stdout.strip()}")

    print("\nSandbox terminated.")


if __name__ == "__main__":
    asyncio.run(main())
