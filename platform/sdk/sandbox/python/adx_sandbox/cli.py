"""Minimal ``adx-sandbox`` CLI over the sandbox v1 HTTP transport.

Subcommands: create / delete / exec / ls. Kept intentionally small — the CLI
is a thin convenience wrapper, not the primary interface.
"""

import argparse
import sys

from ._transport import SandboxClient


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="adx-sandbox", description="Agent DX sandbox CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="create a sandbox")
    p_create.add_argument("--image")
    p_create.add_argument("--cpu", type=int, default=1000)
    p_create.add_argument("--memory", type=int, default=4096)

    p_delete = sub.add_parser("delete", help="delete a sandbox by id")
    p_delete.add_argument("sandbox_id")

    p_exec = sub.add_parser("exec", help="run a command in a sandbox")
    p_exec.add_argument("sandbox_id")
    p_exec.add_argument("command")
    p_exec.add_argument("--timeout", type=int, default=60)

    p_ls = sub.add_parser("ls", help="list a directory in a sandbox")
    p_ls.add_argument("sandbox_id")
    p_ls.add_argument("path")

    args = parser.parse_args(argv)
    client = SandboxClient()
    try:
        if args.cmd == "create":
            body = {"cpu": args.cpu, "memory": args.memory, "namespace": "default"}
            if args.image:
                body["rootfs"] = {
                    "runtime": "runsc",
                    "type": "image",
                    "readonly": False,
                    "imageurl": args.image,
                }
            print(client.create(body))
        elif args.cmd == "delete":
            client.delete(args.sandbox_id)
            print(f"deleted {args.sandbox_id}")
        elif args.cmd == "exec":
            result = client.invoke(
                args.sandbox_id,
                "process.exec",
                {"cmd": args.command, "timeout": args.timeout},
                timeout=args.timeout,
            )
            sys.stdout.write(result.get("stdout", ""))
            sys.stderr.write(result.get("stderr", ""))
            return int(result.get("exit_code", 0))
        elif args.cmd == "ls":
            result = client.invoke(
                args.sandbox_id, "file.list", {"path": args.path, "depth": 1}
            )
            for e in result.get("entries", []):
                print(f"{e['permissions']}\t{e['size']}\t{e['name']}")
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
