"""Basic Usage Example

Demonstrates core Sandbox features: command execution, filesystem operations,
background processes, and lifecycle management.

Prerequisites:
  - ADX_SERVER_ADDRESS and ADX_TOKEN environment variables must be set.

Usage:
  export ADX_SERVER_ADDRESS=your-server.example.com
  export ADX_TOKEN=your-token
  python basic_usage.py
"""

import tempfile
from pathlib import Path

from adx_sandbox import Sandbox


def main():
    with Sandbox(cpu=2000, memory=4096) as sb:
        print(f"Sandbox created: {sb.id}")

        # --- Command execution ---
        result = sb.commands.run("echo 'Hello from adx!'")
        print(f"stdout: {result.stdout.strip()}")
        print(f"exit_code: {result.exit_code}")

        # With environment variables
        result = sb.commands.run("echo $GREETING", envs={"GREETING": "Hi there"})
        print(f"env var: {result.stdout.strip()}")

        # With working directory
        sb.commands.run("mkdir -p /workspace")
        result = sb.commands.run("pwd", cwd="/workspace")
        print(f"cwd: {result.stdout.strip()}")

        # --- Filesystem operations ---
        sb.files.write("/tmp/hello.txt", "hello world")
        content = sb.files.read("/tmp/hello.txt")
        print(f"file content: {content}")

        # Directory listing
        sb.files.make_dir("/tmp/mydir")
        sb.files.write("/tmp/mydir/a.txt", "aaa")
        sb.files.write("/tmp/mydir/b.txt", "bbb")
        entries = sb.files.list("/tmp/mydir")
        for entry in entries:
            print(f"  {entry.type}: {entry.name} ({entry.size} bytes)")

        # File info
        info = sb.files.get_info("/tmp/hello.txt")
        print(f"file info: name={info.name}, size={info.size}")

        # Check existence
        print(f"exists: {sb.files.exists('/tmp/hello.txt')}")
        print(f"not exists: {sb.files.exists('/tmp/nonexistent')}")

        # Rename
        sb.files.rename("/tmp/hello.txt", "/tmp/hello_renamed.txt")
        print(f"renamed: {sb.files.exists('/tmp/hello_renamed.txt')}")

        # Remove
        sb.files.remove("/tmp/hello_renamed.txt")
        print(f"after remove: {sb.files.exists('/tmp/hello_renamed.txt')}")

        # --- File copy (local <-> sandbox) ---
        # Create a local file and upload it to the sandbox
        with tempfile.TemporaryDirectory() as directory:
            local_root = Path(directory)
            local_src = local_root / "upload.txt"
            local_src.write_text("local file content")
            sb.files.copy_from_local(str(local_src), "/tmp/uploaded.txt")
            print(f"uploaded: {sb.files.read('/tmp/uploaded.txt')}")

            # Download a file inside the owned temporary directory.
            sb.files.write("/tmp/to_download.txt", "sandbox content")
            local_dst = local_root / "download.txt"
            sb.files.copy_to_local("/tmp/to_download.txt", str(local_dst))
            print(f"downloaded: {local_dst.read_text()}")

            # copy_from_local streams a directory as one tar archive.
            local_dir = local_root / "uploaded_dir"
            local_dir.mkdir()
            (local_dir / "f1.txt").write_text("file1")
            (local_dir / "f2.txt").write_text("file2")
            sb.files.copy_from_local(str(local_dir), "/tmp/uploaded_dir")
            for entry in sb.files.list("/tmp/uploaded_dir"):
                print(f"  uploaded_dir/{entry.name}: {sb.files.read(entry.path)}")

        # --- Background process ---
        handle = sb.commands.run("sleep 10", background=True)
        print(f"background pid: {handle.pid}")

        processes = sb.commands.list()
        print(f"running processes: {len(processes)}")

        handle.kill()
        print("background process killed")

        # --- Lifecycle ---
        print(f"is running: {sb.is_running()}")
        info = sb.get_info()
        print(f"sandbox info: state={info.state}, cpu={info.cpu}, memory={info.memory}")

    print("Sandbox terminated.")


if __name__ == "__main__":
    main()
