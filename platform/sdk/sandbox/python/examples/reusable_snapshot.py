"""Create a reusable Snapshot and restore an independent clone."""

from adx_sandbox import Sandbox


def main() -> None:
    source = Sandbox(name="snapshot-source")
    clone = None
    snapshot = None
    try:
        source.files.write("/tmp/marker", "from reusable snapshot")
        snapshot = source.create_snapshot(name="python-ready")
        clone = Sandbox.create(snapshot, name="snapshot-clone")
        assert clone.files.read("/tmp/marker") == "from reusable snapshot"
        print(f"Restored {clone.id} from {snapshot.snapshot_id}")
    finally:
        if clone is not None:
            clone.kill()
        source.kill()
        if snapshot is not None:
            Sandbox.delete_snapshot(snapshot.snapshot_id)


if __name__ == "__main__":
    main()
