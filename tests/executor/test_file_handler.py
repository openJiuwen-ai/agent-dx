#!/usr/bin/env python3
# coding=UTF-8

import io
import stat

import pytest

from yr.agentexecutor.file_handler import FileHandler, FileListTimeoutError


def test_upload_is_atomic_and_listable(tmp_path):
    target = tmp_path / "nested" / "file.bin"
    handler = FileHandler(max_file_size=16)

    result = handler.upload(str(target), io.BytesIO(b"content"))

    assert result == {"success": True, "path": str(target), "size": 7}
    assert target.read_bytes() == b"content"
    assert not list(target.parent.glob("*.upload"))
    assert handler.list(str(target.parent))["items"][0]["path"] == str(target)


def test_upload_applies_mode_before_atomic_replace(tmp_path):
    target = tmp_path / "file.bin"

    result = FileHandler().upload(str(target), io.BytesIO(b"content"), mode="640")

    assert result["size"] == 7
    assert stat.S_IMODE(target.stat().st_mode) == 0o640


def test_upload_rejects_invalid_mode_before_creating_file(tmp_path):
    target = tmp_path / "file.bin"

    with pytest.raises(ValueError, match="expected 3-4 digit octal"):
        FileHandler().upload(str(target), io.BytesIO(b"content"), mode="888")

    assert not target.exists()
    assert not list(tmp_path.glob("*.upload"))


def test_upload_rejects_oversized_file_and_removes_temporary_file(tmp_path):
    target = tmp_path / "file.bin"
    handler = FileHandler(max_file_size=3)

    with pytest.raises(ValueError, match="exceeds max"):
        handler.upload(str(target), io.BytesIO(b"four"))

    assert not target.exists()
    assert not list(tmp_path.glob("*.upload"))


def test_open_download_returns_requested_range(tmp_path):
    target = tmp_path / "file.bin"
    target.write_bytes(b"0123456789")
    handler = FileHandler()

    stream, total, end, length = handler.open_download(str(target), 2, 5)
    with stream:
        assert stream.read(length) == b"2345"
    assert (total, end, length) == (10, 5, 4)


def test_list_includes_dangling_symlink_without_failing(tmp_path):
    dangling = tmp_path / "dangling"
    dangling.symlink_to(tmp_path / "missing")

    items = FileHandler().list(str(tmp_path))["items"]

    assert len(items) == 1
    assert items[0]["path"] == str(dangling)
    assert items[0]["is_directory"] is False
    assert items[0]["type"] == "file"


def test_recursive_list_applies_default_depth_limit(tmp_path):
    current = tmp_path
    for index in range(5):
        current = current / f"depth-{index}"
        current.mkdir()
        (current / "file.txt").write_text("content", encoding="utf-8")

    handler = FileHandler(max_list_depth=2)
    items = handler.list(str(tmp_path), recursive=True)["items"]

    assert all("depth-2" not in item["path"] for item in items)


def test_recursive_list_applies_entry_limit(tmp_path):
    for index in range(5):
        (tmp_path / f"file-{index}.txt").write_text("content", encoding="utf-8")

    handler = FileHandler(max_list_entries=3)

    assert len(handler.list(str(tmp_path), recursive=True)["items"]) == 3


def test_recursive_list_times_out(tmp_path):
    (tmp_path / "file.txt").write_text("content", encoding="utf-8")
    handler = FileHandler(list_timeout_seconds=0)

    with pytest.raises(FileListTimeoutError, match="file list exceeded"):
        handler.list(str(tmp_path), recursive=True)


def test_mkdir_creates_single_directory(tmp_path):
    target = tmp_path / "sub"

    result = FileHandler().mkdir(str(target))

    assert result == {"success": True, "path": str(target), "created": True}
    assert target.is_dir()


def test_mkdir_recursive_creates_intermediate_parents(tmp_path):
    target = tmp_path / "a" / "b" / "c"

    result = FileHandler().mkdir(str(target), recursive=True)

    assert result == {"success": True, "path": str(target), "created": True}
    assert target.is_dir()


def test_mkdir_on_existing_directory_is_idempotent(tmp_path):
    target = tmp_path / "sub"
    target.mkdir()
    target.chmod(0o755)

    result = FileHandler().mkdir(str(target))

    assert result == {"success": True, "path": str(target), "created": False}
    assert target.is_dir()


def test_mkdir_mode_is_applied_exactly_ignoring_umask(tmp_path):
    target = tmp_path / "secret"

    FileHandler().mkdir(str(target), mode="0700")

    assert stat.S_IMODE(target.stat().st_mode) == 0o700


def test_mkdir_rejects_invalid_mode(tmp_path):
    target = tmp_path / "sub"

    with pytest.raises(ValueError, match="expected 3-4 digit octal"):
        FileHandler().mkdir(str(target), mode="999")

    assert not target.exists()


def test_mkdir_non_recursive_with_missing_parent_raises(tmp_path):
    target = tmp_path / "missing" / "deep"

    with pytest.raises(ValueError, match="parent does not exist"):
        FileHandler().mkdir(str(target), recursive=False)

    assert not target.exists()
