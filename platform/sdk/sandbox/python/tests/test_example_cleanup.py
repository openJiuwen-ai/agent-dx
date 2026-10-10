"""Examples release local files even when setup or a remote copy fails."""

import importlib.util
import socket
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


def load_example(name):
    path = Path(__file__).resolve().parents[1] / 'examples' / f'{name}.py'
    spec = importlib.util.spec_from_file_location(f'cleanup_{name}', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('name', ['reverse_tunnel', 'tunnel_large_response'])
def test_server_bind_failure_removes_owned_directory(name, tmp_path, monkeypatch):
    module = load_example(name)
    monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))
    if name == 'tunnel_large_response':
        monkeypatch.setattr(module, 'TEST_SIZES', [('fixture', 1024)])
    with socket.socket() as occupied:
        occupied.bind(('127.0.0.1', 0))
        occupied.listen()
        with pytest.raises(OSError):
            module.start_local_server(occupied.getsockname()[1])
    assert list(tmp_path.iterdir()) == []


def test_upload_failure_removes_owned_local_file(tmp_path, monkeypatch):
    module = load_example('basic_usage')
    monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))
    with patch.object(module, 'Sandbox') as sandbox:
        files = sandbox.return_value.__enter__.return_value.files
        files.list.return_value = []
        files.copy_from_local.side_effect = RuntimeError('injected upload failure')
        with pytest.raises(RuntimeError, match='injected upload failure'):
            module.main()
    assert list(tmp_path.iterdir()) == []
