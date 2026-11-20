"""Hold a capacity-file read in a real FIFO without stopping adxlet."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def unblock(root):
    marker = root / 'resource-block.json'
    if not marker.exists():
        return
    state = json.loads(marker.read_text())
    (root / 'capacity-before-block.json').replace(root / 'capacity.json')
    try:
        os.kill(state['keeper_pid'], signal.SIGTERM)
    except ProcessLookupError:
        pass
    marker.unlink()


def block(root, pid):
    capacity = root / 'capacity.json'
    capacity.rename(root / 'capacity-before-block.json')
    os.mkfifo(capacity, 0o600)
    keeper = subprocess.Popen(
        [sys.executable, '-c',
         'import os,signal,sys; fd=os.open(sys.argv[1],os.O_RDWR); signal.pause()',
         str(capacity)],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    state = {'keeper_pid': keeper.pid, 'adxlet_pid': pid}
    (root / 'resource-block.json').write_text(json.dumps(state))
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if keeper.poll() is not None:
            raise RuntimeError('capacity FIFO keeper exited')
        descriptors = set()
        for fd in Path(f'/proc/{pid}/fd').iterdir():
            try:
                if os.readlink(fd) == str(capacity):
                    descriptors.add(int(fd.name))
            except FileNotFoundError:
                continue
        for thread in Path(f'/proc/{pid}/task').iterdir():
            try:
                wait = (thread / 'wchan').read_text().strip()
                syscall = (thread / 'syscall').read_text().split()
            except FileNotFoundError:
                continue
            if 'pipe_read' in wait and len(syscall) > 1 and int(syscall[1], 0) in descriptors:
                state.update(blocked_thread=int(thread.name), wait_channel=wait, blocked_at=time.monotonic())
                (root / 'resource-block.json').write_text(json.dumps(state))
                return state
        time.sleep(0.1)
    raise TimeoutError('adxlet did not enter a blocked capacity-file read')
