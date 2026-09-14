import os, sys, asyncio, tempfile
os.environ["ADX_TLS"] = "0"
os.environ.setdefault("ADX_SERVER_ADDRESS", "172.21.0.2:8889")
os.environ.setdefault("ADX_TOKEN", "x")
from adx_sandbox import Sandbox

P, F = [], []
def chk(name, cond, detail=""):
    (P if cond else F).append(name)
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else f"  {detail}" if detail else ""))

sb = Sandbox(image="aio-adx-runtime:latest", name="sdke2e")
chk("create", bool(sb.id), f"id={sb.id}")
try:
    # filesystem
    ei = sb.files.write("/tmp/a.txt", "hello sdk")
    chk("files.write", ei.size == 9, f"size={ei.size}")
    chk("files.read", sb.files.read("/tmp/a.txt") == "hello sdk")
    chk("files.exists", sb.files.exists("/tmp/a.txt") is True)
    chk("files.make_dir", sb.files.make_dir("/tmp/sub/x") is True)
    lst = sb.files.list("/tmp", depth=1)
    chk("files.list", any(e.name == "a.txt" for e in lst), f"n={len(lst)}")
    info = sb.files.get_info("/tmp/a.txt")
    chk("files.get_info(file.stat)", info.type == "file" and info.size == 9, f"type={info.type}")
    ren = sb.files.rename("/tmp/a.txt", "/tmp/b.txt")
    chk("files.rename", ren.name == "b.txt")
    sb.files.remove("/tmp/b.txt")
    chk("files.remove", sb.files.exists("/tmp/b.txt") is False)
    # binary read/write
    sb.files.write("/tmp/bin", b"\x00\x01\x02\xff")
    chk("files.write/read bytes", sb.files.read("/tmp/bin", format="bytes") == b"\x00\x01\x02\xff")

    # commands sync
    r = sb.commands.run("echo out && echo err >&2; exit 3")
    chk("commands.run sync", r.exit_code == 3 and "out" in r.stdout, f"rc={r.exit_code} out={r.stdout!r}")
    # commands long (start+poll path, timeout>30)
    r2 = sb.commands.run("echo slow", timeout=35)
    chk("commands.run poll-path", r2.exit_code == 0 and "slow" in r2.stdout, f"out={r2.stdout!r}")
    # commands background + stdin
    h = sb.commands.run("cat", background=True, stdin=True)
    chk("commands.run background", h.pid > 0, f"pid={h.pid}")
    h.send_stdin("piped-input\n", eof=True)
    rb = h.wait(timeout=20)
    chk("CommandHandle.send_stdin+wait", "piped-input" in rb.stdout, f"out={rb.stdout!r}")
    plist = sb.commands.list()
    chk("commands.list", isinstance(plist, list))

    # shells (async)
    async def shell_test():
        sh = await sb.shells.create(cwd="/tmp")
        r1 = await sh.run("export FOO=bar; echo started")
        r2 = await sh.run("echo FOO=$FOO")          # state persists
        r3 = await sh.run("pwd")                      # cwd=/tmp
        await sh.kill()
        return r1, r2, r3
    r1, r2, r3 = asyncio.run(shell_test())
    chk("shells.create+run started", r1.exit_code == 0 and r1.stdout == "started", f"out={r1.stdout!r}")
    chk("shell state persists (clean output)", r2.stdout == "FOO=bar", f"out={r2.stdout!r}")
    chk("shell cwd", r3.stdout == "/tmp", f"out={r3.stdout!r}")

    # Direct HTTP copy upload/download
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        f.write("ws-small-payload-64b-xxxxxxxxxxxxxxxxxxxxx"); local_up = f.name
    sb.files.copy_from_local(local_up, "/tmp/up.dat")
    remote_content = sb.files.read("/tmp/up.dat")
    chk("files.copy_from_local (direct)", remote_content == "ws-small-payload-64b-xxxxxxxxxxxxxxxxxxxxx", f"len={len(remote_content)}")
    local_dn = local_up + ".dn"
    sb.files.copy_to_local("/tmp/up.dat", local_dn)
    chk("files.copy_to_local (direct)", open(local_dn).read() == "ws-small-payload-64b-xxxxxxxxxxxxxxxxxxxxx")

    # lifecycle
    chk("is_running", sb.is_running() is True)
    gi = sb.get_info()
    chk("get_info", gi.state == "running" and gi.image == "aio-adx-runtime:latest", f"state={gi.state}")
finally:
    sb.kill()
    chk("kill", sb.is_running() is False)

print(f"\n==== {len(P)} PASS, {len(F)} FAIL ====")
if F: print("FAILED:", F); sys.exit(1)
