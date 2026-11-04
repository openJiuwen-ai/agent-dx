#!/usr/bin/env python3
"""Only invoked inside cadx-build container AFTER pristine host-source rsync."""
import pathlib,hashlib
root=pathlib.Path('/workspace')
p=root/'afs/src/node.rs';s=p.read_text();needle='    let mut services = Services::new();'
assert s.count(needle)==1
s=s.replace(needle,'''    // Test-only direct Backend call, same Node construction and services.
    if let Ok(root) = std::env::var("CADX_DIRECT_PROBE_ROOT") {
        if let Some(fs) = state.dfs.clone() {
            cadx_direct_probe(fs, std::path::PathBuf::from(root), tokio::runtime::Handle::current());
        }
    }
'''+needle)
s+='\ninclude!("../examples/entrance_probe/direct_hook.rs");\n';p.write_text(s)
p=root/'afs/src/node/chunk/store.rs';s=p.read_text();needle='''self.coarse_diagnostic("persist_body_sync", item.chunk.length);
                    file.sync_all().map_err(Error::from)?;'''
assert s.count(needle)==1,s.count(needle)
s=s.replace(needle,'''self.coarse_diagnostic("persist_body_sync", item.chunk.length);
                    if std::env::var_os("CADX_SKIP_BODY_SYNC").is_none() {
                        file.sync_all().map_err(Error::from)?;
                    }''');p.write_text(s)
for path in ['afs/src/node.rs','afs/src/node/chunk/store.rs']:
 print(path,hashlib.sha256((root/path).read_bytes()).hexdigest())
