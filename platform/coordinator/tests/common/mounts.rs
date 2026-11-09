use std::path::Path;

/// Match the actual mount containing a test's Volume root, including nested mounts.
pub fn backend_id(path: &Path) -> String {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            let root = Path::new(fields[4]);
            path.starts_with(root).then(|| {
                (
                    root.as_os_str().len(),
                    fields[fields.iter().position(|field| *field == "-").unwrap() + 2].to_owned(),
                )
            })
        })
        .max_by_key(|(length, _)| *length)
        .expect("test path has a mounted filesystem")
        .1
}
