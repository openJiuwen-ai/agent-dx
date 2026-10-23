//! Bounded filename/path glob search executed inside the Environment.
use super::codec::{kw_str, map_value};
use regex::{Regex, RegexBuilder};
use rmpv::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

const MAX_PATTERN_BYTES: usize = 1024;
const MAX_EXCLUSIONS: usize = 64;
const MAX_RESPONSE_BUDGET: usize = 4 * 1024 * 1024;

type SearchResult<T> = Result<T, SearchError>;

#[derive(Debug)]
struct SearchError {
    code: &'static str,
    message: String,
}

impl SearchError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_ARGUMENT",
            message: message.into(),
        }
    }

    fn io(path: &Path, error: std::io::Error) -> Self {
        let code = match error.kind() {
            std::io::ErrorKind::NotFound => "FILE_NOT_FOUND",
            std::io::ErrorKind::NotADirectory => "NOT_A_DIRECTORY",
            std::io::ErrorKind::PermissionDenied => "PERMISSION_DENIED",
            _ => "FILESYSTEM_ERROR",
        };
        Self {
            code,
            message: format!("file search {}: {error}", path.display()),
        }
    }

    fn limit(name: &str) -> Self {
        Self {
            code: "RESOURCE_EXHAUSTED",
            message: format!("file search {name} exceeded; narrow the search"),
        }
    }
}

fn bounded(
    kw: &BTreeMap<String, Value>,
    key: &str,
    default: usize,
    maximum: usize,
) -> SearchResult<usize> {
    match kw.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| (1..=maximum).contains(n))
            .ok_or_else(|| {
                SearchError::invalid(format!("{key} must be an integer in 1..={maximum}"))
            }),
    }
}

// Shell-style globs, matched case-sensitively against basename OR full path.
// '*' also matches separators, as AgentOS's fnmatch does. Braces are literal.
fn compile(pattern: &str) -> SearchResult<Regex> {
    if pattern.len() > MAX_PATTERN_BYTES {
        return Err(SearchError::invalid(
            "search patterns must be at most 1024 UTF-8 bytes",
        ));
    }
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut expression = String::from("\\A(?:");
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => expression.push_str(".*"),
            '?' => expression.push('.'),
            '[' => {
                let mut end = i + 1;
                if chars.get(end) == Some(&'!') {
                    end += 1;
                }
                if chars.get(end) == Some(&']') {
                    end += 1;
                }
                while end < chars.len() && chars[end] != ']' {
                    end += 1;
                }
                if end == chars.len() {
                    expression.push_str("\\[");
                } else {
                    expression.push('[');
                    let mut start = i + 1;
                    if chars.get(start) == Some(&'!') {
                        expression.push('^');
                        start += 1;
                    }
                    for ch in &chars[start..end] {
                        if matches!(ch, '\\' | '[' | ']' | '^' | '&' | '~') {
                            expression.push('\\');
                        }
                        expression.push(*ch);
                    }
                    expression.push(']');
                    i = end;
                }
            }
            literal => expression.push_str(&regex::escape(&literal.to_string())),
        }
        i += 1;
    }
    expression.push_str(")\\z");
    RegexBuilder::new(&expression)
        .dot_matches_new_line(true)
        .size_limit(256 * 1024)
        .build()
        .map_err(|error| SearchError::invalid(format!("invalid search pattern: {error}")))
}

struct Options {
    root: String,
    pattern: Regex,
    excludes: Vec<Regex>,
    depth: usize,
    max_results: usize,
    max_entries: usize,
    deadline: Instant,
}

impl Options {
    fn parse(kw: &BTreeMap<String, Value>, started: Instant) -> SearchResult<Self> {
        let root = kw_str(kw, "path")
            .filter(|p| !p.is_empty() && p.len() <= 4096)
            .ok_or_else(|| {
                SearchError::invalid("path must be a nonempty string of at most 4096 UTF-8 bytes")
            })?;
        let pattern = match kw.get("pattern") {
            None => "*",
            Some(value) => value
                .as_str()
                .ok_or_else(|| SearchError::invalid("pattern must be a string"))?,
        };
        let patterns = match kw.get("exclude_patterns") {
            None => &[][..],
            Some(Value::Array(values)) if values.len() <= MAX_EXCLUSIONS => values.as_slice(),
            _ => {
                return Err(SearchError::invalid(
                    "exclude_patterns must be an array of at most 64 strings",
                ))
            }
        };
        let excludes = patterns
            .iter()
            .map(|p| {
                p.as_str()
                    .ok_or_else(|| SearchError::invalid("exclude_patterns must contain strings"))
                    .and_then(compile)
            })
            .collect::<SearchResult<Vec<_>>>()?;
        let timeout = bounded(kw, "timeout_ms", 10_000, 30_000)?;
        Ok(Self {
            root,
            pattern: compile(pattern)?,
            excludes,
            depth: bounded(kw, "depth", 64, 64)?,
            max_results: bounded(kw, "max_results", 1000, 10_000)?,
            max_entries: bounded(kw, "max_entries", 100_000, 1_000_000)?,
            deadline: started
                + Duration::from_millis(
                    u64::try_from(timeout)
                        .map_err(|_| SearchError::invalid("invalid timeout_ms"))?,
                ),
        })
    }

    fn check_deadline(&self) -> SearchResult<()> {
        if Instant::now() >= self.deadline {
            return Err(SearchError {
                code: "DEADLINE_EXCEEDED",
                message: "file search timeout_ms exceeded".into(),
            });
        }
        Ok(())
    }

    fn matches(&self, name: &str, path: &str) -> bool {
        (self.pattern.is_match(name) || self.pattern.is_match(path))
            && !self
                .excludes
                .iter()
                .any(|p| p.is_match(name) || p.is_match(path))
    }

    fn run(&self) -> SearchResult<(Vec<Value>, usize)> {
        self.check_deadline()?;
        let root = Path::new(&self.root);
        match fs::metadata(root) {
            Ok(metadata) if metadata.is_dir() => (),
            Ok(_) => return Ok((Vec::new(), 0)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), 0))
            }
            Err(error) => return Err(SearchError::io(root, error)),
        }
        let mut stack = vec![(fs::read_dir(root).map_err(|e| SearchError::io(root, e))?, 1)];
        let mut entries = Vec::new();
        let mut scanned = 0;
        let mut response_budget = 0;
        while let Some((directory, level)) = stack.last_mut() {
            self.check_deadline()?;
            let level = *level;
            let child = match directory.next() {
                None => {
                    stack.pop();
                    continue;
                }
                Some(result) => result.map_err(|e| SearchError::io(root, e))?,
            };
            scanned += 1;
            if scanned > self.max_entries {
                return Err(SearchError::limit("max_entries"));
            }
            let path = child.path();
            let name = child.file_name().to_string_lossy().into_owned();
            let path_text = path.to_string_lossy().into_owned();
            let file_type = child.file_type().map_err(|e| SearchError::io(&path, e))?;
            if self.matches(&name, &path_text) {
                if entries.len() == self.max_results {
                    return Err(SearchError::limit("max_results"));
                }
                // Six bytes per source byte bounds JSON escaping; metadata adds
                // a fixed allowance. Reserve response overhead independently.
                response_budget += (name.len() + path_text.len()) * 6 + 256;
                if response_budget > MAX_RESPONSE_BUDGET - 128 {
                    return Err(SearchError::limit("response budget"));
                }
                let metadata =
                    fs::symlink_metadata(&path).map_err(|e| SearchError::io(&path, e))?;
                entries.push((
                    path_text.clone(),
                    map_value(super::fs::entry_fields_from_metadata(&path_text, &metadata)),
                ));
            }
            // Exclusions filter results, preserving AgentOS semantics: an excluded
            // directory name alone does not exclude its children. Never follow links.
            if file_type.is_dir() && level < self.depth {
                stack.push((
                    fs::read_dir(&path).map_err(|e| SearchError::io(&path, e))?,
                    level + 1,
                ));
            }
        }
        self.check_deadline()?;
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok((
            entries.into_iter().map(|(_, entry)| entry).collect(),
            scanned,
        ))
    }
}

pub(super) fn search(kw: &BTreeMap<String, Value>) -> Value {
    match Options::parse(kw, Instant::now()).and_then(|options| options.run()) {
        Ok((entries, scanned)) => map_value(vec![
            ("entries", Value::Array(entries)),
            ("scanned_entries", Value::from(scanned)),
            ("error", Value::Nil),
        ]),
        Err(error) => map_value(vec![
            ("entries", Value::Array(Vec::new())),
            ("error_code", Value::from(error.code)),
            ("error", Value::from(error.message)),
        ]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs_preserve_fnmatch_literals_unicode_and_separator_matching() {
        for (pattern, name, expected) in [
            ("*.py", "sub/中文.py", true),
            ("*.py", "UP.PY", false),
            ("[!a-c]?.txt", "中文.txt", true),
            ("[abc", "[abc", true),
            ("[]]", "]", true),
            ("[^]", "^", true),
            ("a{b,c}", "ab", false),
            ("a{b,c}", "a{b,c}", true),
            ("*.py", ".hidden.py", true),
            ("*", "a\nb", true),
            ("[[]", "[", true),
            ("a\\b", "a\\b", true),
        ] {
            assert_eq!(
                compile(pattern).unwrap().is_match(name),
                expected,
                "{pattern:?} {name:?}"
            );
        }
    }

    #[test]
    fn response_budget_rejects_large_result_sets_without_partial_success() {
        let directory = tempfile::tempdir().unwrap();
        for i in 0..1500 {
            std::fs::write(
                directory.path().join(format!("{i:04}-{}", "x".repeat(230))),
                b"x",
            )
            .unwrap();
        }
        let args = BTreeMap::from([
            (
                "path".into(),
                Value::from(directory.path().to_string_lossy().as_ref()),
            ),
            ("max_results".into(), Value::from(10000)),
        ]);
        let error = Options::parse(&args, Instant::now())
            .unwrap()
            .run()
            .unwrap_err();
        assert_eq!(error.code, "RESOURCE_EXHAUSTED");
        assert!(error.message.contains("response budget"));
    }

    #[test]
    fn excluded_directory_entry_does_not_prune_eligible_children() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("hidden.py")).unwrap();
        std::fs::write(directory.path().join("hidden.py/keep.py"), b"x").unwrap();
        let args = BTreeMap::from([
            (
                "path".into(),
                Value::from(directory.path().to_string_lossy().as_ref()),
            ),
            ("pattern".into(), Value::from("*.py")),
            (
                "exclude_patterns".into(),
                Value::Array(vec![Value::from("hidden.py")]),
            ),
        ]);
        let (entries, scanned) = Options::parse(&args, Instant::now())
            .unwrap()
            .run()
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(scanned, 2);
    }

    #[test]
    fn expired_deadline_does_not_start_traversal() {
        let options = Options {
            root: "/not-used".into(),
            pattern: compile("*").unwrap(),
            excludes: vec![],
            depth: 64,
            max_results: 1000,
            max_entries: 100_000,
            deadline: Instant::now() - Duration::from_millis(1),
        };
        assert_eq!(options.run().unwrap_err().code, "DEADLINE_EXCEEDED");
    }
}
