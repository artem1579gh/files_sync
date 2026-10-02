//! The "unsafe symlink" check: a faithful port of rsync's
//! `util1.c:unsafe_symlink()` as of rsync 3.4.1 (design §4.2).
//!
//! rsync 3.4.0 (CVE-2024-12088, backported by distributions to older
//! versions) added two lexical rejections in front of the classic depth walk:
//! a `/../` anywhere after the leading run of `../` components, and a trailing
//! `/..`. So `a/../x` is unsafe even though it never leaves the root, because
//! `a` could later be replaced by a symlink.
//!
//! The check is lexical and only decides how a link is *classified*. Security
//! never depends on it: escapes are stopped by the kernel (`RESOLVE_BENEATH`,
//! design §5.1).

use crate::fs::RelPath;

/// Whether the symlink at `link` (relative to the replica root) with target
/// `target` is unsafe in rsync's sense: absolute, empty, or leaving the root
/// through `..` at some point while walking it from the link's directory.
pub fn is_unsafe(link: &RelPath, target: &[u8]) -> bool {
    unsafe_symlink(target, link.as_bytes())
}

/// rsync's `unsafe_symlink(dest, src)`, byte for byte: `dest` is the link
/// target, `src` the link's own path relative to the transfer root.
///
/// Quirks kept on purpose:
/// - `..` components are only allowed as a leading run (`../../x`,
///   `..//../x`); any later `/../`, or a `/..` at the end, is unsafe outright;
/// - a `..` component in `src` resets the safety margin to 0, `.` is ignored;
/// - repeated `/` are skipped after a component (but a leading `/` in `src`
///   counts as an empty component, so it adds depth);
/// - in `dest`, only `..` and `.` *exactly* are special (`...`, `.x` are names);
///   a trailing `..` counts, a trailing `.` or name does not;
/// - like C strings, both inputs end at the first NUL byte.
pub fn unsafe_symlink(dest: &[u8], src: &[u8]) -> bool {
    let dest = until_nul(dest);
    let src = until_nul(src);

    // All absolute and empty symlinks are unsafe.
    if dest.first().is_none_or(|&b| b == b'/') {
        return true;
    }

    // Reject `/../` other than in the leading run of `../` (`..//../` included).
    let mut dest2 = dest;
    while let Some(rest) = dest2.strip_prefix(b"../") {
        dest2 = &rest[rest.iter().take_while(|&&b| b == b'/').count()..];
    }
    if dest2.windows(4).any(|w| w == b"/../") {
        return true;
    }
    // Reject a trailing `/..`.
    if dest.len() > 3 && dest.ends_with(b"/..") {
        return true;
    }

    // Find out what our safety margin is.
    let mut depth: i64 = 0;
    let mut name = src;
    while let Some(slash) = name.iter().position(|&b| b == b'/') {
        // A ".." segment starts the count over; a "." segment is ignored.
        match dot_component(name) {
            Some(Dot::Up) => depth = 0,
            Some(Dot::Here) => {}
            None => depth += 1,
        }
        name = skip_slashes(&name[slash..]);
    }
    if name == b".." {
        depth = 0;
    }

    let mut name = dest;
    while let Some(slash) = name.iter().position(|&b| b == b'/') {
        match dot_component(name) {
            // If at any point we go outside the current directory, it is unsafe.
            Some(Dot::Up) => {
                depth -= 1;
                if depth < 0 {
                    return true;
                }
            }
            Some(Dot::Here) => {}
            None => depth += 1,
        }
        name = skip_slashes(&name[slash..]);
    }
    if name == b".." {
        depth -= 1;
    }

    depth < 0
}

enum Dot {
    Here,
    Up,
}

/// rsync's test `*name == '.' && (name[1] == '/' || (name[1] == '.' && name[2] == '/'))`
/// on the text starting at a component (which is followed by a `/`).
fn dot_component(name: &[u8]) -> Option<Dot> {
    match name {
        [b'.', b'/', ..] => Some(Dot::Here),
        [b'.', b'.', b'/', ..] => Some(Dot::Up),
        _ => None,
    }
}

/// `name` starts at a `/`; returns the text after it and after any `/`s
/// directly following it (rsync's `while (slash[1] == '/') slash++; name = slash+1`).
fn skip_slashes(name: &[u8]) -> &[u8] {
    let n = name.iter().take_while(|&&b| b == b'/').count();
    &name[n..]
}

fn until_nul(s: &[u8]) -> &[u8] {
    s.iter().position(|&b| b == 0).map_or(s, |i| &s[..i])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(link path, target, unsafe?)`. Each one was checked against
    /// `rsync -a --safe-links` (3.2.7 with the 3.4.0 fix backported).
    const CASES: &[(&str, &str, bool)] = &[
        // Absolute and empty targets are always unsafe.
        ("l", "/etc/passwd", true),
        ("a/b/l", "/", true),
        ("a/l", "//x", true),
        ("l", "", true),
        // Plain relative targets.
        ("l", "x", false),
        ("l", "x/y/z", false),
        ("l", ".", false),
        ("l", "./x", false),
        ("l", "x/", false),
        ("l", "...", false),
        ("l", "..x/y", false),
        ("l", "./y/z/.", false),
        // A leading run of `..` is counted against the link's depth.
        ("l", "..", true),
        ("l", "../x", true),
        ("l", "../", true),
        ("a/l", "..", false),
        ("a/l", "../x", false),
        ("a/l", "../", false),
        ("a/l", "../../x", true),
        ("a/b/c/l", "../../../x", false),
        ("a/b/c/l", "../../../../x", true),
        ("a/b/l", "../../a/b/c", false),
        ("a/b/l", "..//..//x", false),
        ("a/b/l", "..//..//..//x", true),
        // `.` and repeated `/` are ignored by the walk.
        ("l", "./../x", true),
        ("a/l", ".//x//y", false),
        // A trailing `/..` is unsafe even when it stays inside the root.
        ("a/b/l", "../..", true),
        ("a/l", "x/..", true),
        ("l", ".hidden/..", true),
        ("l", "..../..", true),
        ("a/b/l", "..//..", true),
        // So is `/../` after anything but the leading `../` run.
        ("l", "a/../x", true),
        ("l", "a/b/../../x", true),
        ("a/l", "../x/../y", true),
        ("a/l", "./.././x", true),
        ("a/l", "x//..//y", true),
        ("a/l", "./y/../z", true),
        // Only exact `..` components count: `x../` and `/...` are names.
        ("l", "x../y", false),
        ("l", "x/.../y", false),
        // Bytes that are not UTF-8 are just names.
        ("\u{e9}/l", "../\u{fffd}", false),
    ];

    #[test]
    fn table() {
        assert!(CASES.len() >= 20);
        for &(link, target, want) in CASES {
            let link = RelPath::new(link).unwrap();
            assert_eq!(is_unsafe(&link, target.as_bytes()), want, "{link} -> {target:?}");
        }
    }

    #[test]
    fn raw_src_quirks() {
        // rsync's own path argument may be unclean; these follow its C code.
        // A `..` in the link path resets the margin.
        assert!(unsafe_symlink(b"../x", b"a/../l"));
        assert!(!unsafe_symlink(b"../x", b"a/../b/l"));
        // `.` in the link path is ignored; repeated `/` are skipped.
        assert!(unsafe_symlink(b"../x", b"./l"));
        assert!(!unsafe_symlink(b"../x", b"a//l"));
        assert!(unsafe_symlink(b"../../x", b"a//l"));
        // A leading `/` is an empty component and adds depth.
        assert!(!unsafe_symlink(b"../x", b"/l"));
        // A link path ending in `..` has margin 0.
        assert!(unsafe_symlink(b"../x", b"a/.."));
        // Input ends at the first NUL, like a C string.
        assert!(unsafe_symlink(b"\0x", b"l"));
        assert!(!unsafe_symlink(b"x\0/../../..", b"l"));
        assert!(!unsafe_symlink(b"../x", b"a/l\0/../.."));
        // Non-UTF-8 bytes.
        assert!(!unsafe_symlink(b"\xff/\xfe", b"\x80"));
        assert!(unsafe_symlink(b"\xff/../\xfe", b"a/\x80"));
    }
}
