//! Computer Use helper processes: their per-session registry directory and
//! cleanup.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use uuid::Uuid;

use crate::fs_ext;

#[cfg(target_os = "macos")]
use std::ffi::OsString;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt as _;

pub(super) fn create_process_directory() -> anyhow::Result<PathBuf> {
    let directory = std::env::temp_dir()
        .join("waku-computer-use")
        .join(Uuid::new_v4().simple().to_string());
    fs::create_dir_all(&directory).with_context(|| {
        format!(
            "could not create Computer Use process directory {}",
            directory.display()
        )
    })?;
    fs_ext::restrict_to_owner(&directory).with_context(|| {
        format!(
            "could not secure Computer Use process directory {}",
            directory.display()
        )
    })?;
    Ok(directory)
}

pub(super) fn stop_registered_processes(directory: &Path, helper_executable: &Path) {
    let expected_executable =
        fs::canonicalize(helper_executable).unwrap_or_else(|_| helper_executable.to_path_buf());
    for (pid, registration) in registered_processes(directory) {
        if process_executable(pid).as_deref() == Some(expected_executable.as_path()) {
            // macOS owns a Launch Services bridge; closing it interrupts the
            // native SDK. Portable hosts poll a cancellation marker so they
            // can cancel the operation and await SDK shutdown on both OSes.
            #[cfg(target_os = "macos")]
            unsafe {
                libc::kill(pid, libc::SIGTERM);
            }
            #[cfg(not(target_os = "macos"))]
            let _ = fs::write(directory.join(format!("cancel-{pid}")), b"");
        }
        let _ = fs::remove_file(registration);
    }
}

pub(super) fn registered_processes(directory: &Path) -> Vec<(i32, PathBuf)> {
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            if !entry.file_type().ok()?.is_file() {
                return None;
            }
            let pid = entry.file_name().to_str()?.parse::<i32>().ok()?;
            (pid > 1).then_some((pid, entry.path()))
        })
        .collect()
}

#[cfg(target_os = "macos")]
pub(super) fn process_executable(pid: i32) -> Option<PathBuf> {
    let mut buffer = vec![0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    let length = unsafe {
        libc::proc_pidpath(
            pid,
            buffer.as_mut_ptr().cast(),
            libc::PROC_PIDPATHINFO_MAXSIZE as u32,
        )
    };
    if length <= 0 {
        return None;
    }
    buffer.truncate(length as usize);
    Some(PathBuf::from(OsString::from_vec(buffer)))
}

#[cfg(target_os = "linux")]
pub(super) fn process_executable(pid: i32) -> Option<PathBuf> {
    fs::read_link(format!("/proc/{pid}/exe")).ok()
}

#[cfg(target_os = "windows")]
pub(super) fn process_executable(pid: i32) -> Option<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, QueryFullProcessImageNameW,
    };
    unsafe {
        let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid as u32);
        if process.is_null() {
            return None;
        }
        let mut buffer = vec![0u16; 32768];
        let mut len = buffer.len() as u32;
        let success = QueryFullProcessImageNameW(process, 0, buffer.as_mut_ptr(), &mut len);
        CloseHandle(process);
        (success != 0)
            .then(|| PathBuf::from(std::ffi::OsString::from_wide(&buffer[..len as usize])))
            .and_then(|path| fs::canonicalize(path).ok())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
pub(super) fn process_executable(_: i32) -> Option<PathBuf> {
    None
}
