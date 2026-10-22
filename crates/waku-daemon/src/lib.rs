//! The daemon's command-line tools besides serving (#58): installing it as
//! a background service and `doctor`. The binaries (`waku-daemon`, and the
//! debug build's `waku-debug-daemon`, which includes the same `main.rs`)
//! dispatch `service` and `doctor` here.

pub mod doctor;
pub mod service;
