//! Computer Use for the desktop: the headless core's types. Its tool calls
//! reach the transcript as dynamic-tool runtime requests (#55); the live
//! window preview waits for the daemon's `computerUse` thread item.

pub use waku_client::computer_use::*;
