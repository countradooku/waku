//! Integration tests over a real `state.sqlite` in a temporary directory.
//! They live inside the crate so they can arm the sink's test-only fault
//! points.

mod common;

mod crash;
mod effects;
mod feed;
mod forward_compat;
mod idempotency;
mod migrations;
mod rebuild;
mod serialization;
mod streams;
