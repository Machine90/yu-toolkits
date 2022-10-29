//! `Partitions` is a useful tool for distributed application, which use
//! Btree map implement a sparse indexes to maitain some partitions with 
//! key range in it, so that application can find the partition which a 
//! specific key belongs.
//! The `Partitions` support add, split, compact, remove and find operation.

pub mod key;
pub mod index;
pub mod partition;
