use std::{
    io::{Result, Write},
    sync::Arc,
};

use crate::fast_cp::{progress::Progress, MetaInfo};

pub type ReceiveHandlerRef<W> = Arc<dyn ReceiveHandler<W>>;

/// Receive handler of [Receiver](crate::fast_cp::sync::cp_receiver::Receiver)
/// used to aware receive events and handle it in 
/// different ways.
pub trait ReceiveHandler<W: Write>: Send + Sync {
    #[inline]
    fn begin(&self) {
        // ignored
    }

    fn update_progress(&self, _progress: &Progress) {}

    /// check if item already exists before do really 
    /// send it to receiver.
    fn exists(&self, item: &MetaInfo) -> bool;

    /// before receive an item from sender, open a writer
    /// to receive it, and used to write buf.
    fn open_writer(&self, item: &MetaInfo) -> Result<W>;

    /// after received item and written to `writer`, the 
    /// writer will be passed in.
    #[inline]
    fn complete(&self, _writer: &mut W) {}

    #[inline]
    fn end(&self) {
        // ignored
    }
}
