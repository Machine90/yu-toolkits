//! Torrent is a network topology and cluster management
//! toolkit, it provide some tools for managing your cluster,
//! for example [Cluster](crate::network::cluster), high-level
//! codes can use this module to register and get the `connection`
//! easy, and also reconnect to specific peer in the cluster.
//! The connection here could be any thing, e.g. http client, 
//! tcp stream, etc..
//! ### Features
//! **tokio1**: enable this feature will explore some async methods 
//! (e.g. `reconnect_and_get`) and some tools like [runtime](crate::runtime).
//! 

pub mod partitions;
pub mod topology;
pub use bincode::{*, self};
pub mod network;
pub mod dams;

#[cfg(feature = "fastcp")] pub mod fast_cp;
#[cfg(feature = "tokio1-rt")] pub use futures::{self, *};

pub mod tokio1 {
    #[cfg(feature = "tokio1-rt")]
    pub use tokio::{self, *};
    #[cfg(feature = "tokio1-net")]
    pub use tokio::net::{self, *};
    #[cfg(feature = "tokio1-io")]
    pub use tokio::{fs::{self, *}, io::{self, *}};
}

#[cfg(feature = "tokio1-rt")]
pub mod runtime {
    use futures::Future;
    use tokio::{
        runtime::{Handle, Runtime, Builder},
        task::{block_in_place, JoinHandle},
    };
    use vendor::{prelude::Singleton};

    // Delegation Runtime singleton
    pub static RUNTIME: Singleton<Runtime> = Singleton::INIT;

    /// blocking async task in **multi-threads** runtime, this will
    /// try using [blocking_in_place](tokio::task::block_in_place) of `current` runtime if (exists)
    /// or using singleton [RUNTIME](crate::network::runtime::RUNTIME) instead.
    /// #### panic
    /// panic if blocking task in single-thread runtime.
    /// e.g. blocking in [**#\[tokio::test\]**](tokio::test)
    ///
    /// using [**#\[tokio::test(flavor = "multi_thread")\]**](tokio::test) instead
    pub fn blocking<F>(task: F) -> F::Output
    where
        F: Future,
    {
        let h = Handle::try_current();
        if let Ok(h) = h {
            // give current's task to another thread and block on current
            block_in_place(move || h.block_on(task))
        } else {
            // what about RUNTIME constructed failed?
            let blocker = RUNTIME.get(build_runtime);
            let handler = blocker.handle();
            block_in_place(|| handler.block_on(task))
        }
    }

    pub fn spawn<F>(task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let h = Handle::try_current();
        if let Ok(h) = h {
            h.spawn(task)
        } else {
            let runtime = RUNTIME.get(build_runtime);
            runtime.spawn(task)
        }
    }

    pub fn spawn_blocking<F, R>(task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let h = Handle::try_current();
        if let Ok(h) = h {
            h.spawn_blocking(task)
        } else {
            let runtime = RUNTIME.get(build_runtime);
            runtime.spawn_blocking(task)
        }
    }

    fn build_runtime() -> Runtime {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("default-thread-pool")
            .build();
        if let Err(e) = runtime {
            panic!("failed to init default Runtime, because {:?}", e);
        }
        runtime.unwrap()
    }
}

#[cfg(test)]
#[cfg(feature = "tokio1-rt")]
mod tests {
    use std::time::Instant;

    use tokio::runtime::Runtime;
    use crate::runtime;

    #[test]
    fn default_blocking() {

        let mut cnt = 0;
        let time = Instant::now();
        for _ in 0..100000 {
            runtime::blocking(async {
                cnt += 1;
            });
        }
        println!("cnt => {cnt} elapsed: {:?}ms", time.elapsed().as_millis());
    }

    #[test]
    fn current_blocking() {
        Runtime::new().unwrap().block_on(async {
            let mut cnt = 0;
            let time = Instant::now();
            for _ in 0..100000 {
                runtime::blocking(async {
                    cnt += 1;
                });
            }
            println!("cnt => {cnt} elapsed: {:?}ms", time.elapsed().as_millis());

            let h = runtime::spawn(async move {
                println!("spawn task");
            });
            let _ = h.await;
        });
    }
}