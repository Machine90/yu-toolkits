pub mod tcp;
pub use torrent::runtime;

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    use torrent::{
        network::connections::connection::Connection,
        tokio1 as tokio,
        topology::{node::Node, topo::Topology},
    };

    use tarpc::{
        context::{self, Context},
        server, service,
    };
    use tokio::{
        runtime::{Builder, Runtime},
        time::sleep,
    };
    use torrent::{network::Network, runtime};

    use crate::{
        new_client_config,
        tcp::{
            rpc_client::ClientConfig,
            rpc_server::{Server, ServerConfig},
        },
    };

    #[service]
    pub trait Foo {
        async fn say_hello(who: String) -> String;
    }

    #[service]
    pub trait Foo2 {
        async fn say_hello2(who: String) -> String;
    }

    #[derive(Clone)]
    pub struct Bar;

    #[server]
    impl Foo for Bar {
        async fn say_hello(self, _ctx: Context, who: String) -> String {
            format!("Hello {}", who)
        }
    }

    #[test]
    fn concurrency() {
        let topo = Topology::<u64, u64>::new();
        let n1 = Node::parse(1, "rpc://127.0.0.1:50011").unwrap();
        let n2 = Node::parse(2, "rpc://127.0.0.1:50012").unwrap();
        let n3 = Node::parse(3, "rpc://127.0.0.1:50013").unwrap();
        let n4 = Node::parse(4, "rpc://127.0.0.1:50014").unwrap();

        topo.get_or_add_group(1)
            .add(n1.clone())
            .add(n2.clone())
            .add(n3.clone())
            .add(n4.clone());

        let cluster = Network::restore(topo);
        cluster.register_connector_all("rpc", false, false, move |addr| {
            new_client_config!(
                ClientConfig {
                    target_address: addr,
                    handshake_timeout: Duration::from_millis(10),
                    ..Default::default()
                },
                FooClient
            )
        });
        println!("start to test");
        println!("connections: {:#?}", cluster.get_conns());

        let do_request = |topo: Arc<Network<u64, u64>>, concurrency| {
            for i in 0..concurrency {
                let cluster = topo.clone();

                runtime::spawn(async move {
                    let to = (i % 3) + 1;
                    let cli = cluster.maybe_get_client::<FooClient>(&1, &to, "rpc");
                    if cli.is_none() {
                        return;
                    }
                    let cli = cli.unwrap();
                    let resp = cli
                        .say_hello(context::current(), format!("zhangsan-{}", i))
                        .await;
                    println!("[from-{:?}] resp {:?}", to, resp);
                });
            }
        };

        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("thread-pool")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async move {
            let c = Arc::new(cluster);
            println!("-------->>> Round 1 <<<-----------");
            do_request(c.clone(), 5);
            tokio::time::sleep(Duration::from_secs(1)).await;

            println!("------>>> Re-connect <<<------");
            for i in 0..10 {
                let clus = c.clone();
                runtime::spawn(async move {
                    let to = (i % 3) + 1;
                    let cli = clus
                        .reconnect_and_get::<FooClient>(&1, &to, "rpc", 10000)
                        .await;
                    if let Err(e) = cli {
                        println!("{:?}", e);
                    } else {
                        let cli = cli.unwrap();
                        let resp = cli
                            .say_hello(context::current(), format!("zhangsan-{}", i))
                            .await;
                        println!("[from-{:?}] resp {:?}", to, resp);
                    }
                });
            }
            tokio::time::sleep(Duration::from_secs(10)).await;

            println!("-------->>> Round 2 <<<-----------");
            do_request(c.clone(), 5);
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
    }

    #[test]
    fn serve_all() {
        for i in 1..=3 {
            std::thread::spawn(move || {
                if i == 3 {
                    std::thread::sleep(Duration::from_secs(10));
                }
                serve_at(format!("127.0.0.1:5001{:?}", i).as_str());
            });
        }
        loop {}
    }

    #[test]
    pub fn server1() {
        println!("start server 1");
        serve_at("127.0.0.1:50010");
    }

    #[test]
    pub fn server2() {
        println!("start server 2");
        serve_at("127.0.0.1:50011");
    }

    #[test]
    pub fn server3() {
        std::thread::sleep(Duration::from_secs(15));
        println!("start server 3");
        serve_at("127.0.0.1:50012");
    }

    pub fn serve_at(addr: &str) {
        let mut server = Server::new(ServerConfig {
            bind_address: addr.parse().unwrap(),
            max_frame_buffer_size: usize::MAX,
            max_channels_per_ip: 64,
            ..Default::default()
        });

        Runtime::new().unwrap().block_on(async move {
            let proc = server.start_with(Bar.serve(), move |addr| {
                println!("---->>>> server startup at {:?} <<<<----", addr);
            });

            tokio::task::spawn(async move {
                sleep(Duration::from_secs(45)).await;
                server.shutdown();
            });
            let _r = proc.await;
        });
    }

    #[test]
    pub fn server8080() {
        println!("serve at 8080");
        serve_at("127.0.0.1:8080");
    }

    #[test]
    fn test_clone() {
        Runtime::new().unwrap().block_on(async move {
            let conn = Connection::with_connector(
                "127.0.0.1:8080".parse().unwrap(),
                |addr| {
                    // simulate a connect delay for 1 sec
                    std::thread::sleep(Duration::from_millis(1000));
                    new_client_config!(
                        ClientConfig {
                            target_address: addr,
                            handshake_timeout: Duration::from_millis(10),
                            ..Default::default()
                        },
                        FooClient
                    )
                },
                true,
            );

            let concu = Arc::new(conn);
            let mut ts = vec![];
            let time = Instant::now();
            for _ in 0..10 {
                let connect = concu.clone();
                let t = runtime::spawn(async move {
                    let _conn = connect.as_ref().clone();
                });
                ts.push(t);
            }
            for t in ts {
                let _ = t.await;
            }
            println!("{:?}ms", time.elapsed().as_millis());
        });
    }
}
