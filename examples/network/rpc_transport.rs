

mod rpc_case {
    use std::time::Duration;
    use torrent::tokio1::runtime::Runtime;
    use torrent::tokio1::time::Instant;

    use tarpc_ext::{
        new_client, new_client_timeout,
        tcp::{
            rpc::tarpc::{self, context::Context},
            rpc_server::{Server, ServerConfig},
        },
    };
    use torrent::tokio1;

    #[tarpc::service]
    pub trait Foo {
        async fn say_hello(name: String) -> String;
        async fn not_resp();
    }

    #[derive(Clone)]
    pub struct Bar;

    #[tarpc::server]
    impl Foo for Bar {
        async fn say_hello(self, _ctx: Context, name: String) -> String {
            format!("Hello {}", name)
        }

        async fn not_resp(self, _ctx: Context) {}
    }

    pub fn run() {
        Runtime::new().unwrap().block_on(async move {
            let mut server = Server::new(ServerConfig {
                bind_address: "127.0.0.1:8080".parse().unwrap(),
                max_channels_per_ip: 2,
                ..Default::default()
            });
            let proc = server.start(Bar.serve());
            let server_proc = tokio1::task::spawn(async move {
                let _ = proc.await;
            });

            tokio1::time::sleep(Duration::from_millis(1000)).await;

            let client = new_client_timeout!(
                "127.0.0.1:8080".parse().unwrap(),
                FooClient,
                Duration::from_millis(100)
            )
            .unwrap();
            let client2 = new_client!("127.0.0.1:8080".parse().unwrap(), FooClient).unwrap();
            let client3 = new_client!("127.0.0.1:8081".parse().unwrap(), FooClient);
            if let Err(e) = client3 {
                println!("{:?}", e);
            }

            let mut concurrency = vec![];
            let now = Instant::now();
            for _i in 0..5000 {
                let cli = client.clone();
                let cli2 = client2.clone();
                let t = tokio1::spawn(async move {
                    // let _ = cli.say_hello(tarpc::context::current(), "Xun".to_owned()).await;
                    let _ = cli.not_resp(tarpc::context::current()).await;
                });
                let t2 = tokio1::spawn(async move {
                    // let _ = cli2.say_hello(tarpc::context::current(), "Xun".to_owned()).await;
                    let _ = cli2.not_resp(tarpc::context::current()).await;
                });
                concurrency.push(t);
                concurrency.push(t2);
            }
            for t in concurrency {
                let _ = t.await;
            }
            println!("total use: {:?}", now.elapsed());
            let client1 = client.clone();
            let client2 = client.clone();

            tokio1::task::spawn(async move {
                let greeting = client1
                    .say_hello(tarpc::context::current(), "Alice".to_owned())
                    .await;
                println!("{:?}", greeting);
            });

            tokio1::task::spawn(async move {
                let greeting = client2
                    .say_hello(tarpc::context::current(), "Bob".to_owned())
                    .await;
                println!("{:?}", greeting);
                server.shutdown();
            });

            let _ = server_proc.await;
        });
    }
}

fn main() {
    rpc_case::run();
}
