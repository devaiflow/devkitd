// use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::transport::sender::TcpCallbackSender;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener; // Tu struct de salida

pub struct Broker {
    listener: TcpListener,
}

impl Broker {
    pub async fn new(config: &str) -> Result<(Self, TcpCallbackSender), std::io::Error> {
        println!("Initializing broker with config: {}", config);
        let listener = TcpListener::bind(config).await?;
        let sender = TcpCallbackSender::new();
        let broker = Self { listener };
        Ok((broker, sender))
    }

    pub async fn run(&self, handler_tx: tokio::sync::mpsc::Sender<crate::handler::Message>) {
        println!("Broker is running and waiting for connections");
        loop {
            match self.listener.accept().await {
                Ok((mut stream, socket)) => {
                    let tx_clonado = handler_tx.clone();
                    tokio::spawn(async move {
                        println!("Accepted connection from: {}", socket);
                        let mut buf = [0; 1024];
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) => {
                                    println!("Connection closed by client: {}", socket);
                                    break;
                                }
                                Ok(n) => {
                                    let msg = String::from_utf8_lossy(&buf[..n]);
                                    match serde_json::from_str::<crate::handler::Message>(&msg) {
                                        Ok(msg) => {
                                            if let Err(e) = tx_clonado.send(msg).await {
                                                println!("Error handling message: {}", e);
                                            };
                                        }
                                        Err(e) => {
                                            println!(
                                                "Error parsing message from {}: {}",
                                                socket, e
                                            );
                                            break;
                                        }
                                    }
                                    println!("Received message from {}: {}", socket, msg);
                                }
                                Err(e) => {
                                    println!("Error reading from {}: {}", socket, e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    println!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}
