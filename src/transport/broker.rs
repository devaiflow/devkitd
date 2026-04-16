use crate::transport::sender::TcpCallbackSender;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;

pub struct Broker {
    listener: TcpListener,
}

impl Broker {
    pub async fn new(config: &str) -> Result<(Self, TcpCallbackSender), std::io::Error> {
        tracing::info!(bind_address = config, "Initializing broker");
        let listener = TcpListener::bind(config).await?;
        let sender = TcpCallbackSender::new();
        let broker = Self { listener };
        Ok((broker, sender))
    }

    pub async fn run(&self, handler_tx: tokio::sync::mpsc::Sender<crate::handler::Message>) {
        tracing::info!("Broker is running and waiting for connections");
        loop {
            match self.listener.accept().await {
                Ok((stream, socket)) => {
                    let tx_clonado = handler_tx.clone();
                    tokio::spawn(async move {
                        tracing::debug!(peer = %socket, "Accepted connection");
                        let reader = BufReader::new(stream);
                        let mut lines = reader.lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            if line.is_empty() {
                                continue;
                            }
                            match serde_json::from_str::<crate::handler::Message>(&line) {
                                Ok(msg) => {
                                    tracing::debug!(peer = %socket, command = %msg.command, "Received message");
                                    if let Err(e) = tx_clonado.send(msg).await {
                                        tracing::error!(error = %e, "Error forwarding message to handler");
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(peer = %socket, error = %e, "Error parsing message, continuing");
                                }
                            }
                        }
                        tracing::debug!(peer = %socket, "Connection closed");
                    });
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }
}
