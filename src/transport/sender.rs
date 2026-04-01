use crate::handler::MessageSender;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct TcpCallbackSender;

impl TcpCallbackSender {
    pub fn new() -> Self {
        TcpCallbackSender {}
    }
}

#[async_trait]
impl MessageSender for TcpCallbackSender {
    async fn send(&self, channel: &str, payload: Vec<u8>) -> Result<(), std::io::Error> {
        if let Ok(mut stream) = TcpStream::connect(channel).await {
            stream.write_all(&payload).await?;
            stream.write_all(b"\n").await?;
        } else {
            println!("Warning: No se pudo conectar al orquestador en {}", channel);
        }
        Ok(())
    }
}
