use std::fmt;
use tokio::sync::{mpsc, oneshot};

/// A message wrapped with an acknowledgment channel
pub struct WithAck<T> {
    pub data: T,
    ack: Option<oneshot::Sender<()>>,
}

impl<T> WithAck<T> {
    /// Manually acknowledge the message
    pub fn ack(mut self) {
        if let Some(ack) = self.ack.take() {
            let _ = ack.send(());
        }
    }
}

impl<T> Drop for WithAck<T> {
    fn drop(&mut self) {
        if let Some(ack) = self.ack.take() {
            // Ack if the message is dropped to avoid
            // blocking the sender
            let _ = ack.send(());
        }
    }
}

/// An acknowledged sender
pub struct Sender<T> {
    inner: mpsc::Sender<WithAck<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Sender<T> {
    pub fn new(inner: mpsc::Sender<WithAck<T>>) -> Self {
        Sender { inner }
    }

    /// Sends a message and waits for acknowledgment
    pub async fn send(&self, data: T) -> Result<(), SendError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .send(WithAck {
                data,
                ack: Some(ack_tx),
            })
            .await
            .map_err(|_| SendError)?;
        ack_rx.await.map_err(|_| SendError)?;
        Ok(())
    }
}

/// Possible errors when sending
#[derive(Debug)]
pub struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "send error")
    }
}

impl std::error::Error for SendError {}

/// Create a new acknowledged channel
pub fn channel<T>(capacity: usize) -> (Sender<T>, mpsc::Receiver<WithAck<T>>) {
    let (tx, rx) = mpsc::channel(capacity);
    (Sender::new(tx), rx)
}
