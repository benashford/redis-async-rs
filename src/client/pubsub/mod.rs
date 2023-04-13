/*
 * Copyright 2017-2023 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

mod inner;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_channel::{mpsc, oneshot};
use futures_util::{
    future::TryFutureExt,
    stream::{Stream, StreamExt},
};

use super::{connect::connect_with_auth, ConnectionBuilder};

use crate::{
    error,
    reconnect::{reconnect, Reconnect},
    resp,
};

use self::inner::PubsubConnectionInner;

#[derive(Debug)]
pub(crate) enum PubsubEvent {
    /// The: topic, sink to send messages through, and a oneshot to signal subscription has
    /// occurred.
    Subscribe(String, PubsubSink, oneshot::Sender<()>),
    Psubscribe(String, PubsubSink, oneshot::Sender<()>),
    /// The name of the topic to unsubscribe from. Unsubscription will be signaled by the stream
    /// closing without error.
    Unsubscribe(String),
    Punsubscribe(String),
}

type PubsubStreamInner = mpsc::UnboundedReceiver<Result<resp::RespValue, error::Error>>;
type PubsubSink = mpsc::UnboundedSender<Result<resp::RespValue, error::Error>>;

/// A shareable reference to subscribe to PUBSUB topics
#[derive(Debug, Clone)]
pub struct PubsubConnection {
    out_tx_c: Arc<Reconnect<PubsubEvent, mpsc::UnboundedSender<PubsubEvent>>>,
}

async fn inner_conn_fn(
    // Needs to be a String for lifetime reasons
    host: String,
    port: u16,
    username: Option<Arc<str>>,
    password: Option<Arc<str>>,
    tls: bool,
) -> Result<mpsc::UnboundedSender<PubsubEvent>, error::Error> {
    let username = username.as_deref();
    let password = password.as_deref();

    let connection = connect_with_auth(&host, port, username, password, tls).await?;
    let (out_tx, out_rx) = mpsc::unbounded();
    tokio::spawn(async {
        match PubsubConnectionInner::new(connection, out_rx).await {
            Ok(_) => (),
            Err(e) => log::error!("Pub/Sub error: {:?}", e),
        }
    });
    Ok(out_tx)
}

impl ConnectionBuilder {
    pub fn pubsub_connect(&self) -> impl Future<Output = Result<PubsubConnection, error::Error>> {
        let username = self.username.clone();
        let password = self.password.clone();

        #[cfg(feature = "tls")]
        let tls = self.tls;
        #[cfg(not(feature = "tls"))]
        let tls = false;

        let host = self.host.clone();
        let port = self.port;

        let reconnecting_f = reconnect(
            |con: &mpsc::UnboundedSender<PubsubEvent>, act| {
                con.unbounded_send(act).map_err(|e| e.into())
            },
            move || {
                let con_f =
                    inner_conn_fn(host.clone(), port, username.clone(), password.clone(), tls);
                Box::pin(con_f)
            },
        );
        reconnecting_f.map_ok(|con| PubsubConnection {
            out_tx_c: Arc::new(con),
        })
    }
}

/// Used for Redis's PUBSUB functionality.
///
/// Returns a future that resolves to a `PubsubConnection`. The future will only resolve once the
/// connection is established; after the intial establishment, if the connection drops for any
/// reason (e.g. Redis server being restarted), the connection will attempt re-connect, however
/// any subscriptions will need to be re-subscribed.
pub async fn pubsub_connect(
    host: impl Into<String>,
    port: u16,
) -> Result<PubsubConnection, error::Error> {
    ConnectionBuilder::new(host, port)?.pubsub_connect().await
}

impl PubsubConnection {
    /// Subscribes to a particular PUBSUB topic.
    ///
    /// Returns a future that resolves to a `Stream` that contains all the messages published on
    /// that particular topic.
    ///
    /// The resolved stream will end with `redis_async::error::Error::EndOfStream` if the
    /// underlying connection is lost for unexpected reasons. In this situation, clients should
    /// `subscribe` to re-subscribe; the underlying connect will automatically reconnect. However,
    /// clients should be aware that resubscriptions will only succeed if the underlying connection
    /// has re-established, so multiple calls to `subscribe` may be required.
    pub async fn subscribe(&self, topic: &str) -> Result<PubsubStream, error::Error> {
        let (tx, rx) = mpsc::unbounded();
        let (signal_t, signal_r) = oneshot::channel();
        self.out_tx_c
            .do_work(PubsubEvent::Subscribe(topic.to_owned(), tx, signal_t))?;

        match signal_r.await {
            Ok(_) => Ok(PubsubStream {
                topic: topic.to_owned(),
                underlying: rx,
                con: self.clone(),
                is_pattern: false,
            }),
            Err(_) => Err(error::internal("Subscription failed, try again later...")),
        }
    }

    pub async fn psubscribe(&self, topic: &str) -> Result<PubsubStream, error::Error> {
        let (tx, rx) = mpsc::unbounded();
        let (signal_t, signal_r) = oneshot::channel();
        self.out_tx_c
            .do_work(PubsubEvent::Psubscribe(topic.to_owned(), tx, signal_t))?;

        match signal_r.await {
            Ok(_) => Ok(PubsubStream {
                topic: topic.to_owned(),
                underlying: rx,
                con: self.clone(),
                is_pattern: true,
            }),
            Err(_) => Err(error::internal("Subscription failed, try again later...")),
        }
    }

    /// Tells the client to unsubscribe from a particular topic. This will return immediately, the
    /// actual unsubscription will be confirmed when the stream returned from `subscribe` ends.
    pub fn unsubscribe<T: Into<String>>(&self, topic: T) {
        // Ignoring any results, as any errors communicating with Redis would de-facto unsubscribe
        // anyway, and would be reported/logged elsewhere
        let _ = self
            .out_tx_c
            .do_work(PubsubEvent::Unsubscribe(topic.into()));
    }

    pub fn punsubscribe<T: Into<String>>(&self, topic: T) {
        // Ignoring any results, as any errors communicating with Redis would de-facto unsubscribe
        // anyway, and would be reported/logged elsewhere
        let _ = self
            .out_tx_c
            .do_work(PubsubEvent::Punsubscribe(topic.into()));
    }
}

#[derive(Debug)]
pub struct PubsubStream {
    topic: String,
    underlying: PubsubStreamInner,
    con: PubsubConnection,
    is_pattern: bool,
}

impl Stream for PubsubStream {
    type Item = Result<resp::RespValue, error::Error>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.get_mut().underlying.poll_next_unpin(cx)
    }
}

impl Drop for PubsubStream {
    fn drop(&mut self) {
        let topic: &str = self.topic.as_ref();
        if self.is_pattern {
            self.con.punsubscribe(topic);
        } else {
            self.con.unsubscribe(topic);
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem;

    use futures::{try_join, StreamExt, TryStreamExt};

    use crate::{client, resp};

    /* IMPORTANT: The tests run in parallel, so the topic names used must be exclusive to each test */
    static SUBSCRIBE_TEST_TOPIC: &str = "test-topic";
    static SUBSCRIBE_TEST_NON_TOPIC: &str = "test-not-topic";

    static UNSUBSCRIBE_TOPIC_1: &str = "test-topic-1";
    static UNSUBSCRIBE_TOPIC_2: &str = "test-topic-2";
    static UNSUBSCRIBE_TOPIC_3: &str = "test-topic-3";

    static RESUBSCRIBE_TOPIC: &str = "test-topic-resubscribe";

    static DROP_CONNECTION_TOPIC: &str = "test-topic-drop-connection";

    static PSUBSCRIBE_PATTERN: &str = "ptest.*";
    static PSUBSCRIBE_TOPIC_1: &str = "ptest.1";
    static PSUBSCRIBE_TOPIC_2: &str = "ptest.2";
    static PSUBSCRIBE_TOPIC_3: &str = "ptest.3";

    static UNSUBSCRIBE_TWICE_TOPIC_1: &str = "test-topic-1-twice";
    static UNSUBSCRIBE_TWICE_TOPIC_2: &str = "test-topic-2-twice";

    #[tokio::test]
    async fn subscribe_test() {
        let paired_c = client::paired_connect("127.0.0.1", 6379);
        let pubsub_c = super::pubsub_connect("127.0.0.1", 6379);
        let (paired, pubsub) = try_join!(paired_c, pubsub_c).expect("Cannot connect to Redis");

        let topic_messages = pubsub
            .subscribe(SUBSCRIBE_TEST_TOPIC)
            .await
            .expect("Cannot subscribe to topic");

        paired.send_and_forget(resp_array!["PUBLISH", SUBSCRIBE_TEST_TOPIC, "test-message"]);
        paired.send_and_forget(resp_array![
            "PUBLISH",
            SUBSCRIBE_TEST_NON_TOPIC,
            "test-message-1.5"
        ]);
        let _: resp::RespValue = paired
            .send(resp_array![
                "PUBLISH",
                SUBSCRIBE_TEST_TOPIC,
                "test-message2"
            ])
            .await
            .expect("Cannot send to topic");

        let result: Vec<_> = topic_messages
            .take(2)
            .try_collect()
            .await
            .expect("Cannot collect two values");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "test-message".into());
        assert_eq!(result[1], "test-message2".into());
    }

    /// A test to examine the edge-case where a client subscribes to a topic, then the subscription is specifically unsubscribed,
    /// vs. where the subscription is automatically unsubscribed.
    #[tokio::test]
    async fn unsubscribe_test() {
        let paired_c = client::paired_connect("127.0.0.1", 6379);
        let pubsub_c = super::pubsub_connect("127.0.0.1", 6379);
        let (paired, pubsub) = try_join!(paired_c, pubsub_c).expect("Cannot connect to Redis");

        let mut topic_1 = pubsub
            .subscribe(UNSUBSCRIBE_TOPIC_1)
            .await
            .expect("Cannot subscribe to topic");
        let mut topic_2 = pubsub
            .subscribe(UNSUBSCRIBE_TOPIC_2)
            .await
            .expect("Cannot subscribe to topic");
        let mut topic_3 = pubsub
            .subscribe(UNSUBSCRIBE_TOPIC_3)
            .await
            .expect("Cannot subscribe to topic");

        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TOPIC_1,
            "test-message-1"
        ]);
        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TOPIC_2,
            "test-message-2"
        ]);
        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TOPIC_3,
            "test-message-3"
        ]);

        let result1 = topic_1
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result1, "test-message-1".into());

        let result2 = topic_2
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result2, "test-message-2".into());

        let result3 = topic_3
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result3, "test-message-3".into());

        // Unsubscribe from topic 2
        pubsub.unsubscribe(UNSUBSCRIBE_TOPIC_2);

        // Drop the subscription for topic 3
        mem::drop(topic_3);

        // Send some more messages
        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TOPIC_1,
            "test-message-1.5"
        ]);
        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TOPIC_2,
            "test-message-2.5"
        ]);
        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TOPIC_3,
            "test-message-3.5"
        ]);

        // Get the next message for topic 1
        let result1 = topic_1
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result1, "test-message-1.5".into());

        // Get the next message for topic 2
        let result2 = topic_2.next().await;
        assert!(result2.is_none());
    }

    /// Test that we can subscribe, unsubscribe, and resubscribe to a topic.
    #[tokio::test]
    async fn resubscribe_test() {
        let paired_c = client::paired_connect("127.0.0.1", 6379);
        let pubsub_c = super::pubsub_connect("127.0.0.1", 6379);
        let (paired, pubsub) = try_join!(paired_c, pubsub_c).expect("Cannot connect to Redis");

        let mut topic_1 = pubsub
            .subscribe(RESUBSCRIBE_TOPIC)
            .await
            .expect("Cannot subscribe to topic");

        paired.send_and_forget(resp_array!["PUBLISH", RESUBSCRIBE_TOPIC, "test-message-1"]);

        let result1 = topic_1
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result1, "test-message-1".into());

        // Unsubscribe from topic 1
        pubsub.unsubscribe(RESUBSCRIBE_TOPIC);

        // Send some more messages
        paired.send_and_forget(resp_array![
            "PUBLISH",
            RESUBSCRIBE_TOPIC,
            "test-message-1.5"
        ]);

        // Get the next message for topic 1
        let result1 = topic_1.next().await;
        assert!(result1.is_none());

        // Resubscribe to topic 1
        let mut topic_1 = pubsub
            .subscribe(RESUBSCRIBE_TOPIC)
            .await
            .expect("Cannot subscribe to topic");

        // Send some more messages
        paired.send_and_forget(resp_array![
            "PUBLISH",
            RESUBSCRIBE_TOPIC,
            "test-message-1.75"
        ]);

        // Get the next message for topic 1
        let result1 = topic_1
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result1, "test-message-1.75".into());
    }

    /// Test that dropping the connection doesn't stop the subscriptions. Not initially anyway.
    #[tokio::test]
    async fn drop_connection_test() {
        let paired_c = client::paired_connect("127.0.0.1", 6379);
        let pubsub_c = super::pubsub_connect("127.0.0.1", 6379);
        let (paired, pubsub) = try_join!(paired_c, pubsub_c).expect("Cannot connect to Redis");

        let mut topic_1 = pubsub
            .subscribe(DROP_CONNECTION_TOPIC)
            .await
            .expect("Cannot subscribe to topic");

        mem::drop(pubsub);

        paired.send_and_forget(resp_array![
            "PUBLISH",
            DROP_CONNECTION_TOPIC,
            "test-message-1"
        ]);

        let result1 = topic_1
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result1, "test-message-1".into());

        mem::drop(topic_1);
    }

    #[tokio::test]
    async fn psubscribe_test() {
        let paired_c = client::paired_connect("127.0.0.1", 6379);
        let pubsub_c = super::pubsub_connect("127.0.0.1", 6379);
        let (paired, pubsub) = try_join!(paired_c, pubsub_c).expect("Cannot connect to Redis");

        let topic_messages = pubsub
            .psubscribe(PSUBSCRIBE_PATTERN)
            .await
            .expect("Cannot subscribe to topic");

        paired.send_and_forget(resp_array!["PUBLISH", PSUBSCRIBE_TOPIC_1, "test-message-1"]);
        paired.send_and_forget(resp_array!["PUBLISH", PSUBSCRIBE_TOPIC_2, "test-message-2"]);
        let _: resp::RespValue = paired
            .send(resp_array!["PUBLISH", PSUBSCRIBE_TOPIC_3, "test-message-3"])
            .await
            .expect("Cannot send to topic");

        let result: Vec<_> = topic_messages
            .take(3)
            .try_collect()
            .await
            .expect("Cannot collect two values");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], "test-message-1".into());
        assert_eq!(result[1], "test-message-2".into());
        assert_eq!(result[2], "test-message-3".into());
    }

    /// Allow unsubscribe to be called twice
    #[tokio::test]
    async fn unsubscribe_twice_test() {
        let paired_c = client::paired_connect("127.0.0.1", 6379);
        let pubsub_c = super::pubsub_connect("127.0.0.1", 6379);
        let (paired, pubsub) = try_join!(paired_c, pubsub_c).expect("Cannot connect to Redis");

        let mut topic_1 = pubsub
            .subscribe(UNSUBSCRIBE_TWICE_TOPIC_1)
            .await
            .expect("Cannot subscribe to topic");
        let mut topic_2 = pubsub
            .subscribe(UNSUBSCRIBE_TWICE_TOPIC_2)
            .await
            .expect("Cannot subscribe to topic");

        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TWICE_TOPIC_1,
            "test-message-1"
        ]);
        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TWICE_TOPIC_2,
            "test-message-2"
        ]);

        pubsub.unsubscribe(UNSUBSCRIBE_TWICE_TOPIC_2);
        pubsub.unsubscribe(UNSUBSCRIBE_TWICE_TOPIC_2);

        paired.send_and_forget(resp_array![
            "PUBLISH",
            UNSUBSCRIBE_TWICE_TOPIC_1,
            "test-message-1.5"
        ]);

        pubsub.unsubscribe(UNSUBSCRIBE_TWICE_TOPIC_1);

        let result1 = topic_1
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result1, "test-message-1".into());

        let result1 = topic_1
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result1, "test-message-1.5".into());

        let result2 = topic_2
            .next()
            .await
            .expect("Cannot get next value")
            .expect("Cannot get next value");
        assert_eq!(result2, "test-message-2".into());

        let result1 = topic_1.next().await;
        assert!(result1.is_none());

        let result2 = topic_2.next().await;
        assert!(result2.is_none());
    }
}
