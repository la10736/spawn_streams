use std::{
    collections::HashMap,
    convert::identity,
    sync::{Arc, Weak},
};

use futures::{
    channel::mpsc, future::join_all, lock::Mutex as FutMutex, never::Never, Future, SinkExt,
    Stream, StreamExt,
};

pub struct Spawnable<T> {
    channels: Channels<T>,
}

impl<T> Spawnable<T> {
    pub fn new() -> Self {
        Self {
            channels: Channels::new(),
        }
    }

    pub async fn spawn(&self) -> impl Stream<Item = Arc<T>> {
        self.channels.create().await
    }

    pub fn feed(&self, feed: impl Stream<Item = T>) -> impl Future<Output = ()> {
        let channels = self.channels.clone().weak();
        feed.for_each(move |t| {
            let channels = channels.clone();
            async move {
                channels.send_to_all(t).await.unwrap();
            }
        })
    }
}

struct Channels<T> {
    txs: Arc<FutMutex<Txs<T>>>,
}
impl<T> Channels<T> {
    fn new() -> Self {
        Self {
            txs: Arc::new(FutMutex::new(Txs::new())),
        }
    }

    async fn create(&self) -> impl Stream<Item = Arc<T>> {
        self.txs.lock().await.create()
    }
}

impl<T> Channels<T> {
    fn weak(self) -> WeakChannels<T> {
        self.into()
    }
}

impl<T> Clone for Channels<T> {
    fn clone(&self) -> Self {
        Self {
            txs: self.txs.clone(),
        }
    }
}

struct WeakChannels<T> {
    txs: Weak<FutMutex<Txs<T>>>,
}

impl<T> Clone for WeakChannels<T> {
    fn clone(&self) -> Self {
        Self {
            txs: self.txs.clone(),
        }
    }
}

impl<T> WeakChannels<T> {
    async fn send_to_all(&self, t: T) -> Result<(), Never> {
        if let Some(txs) = self.txs.upgrade() {
            let t = Arc::new(t);
            let entries = txs.lock().await.entries();
            let dead_indexs = join_all(entries.into_iter().map(|(id, tx)| {
                let tx = tx.clone();
                let t = t.clone();
                async move {
                    match tx.send(t.clone()).await {
                        Ok(_) => None,
                        Err(_) => Some(id),
                    }
                }
            }))
            .await
            .into_iter()
            .filter_map(identity);
            txs.lock().await.remove(dead_indexs);
        }
        Ok(())
    }
}

impl<T> From<Channels<T>> for WeakChannels<T> {
    fn from(value: Channels<T>) -> Self {
        Self {
            txs: Arc::downgrade(&value.txs),
        }
    }
}

struct Txs<T> {
    txs: HashMap<usize, Tx<T>>,
    last_index: usize,
}
impl<T> Txs<T> {
    fn new() -> Txs<T> {
        Self {
            txs: HashMap::new(),
            last_index: 0,
        }
    }

    fn create(&mut self) -> impl Stream<Item = Arc<T>> {
        let (tx, rx) = mpsc::channel(0);
        self.txs.insert(self.last_index, Tx::new(tx));
        self.last_index += 1;
        rx
    }

    fn entries(&self) -> Vec<(usize, Tx<T>)> {
        self.txs.iter().map(|(id, tx)| (*id, tx.clone())).collect()
    }

    fn remove(&mut self, ids: impl Iterator<Item = usize>) {
        for id in ids {
            self.txs.remove(&id);
        }
    }
}

struct Tx<T> {
    tx: Arc<FutMutex<mpsc::Sender<Arc<T>>>>,
}

impl<T> Clone for Tx<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T> Tx<T> {
    fn new(tx: mpsc::Sender<Arc<T>>) -> Tx<T> {
        Self {
            tx: Arc::new(FutMutex::new(tx)),
        }
    }

    async fn send(&self, t: Arc<T>) -> Result<(), ()> {
        self.tx.lock().await.send(t).await.map_err(|_| ())
    }
}

#[cfg(test)]
mod should {

    use futures::{stream, StreamExt};

    use super::*;

    #[async_std::test]
    async fn return_a_stream_of_references_of_the_original_one() {
        let data: Vec<i32> = vec![1, 2, 3];

        let spawnable = Spawnable::<i32>::new();

        let spawned = spawnable.spawn().await;

        let _engine = async_std::task::spawn(spawnable.feed(stream::iter(data.clone())));

        assert_eq!(
            data,
            spawned
                .map(|i| *i)
                .take(data.len())
                .collect::<Vec<_>>()
                .await
        );
    }

    #[async_std::test]
    async fn return_some_streams_of_references_of_the_original_one() {
        let data: Vec<i32> = vec![1, 2, 3];

        let spawnable = Spawnable::<i32>::new();

        let (spawned_1, spawned_2, spawned_3) = (
            spawnable.spawn().await,
            spawnable.spawn().await,
            spawnable.spawn().await,
        );

        let _engine = async_std::task::spawn(spawnable.feed(stream::iter(data.clone())));

        let (res1, res2, res3) = futures::join!(
            spawned_1.map(|i| *i).take(data.len()).collect::<Vec<_>>(),
            spawned_2.map(|i| *i).take(data.len()).collect::<Vec<_>>(),
            spawned_3.map(|i| *i).take(data.len()).collect::<Vec<_>>(),
        );

        assert_eq!(data, res1);
        assert_eq!(data, res2);
        assert_eq!(data, res3);
    }

    #[async_std::test]
    async fn add_more_receivers() {
        let data: Vec<i32> = vec![1, 2, 3];

        let spawnable = Spawnable::<i32>::new();

        let mut spawned_1 = spawnable.spawn().await;

        let _engine = async_std::task::spawn(spawnable.feed(stream::iter(data.clone())));

        assert_eq!(1, *(spawned_1.next().await.unwrap()));

        let mut spawned_2 = spawnable.spawn().await;

        let (one, two) = (spawned_1.next().await, spawned_2.next().await);

        assert_eq!(2, *(one.unwrap()));
        assert_eq!(2, *(two.unwrap()));

        let mut spawned_3 = spawnable.spawn().await;

        let (one, two, three) = (
            spawned_1.next().await,
            spawned_2.next().await,
            spawned_3.next().await,
        );

        assert_eq!(3, *(one.unwrap()));
        assert_eq!(3, *(two.unwrap()));
        assert_eq!(3, *(three.unwrap()));
    }

    #[async_std::test]
    async fn close_receiver_stream_if_spawnable_owner_die() {
        let data: Vec<i32> = vec![1, 2, 3];

        let spawnable = Spawnable::<i32>::new();

        let spawned = spawnable.spawn().await;

        let _engine = async_std::task::spawn(spawnable.feed(stream::iter(data.clone())));

        async_std::task::spawn(async move {
            async_std::task::sleep(std::time::Duration::from_millis(150)).await;
            drop(spawnable);
        });
        assert_eq!(data, spawned.map(|i| *i).collect::<Vec<_>>().await);
    }

    #[async_std::test]
    async fn drop_original_stream_should_interrupt_receivers() {
        let data: Vec<i32> = vec![1, 2, 3];

        let spawnable = Spawnable::<i32>::new();

        let spawned = spawnable.spawn().await;

        let _engine = async_std::task::spawn(spawnable.feed(stream::iter(data.clone())));

        drop(spawnable);

        assert!(spawned.collect::<Vec<_>>().await.len() <= 1);
    }

    #[async_std::test]
    async fn should_use_backpressure_of_at_most_one_element() {
        let data: Vec<i32> = (1..10).collect();

        let spawnable = Spawnable::<i32>::new();

        let spawned = spawnable.spawn().await;

        let _engine = async_std::task::spawn(spawnable.feed(stream::iter(data.clone())));

        async_std::task::sleep(std::time::Duration::from_millis(150)).await;
        drop(spawnable);

        assert_eq!(spawned.map(|i| *i).collect::<Vec<_>>().await, vec![1]);
    }
}
