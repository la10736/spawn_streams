use std::{
    collections::HashMap,
    convert::identity,
    pin::Pin,
    sync::{Arc, Weak},
};

use futures::{
    channel::mpsc::{Receiver as ReceiverF, Sender as SenderF},
    lock::Mutex,
    never::Never,
    SinkExt, Stream, StreamExt,
};

struct Spawnable<T> {
    channels: Senders<T>,
}

impl<T: 'static + Send + Sync + std::fmt::Debug> Spawnable<T> {
    pub fn new() -> Self {
        Self {
            channels: Senders::new(),
        }
    }

    fn engine(&self) -> Engine<T> {
        Engine::<T>::from(self.channels.clone())
    }

    async fn spawn(&mut self) -> impl Stream<Item = Arc<T>> {
        self.channels.create().await
    }
}

type Sender<T> = Arc<Mutex<SenderF<Arc<T>>>>;

struct Txs<T> {
    txs: HashMap<usize, Sender<T>>,
    last_id: usize,
}
impl<T> Default for Txs<T> {
    fn default() -> Self {
        Self {
            txs: HashMap::new(),
            last_id: Default::default(),
        }
    }
}

impl<T> Txs<T> {
    fn new() -> Self {
        Default::default()
    }

    fn create(&mut self) -> ReceiverF<Arc<T>> {
        let (tx, rx) = futures::channel::mpsc::channel(0);
        self.last_id += 1;
        self.txs.insert(self.last_id, Arc::new(Mutex::new(tx)));
        rx
    }

    fn entries(&self) -> Vec<(usize, Sender<T>)> {
        self.txs.iter().map(|(id, tx)| (*id, tx.clone())).collect()
    }

    fn removes(&mut self, idxs: impl Iterator<Item = usize>) {
        for id in idxs {
            self.txs.remove(&id);
        }
    }
}

struct Senders<T>(Arc<Mutex<Txs<T>>>);

impl<T> Clone for Senders<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Senders<T> {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Txs::new())))
    }

    async fn create(&self) -> ReceiverF<Arc<T>> {
        self.0.lock().await.create()
    }

    fn weak(&self) -> Weak<Mutex<Txs<T>>> {
        Arc::<Mutex<Txs<T>>>::downgrade(&self.0)
    }
}
struct Engine<T>(Pin<Box<dyn futures::Sink<T, Error = Never> + Send>>);

impl<T: 'static + Send + Sync + std::fmt::Debug> Engine<T> {
    async fn process(channels: Weak<Mutex<Txs<T>>>, t: T) -> Result<(), Never> {
        if let Some(channels) = channels.upgrade() {
            let entries = channels.lock().await.entries();
            let t = Arc::new(t);
            let dead_indexes = futures::future::join_all(entries.into_iter().map(|(id, tx)| {
                let t = t.clone();
                async move {
                    if let Ok(_) = tx.lock().await.send(t).await {
                        None
                    } else {
                        Some(id)
                    }
                }
            }))
            .await
            .into_iter()
            .filter_map(identity);
            channels.lock().await.removes(dead_indexes);
        }
        Ok(())
    }

    async fn run(mut self, feeder: impl Stream<Item = T> + Unpin) {
        let mut stream = feeder.map(Ok);
        self.0.send_all(&mut stream).await.unwrap();
    }
}

impl<T: 'static + Send + Sync + std::fmt::Debug> From<Senders<T>> for Engine<T> {
    fn from(channels: Senders<T>) -> Self {
        let c = channels.weak();
        Engine(Box::pin(futures::sink::drain().with(move |t| {
            let c = c.clone();
            async move { Self::process(c, t).await }
        })))
    }
}

#[cfg(test)]
mod should {

    use futures::stream;

    use super::*;

    #[async_std::test]
    async fn return_a_stream_of_references_of_the_original_one() {
        let data: Vec<i32> = vec![1, 2, 3];

        let mut spawnable = Spawnable::<i32>::new();

        let _engine = async_std::task::spawn(spawnable.engine().run(stream::iter(data.clone())));

        let spawned = spawnable.spawn().await;

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

        let mut spawnable = Spawnable::<i32>::new();

        let _engine = async_std::task::spawn(spawnable.engine().run(stream::iter(data.clone())));

        let (spawned_1, spawned_2, spawned_3) = (
            spawnable.spawn().await,
            spawnable.spawn().await,
            spawnable.spawn().await,
        );

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

        let mut spawnable = Spawnable::<i32>::new();

        let _engine = async_std::task::spawn(spawnable.engine().run(stream::iter(data.clone())));

        let mut spawned_1 = spawnable.spawn().await;

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

        let mut spawnable = Spawnable::<i32>::new();

        let _engine = async_std::task::spawn(spawnable.engine().run(stream::iter(data.clone())));

        let spawned = spawnable.spawn().await;

        async_std::task::spawn(async move {
            async_std::task::sleep(std::time::Duration::from_millis(150)).await;
            drop(spawnable);
        });
        assert_eq!(data, spawned.map(|i| *i).collect::<Vec<_>>().await);
    }

    #[async_std::test]
    async fn drop_original_stream_should_interrupt_receivers() {
        let data: Vec<i32> = vec![1, 2, 3];

        let mut spawnable = Spawnable::<i32>::new();

        let _engine = async_std::task::spawn(spawnable.engine().run(stream::iter(data.clone())));

        let spawned = spawnable.spawn().await;

        drop(spawnable);

        assert!(spawned.collect::<Vec<_>>().await.len() <= 1);
    }

    #[async_std::test]
    async fn should_use_backpressure_of_one_element() {
        let data: Vec<i32> = (1..10000).collect();

        let mut spawnable = Spawnable::<i32>::new();

        let _engine = async_std::task::spawn(spawnable.engine().run(stream::iter(data.clone())));

        let spawned = spawnable.spawn().await;

        async_std::task::sleep(std::time::Duration::from_millis(150)).await;
        drop(spawnable);

        assert_eq!(spawned.map(|i| *i).collect::<Vec<_>>().await, vec![1]);
    }
}
