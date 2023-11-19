use std::sync::Arc;

use futures::{Future, Stream};

pub struct Spawnable<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Spawnable<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn spawn(&self) -> impl Stream<Item = Arc<T>> {
        futures::stream::empty()
    }

    pub fn feed(&self, _feed: impl Stream<Item = T>) -> impl Future<Output = ()> {
        async { () }
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
