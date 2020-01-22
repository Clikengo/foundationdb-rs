use foundationdb::*;
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};

mod common;

struct AbortingFuture<F> {
    inner: F,
    polled: bool,
}

impl<T> Future for AbortingFuture<T>
where
    T: Future + Unpin,
{
    type Output = FdbResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<FdbResult<()>> {
        // poll once only
        if !self.polled {
            self.polled = true;
            match Pin::new(&mut self.as_mut().inner).poll(cx) {
                Poll::Pending => (),
                _ => panic!("pending was expected"),
            }
        }

        Poll::Ready(Ok(()))
    }
}

#[test]
// dropping a future while it's in the pending state should not crash
fn test_future_discard() {
    common::boot();
    futures::executor::block_on(test_future_discard_async()).expect("failed to run");
}
async fn test_future_discard_async() -> FdbResult<()> {
    let db = common::database().await?;
    for _i in 0..=1000 {
        db.transact_boxed_local(
            (),
            |trx, ()| {
                AbortingFuture {
                    inner: trx.get(b"key", false),
                    polled: false,
                }
                .boxed_local()
            },
            TransactOption::default(),
        )
        .await?;
    }

    Ok(())
}
