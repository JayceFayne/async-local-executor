use crate::tls::{executor, try_executor};
use async_local_channel::oneshot;
use crossbeam_queue::SegQueue;
use slotmap::new_key_type;
use slotmap::{Key, SlotMap};
use std::fmt::Debug;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Wake;
use std::task::Waker;
use std::task::{Context, Poll};

new_key_type! { pub struct TaskId; }

type WakeFn = Arc<dyn Fn() + Send + Sync>;

struct WakerData {
    task_id: TaskId,
    queue: Arc<SegQueue<TaskId>>,
    wake_fn: WakeFn,
}

impl Wake for WakerData {
    fn wake(self: Arc<Self>) {
        self.queue.push(self.task_id);
        (self.wake_fn)();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.queue.push(self.task_id);
        (self.wake_fn)();
    }
}

fn create_waker(
    task_id: TaskId,
    tx: Arc<SegQueue<TaskId>>,
    wake_fn: Arc<dyn Fn() + Send + Sync>,
) -> Waker {
    Waker::from(Arc::new(WakerData {
        task_id,
        queue: tx,
        wake_fn,
    }))
}

type LocalFuture = Pin<Box<dyn Future<Output = ()>>>;

struct Task {
    future: LocalFuture,
    waker: Waker,
}

#[must_use]
pub struct Ticker {
    task_id: TaskId,
    task: Task,
}

impl Ticker {
    #[inline]
    pub fn tick(mut self) {
        let mut context = Context::from_waker(&self.task.waker);
        if pin!(&mut self.task.future).poll(&mut context).is_ready() {
            executor().task_completed(self.task_id);
        } else {
            executor().return_poller(self);
        }
    }
}

#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
pub struct JoinHandle<T> {
    task_id: TaskId,
    result: oneshot::Receiver<T>,
    detached: bool,
}

impl<T> Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("JoinHandle")
            .field(&self.task_id.data())
            .finish()
    }
}

impl<T> JoinHandle<T> {
    const fn new(task_id: TaskId, result: oneshot::Receiver<T>) -> Self {
        Self {
            task_id,
            result,
            detached: false,
        }
    }

    #[inline]
    pub fn detach(mut self) {
        self.detached = true;
    }

    #[inline]
    #[must_use]
    pub fn result(&self) -> Option<T> {
        self.result.try_recv()
    }
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match pin!(self.result.recv()).poll(cx) {
            Poll::Ready(value) => Poll::Ready(value.unwrap()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    #[inline]
    fn drop(&mut self) {
        if !self.detached
            && let Some(mut executor) = try_executor()
        {
            executor.task_completed(self.task_id);
        }
    }
}

pub struct Executor {
    tasks: SlotMap<TaskId, Option<Task>>,
    wake_fn: WakeFn,
    queue: Arc<SegQueue<TaskId>>,
}

impl Debug for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Executor").field(&self.tasks.len()).finish()
    }
}

impl Executor {
    #[inline]
    pub fn new<F: Fn() + Send + Sync + 'static>(f: F) -> Self {
        let queue = Arc::new(SegQueue::new());
        let tasks = SlotMap::with_key();
        let wake_fn = Arc::new(f);
        Self {
            tasks,
            wake_fn,
            queue,
        }
    }

    pub(crate) fn spawn_local<F>(&mut self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let (tx, rx) = oneshot::channel();
        let future = async {
            let res = future.await;
            let _ = tx.send(res);
        };
        let future = Box::pin(future);
        let wake_fn = self.wake_fn.clone();
        let task_id = self.tasks.insert_with_key(|id| {
            let waker = create_waker(id, self.queue.clone(), wake_fn);
            Some(Task { future, waker })
        });
        self.queue.push(task_id);
        (self.wake_fn)();
        JoinHandle::new(task_id, rx.activate())
    }

    pub(crate) fn ticker(&mut self) -> Option<Ticker> {
        let task_id = self.queue.pop()?;
        let task = self.tasks.get_mut(task_id)?.take()?;
        Some(Ticker { task_id, task })
    }

    fn task_completed(&mut self, task_id: TaskId) {
        self.tasks.remove(task_id);
    }

    fn return_poller(&mut self, ticker: Ticker) {
        self.tasks[ticker.task_id] = Some(ticker.task);
    }
}
