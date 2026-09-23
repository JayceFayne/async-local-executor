use crate::tls::{executor, try_executor};
use async_local_channel::oneshot;
use slotmap::new_key_type;
use slotmap::{Key, SlotMap};
use std::fmt::Debug;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Wake;
use std::task::Waker;
use std::task::{Context, Poll};

new_key_type! { pub struct TaskId; }

type WakerFn = Arc<dyn Fn(TaskHandle) + Send + Sync>;

struct WakerData {
    handle: TaskHandle,
    f: WakerFn,
}

impl Wake for WakerData {
    fn wake(self: Arc<Self>) {
        (self.f)(self.handle);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        (self.f)(self.handle);
    }
}

fn create_waker(handle: TaskHandle, f: WakerFn) -> Waker {
    Waker::from(Arc::new(WakerData { handle, f }))
}

type LocalFuture = Pin<Box<dyn Future<Output = ()>>>;

struct Task {
    future: LocalFuture,
    waker: Waker,
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TaskHandle {
    id: TaskId,
}

impl Debug for TaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.id.data().fmt(f)
    }
}

impl TaskHandle {
    #[inline]
    pub fn tick(self) {
        if let Some(ticker) = { executor().ticker(self) } {
            ticker.tick();
        }
    }
}

#[must_use]
pub struct Ticker {
    handle: TaskHandle,
    task: Task,
}

impl Ticker {
    #[inline]
    pub fn tick(mut self) {
        let mut context = Context::from_waker(&self.task.waker);
        if pin!(&mut self.task.future).poll(&mut context).is_ready() {
            executor().task_completed(self.handle);
        } else {
            executor().return_poller(self);
        }
    }
}

#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
pub struct JoinHandle<T> {
    handle: TaskHandle,
    result: oneshot::Receiver<T>,
    detached: bool,
}

impl<T> Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinHandle")
            .field("task", &self.handle)
            .finish()
    }
}

impl<T> JoinHandle<T> {
    const fn new(handle: TaskHandle, result: oneshot::Receiver<T>) -> Self {
        Self {
            handle,
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
            executor.task_completed(self.handle);
        }
    }
}

pub struct Executor {
    tasks: SlotMap<TaskId, Option<Task>>,
    waker_fn: WakerFn,
}

impl Debug for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Executor").field(&self.tasks.len()).finish()
    }
}

impl Executor {
    #[inline]
    pub fn new<F: Fn(TaskHandle) + Send + Sync + 'static>(f: F) -> Self {
        Self {
            tasks: SlotMap::with_key(),
            waker_fn: Arc::new(f),
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
        let waker_fn = self.waker_fn.clone();
        let future = Box::pin(future);
        let id = self.tasks.insert_with_key(|id| {
            let waker = create_waker(TaskHandle { id }, waker_fn);
            Some(Task { future, waker })
        });
        let handle = TaskHandle { id };
        (self.waker_fn)(handle);
        JoinHandle::new(handle, rx.activate())
    }

    fn ticker(&mut self, handle: TaskHandle) -> Option<Ticker> {
        let task = self.tasks.get_mut(handle.id)?.take()?;
        Some(Ticker { handle, task })
    }

    fn task_completed(&mut self, handle: TaskHandle) {
        self.tasks.remove(handle.id);
    }

    fn return_poller(&mut self, ticker: Ticker) {
        self.tasks[ticker.handle.id] = Some(ticker.task);
    }
}
