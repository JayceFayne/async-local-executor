use crate::tls::{executor, try_executor};
use async_local_channel::oneshot;
use crossbeam_queue::SegQueue;
use slotmap::new_key_type;
use slotmap::{Key, SlotMap};
use std::collections::VecDeque;
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
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(mut executor) = try_executor() {
            return executor.schedule_task(self.task_id);
        }
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

struct TaskState {
    future: LocalFuture,
    waker: Waker,
}

struct Task {
    state: Option<TaskState>,
    queued: bool,
}

#[must_use]
pub struct Ticker {
    task_id: TaskId,
    task: TaskState,
}

impl Ticker {
    #[inline]
    pub fn tick(mut self) {
        let mut cx = Context::from_waker(&self.task.waker);
        if self.task.future.as_mut().poll(&mut cx).is_ready() {
            executor().task_completed(self.task_id);
        } else {
            executor().return_ticker(self);
        }
    }
}

pub struct JoinHandle<T> {
    task_id: TaskId,
    result: oneshot::Receiver<T>,
    cancel: bool,
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
            cancel: false,
        }
    }

    #[inline]
    pub fn cancel(mut self) {
        self.cancel = true;
    }

    #[inline]
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.result.is_closed()
    }

    #[inline]
    #[must_use]
    pub fn result(self) -> Option<T> {
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
        if self.cancel
            && let Some(mut executor) = try_executor()
        {
            executor.task_completed(self.task_id);
        }
    }
}

pub struct Executor {
    tasks: SlotMap<TaskId, Task>,
    wake_fn: WakeFn,
    local_queue: VecDeque<TaskId>,
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
        let tasks = SlotMap::with_key();
        let wake_fn = Arc::new(f);
        let queue = Arc::new(SegQueue::new());
        let local_queue = VecDeque::new();
        Self {
            tasks,
            wake_fn,
            local_queue,
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
        let task_id = self.tasks.insert_with_key(|id| {
            let waker = create_waker(id, self.queue.clone(), self.wake_fn.clone());
            Task {
                state: Some(TaskState { future, waker }),
                queued: true,
            }
        });
        self.local_queue.push_back(task_id);
        JoinHandle::new(task_id, rx)
    }

    pub(crate) fn exit(&mut self) {
        self.tasks.clear();
    }

    fn schedule_task(&mut self, task_id: TaskId) {
        if let Some(task) = self.tasks.get_mut(task_id)
            && !task.queued
        {
            task.queued = true;
            self.local_queue.push_back(task_id);
        }
    }

    fn next_task_id(&mut self) -> Option<TaskId> {
        if let Some(task_id) = self.local_queue.pop_front() {
            return Some(task_id);
        }
        while let Some(task_id) = self.queue.pop() {
            self.schedule_task(task_id);
        }
        self.local_queue.pop_front()
    }

    pub(crate) fn ticker(&mut self) -> Option<Ticker> {
        let task_id = self.next_task_id()?;
        let task = self.tasks.get_mut(task_id)?;
        task.queued = false;
        let task = task.state.take()?;
        Some(Ticker { task_id, task })
    }

    fn task_completed(&mut self, task_id: TaskId) {
        self.tasks.remove(task_id);
    }

    fn return_ticker(&mut self, ticker: Ticker) {
        let Some(task) = self.tasks.get_mut(ticker.task_id) else {
            return;
        };
        task.state = Some(ticker.task);
    }
}
