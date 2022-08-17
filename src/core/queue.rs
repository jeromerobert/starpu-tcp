use std::{
    env,
    sync::atomic::AtomicUsize,
    sync::{atomic::Ordering, Arc, Condvar, Mutex},
    thread::{self, ThreadId},
};

use log::debug;

use super::multimap::PrioQueue;
pub type Priority = isize;

pub trait Task {
    /// Execute the task
    fn run(&self);
    /// The size of the data associated to this task.
    /// This is only used for logging and statistics
    fn size(&self) -> usize;
}

pub struct TaskQueue<T> {
    /// If tasks are not submitted from this thread execute them right now
    thread_id: Option<ThreadId>,
    /// The queue of tasks waiting for execution
    tasks: Arc<(Mutex<PrioQueue<Priority, T>>, Condvar)>,
    /// Maximum number of tasks waiting (for logging)
    max_tasks: Arc<AtomicUsize>,
    /// Maximum size of data waiting in the queue (for logging)
    max_size: Arc<AtomicUsize>,
    /// Maximum size of data waiting in the queue (for logging)
    cur_size: Arc<AtomicUsize>,
    /// If the queue is more than this size we process new urgent task synchronously
    sync_prio_size: usize,
    /// Task smaller than this will be processed in the push thread
    sync_small: usize,
    /// Force push to be always asynchrone
    always_async: bool,
}

impl<T> Clone for TaskQueue<T> {
    fn clone(&self) -> Self {
        Self {
            tasks: Arc::clone(&self.tasks),
            thread_id: self.thread_id,
            max_tasks: Arc::clone(&self.max_tasks),
            max_size: Arc::clone(&self.max_size),
            cur_size: Arc::clone(&self.cur_size),
            sync_prio_size: self.sync_prio_size,
            sync_small: self.sync_small,
            always_async: self.always_async,
        }
    }
}

impl<T: Task + Send + Sync + 'static> TaskQueue<T> {
    fn run_tasks(&self, threshold: usize, label: String) {
        loop {
            let mut l = self.tasks.0.lock().unwrap();
            let ot = l.pop();
            match ot {
                Some(task) => {
                    if l.len() > 0 && l.len() % threshold == 0 {
                        debug!("{}{}", label, l.len());
                    }
                    drop(l);
                    self.cur_size.fetch_sub(task.size(), Ordering::Relaxed);
                    task.run();
                }
                None => {
                    // we don't use the lock
                    // https://rust-lang.github.io/rust-clippy/master/index.html#let_underscore_lock
                    std::mem::drop(self.tasks.1.wait(l).unwrap());
                }
            }
        }
    }

    pub fn with_logging(threshold: usize, label: String) -> Self {
        let thread_id = match env::var("STARPU_TCP_SYNC_SUB") {
            Err(_) => Some(thread::current().id()),
            Ok(_) => None,
        };
        let sync_prio_size = match env::var("STARPU_TCP_SYNC_PRIO") {
            Err(_) => 0,
            Ok(s) => s.parse().unwrap(),
        };
        let sync_small = match env::var("STARPU_TCP_SYNC_SMALL") {
            Err(_) => 0,
            Ok(s) => s.parse().unwrap(),
        };
        let always_async = env::var("STARPU_TCP_SYNC_NEVER").is_ok();
        let r = Self {
            thread_id,
            tasks: Arc::new((Mutex::new(PrioQueue::new()), Condvar::new())),
            max_tasks: Arc::new(AtomicUsize::new(0)),
            max_size: Arc::new(AtomicUsize::new(0)),
            cur_size: Arc::new(AtomicUsize::new(0)),
            sync_prio_size,
            sync_small,
            always_async,
        };
        let rr = r.clone();
        // TODO: Only one thread for all peer may not be enough
        std::thread::Builder::new()
            .name("Writer".to_string())
            .spawn(move || {
                rr.run_tasks(threshold, label);
            })
            .unwrap();
        r
    }

    pub fn push(&self, task: T, priority: isize) {
        if task.size() < self.sync_small {
            task.run();
        } else {
            // We need to lock self.tasks
            self.push_with_lock(task, priority);
        }
    }

    fn is_sync(&self, top_prio: Option<isize>, priority: isize) -> bool {
        if Some(thread::current().id()) == self.thread_id {
            // Main thread so we are async to do not delay submission
            return false;
        }
        if self.cur_size.load(Ordering::Relaxed) < self.sync_prio_size {
            // The queue is small enough so we keep are async
            return false;
        }
        match top_prio {
            // True if this task is urgent
            Some(p) => priority > p,
            // The queue is empty so all tasks are concidered urgent
            None => true,
        }
    }

    fn push_with_lock(&self, task: T, priority: isize) {
        let mut l = self.tasks.0.lock().unwrap();
        let size_before = l.len();
        if !self.always_async && self.is_sync(l.highest(), priority) {
            task.run();
        } else {
            let o = Ordering::Relaxed;
            self.cur_size.fetch_add(task.size(), o);
            self.max_size.fetch_max(self.cur_size.load(o), o);
            self.max_tasks.fetch_max(l.len(), o);
            l.push(priority, task);
        }
        drop(l);
        if size_before == 0 {
            self.tasks.1.notify_one();
        }
    }

    pub fn log_stats(&self) {
        let mt: usize = self.max_tasks.load(Ordering::Relaxed);
        let ms: usize = self.max_size.load(Ordering::Relaxed);
        debug!("TaskQueue: max_tasks={}, max_size={}", mt, ms);
    }
}
