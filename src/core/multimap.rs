use log::debug;
use std::cmp;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
#[cfg(not(feature = "debug"))]
use std::sync::Mutex;
#[cfg(feature = "debug")]
use tracing_mutex::stdsync::TracingMutex as Mutex;

pub struct PrioQueue<K, V> {
    data: BTreeMap<K, VecDeque<V>>,
    size: usize,
}

impl<K: cmp::Ord + Clone, V> PrioQueue<K, V> {
    pub fn new() -> Self {
        PrioQueue {
            data: BTreeMap::new(),
            size: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.size
    }

    pub fn highest(&self) -> Option<K> {
        self.data.iter().next_back().map(|(k, _)| k.clone())
    }

    pub fn push(&mut self, key: K, value: V) {
        self.size += 1;
        if let Some(v) = self.data.get_mut(&key) {
            v.push_front(value);
        } else {
            self.data.insert(key, VecDeque::from([value]));
        }
    }

    pub fn pop(&mut self) -> Option<V> {
        let (key, values) = self.data.iter_mut().next_back()?;
        let kc = key.clone();
        let r = values.pop_back().unwrap();
        self.size -= 1;
        if values.is_empty() {
            self.data.remove(&kc);
        }
        Some(r)
    }
}
#[derive(Debug)]
struct UnsafeNoValMap<K: Hash + Eq + Debug, V: Debug> {
    internal: UnsafeMultiMap<K, V>,
    no_val: UnsafeMultiSet<K>,
    /// Number of unkown values
    unk_size: usize,
}

impl<K: Hash + Eq + Debug, V: Debug> UnsafeNoValMap<K, V> {
    pub fn pop_or_insert_nv(&mut self, key: K) -> Option<V> {
        let r = self.internal.pop(&key);
        if r.is_none() {
            self.no_val.insert(key);
            self.unk_size += 1;
        }
        r
    }
    pub fn pop_nv_or_insert(&mut self, key: K, value: V) -> Option<V> {
        if self.no_val.pop(&key) {
            self.unk_size -= 1;
            Some(value)
        } else {
            self.internal.insert(key, value);
            None
        }
    }

    pub fn new() -> Self {
        Self {
            internal: UnsafeMultiMap::new(),
            no_val: UnsafeMultiSet::new(),
            unk_size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.internal.len()
    }

    pub fn unk_len(&self) -> usize {
        self.unk_size
    }
}

/// A multi-map which allow to have keys with a given number of unknown values
pub struct UnkValMap<K: Hash + Eq + Debug, V: Debug> {
    data: Mutex<UnsafeNoValMap<K, V>>,
    log_label: String,
    log_threshold: usize,
}

impl<K: Hash + Eq + Debug, V: Debug> Debug for UnkValMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", *self.data.lock().unwrap())
    }
}

impl<K: Hash + Eq + Debug, V: Debug> UnkValMap<K, V> {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(UnsafeNoValMap::new()),
            log_label: "".to_string(),
            log_threshold: usize::MAX,
        }
    }

    pub fn log(&mut self, label: String, threshold: usize) {
        self.log_label = label;
        self.log_threshold = threshold;
    }

    pub fn is_empty(&self) -> bool {
        self.data.lock().unwrap().internal.data.is_empty()
    }
    pub fn pop_or_insert_nv(&self, key: K) -> Option<V> {
        let mut l = self.data.lock().unwrap();
        if l.len() > 0 && l.len() % self.log_threshold == 0 {
            debug!("{} known size: {}", self.log_label, l.len());
        }
        if l.unk_len() > 0 && l.unk_len() % self.log_threshold == 0 {
            debug!("{} unknown size: {}", self.log_label, l.unk_len());
        }
        l.pop_or_insert_nv(key)
    }
    pub fn pop_nv_or_insert(&self, key: K, value: V) -> Option<V> {
        let mut l = self.data.lock().unwrap();
        if l.len() > 0 && l.len() % self.log_threshold == 0 {
            debug!("{} known size: {}", self.log_label, l.len());
        }
        if l.unk_len() > 0 && l.unk_len() % self.log_threshold == 0 {
            debug!("{} unknown size: {}", self.log_label, l.unk_len());
        }
        l.pop_nv_or_insert(key, value)
    }
}

#[derive(Debug)]
struct UnsafeMultiSet<K: Hash + Eq + Debug>(HashMap<K, usize>);

impl<K: Hash + Eq + Debug> UnsafeMultiSet<K> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn pop(&mut self, key: &K) -> bool {
        let h = &mut self.0;
        match h.get_mut(key) {
            Some(vc) => {
                *vc -= 1;
                if *vc == 0 {
                    h.remove(key);
                }
                true
            }
            None => false,
        }
    }
    pub fn insert(&mut self, key: K) {
        let h = &mut self.0;
        match h.get_mut(&key) {
            Some(vc) => {
                *vc += 1;
            }
            None => {
                h.insert(key, 1);
            }
        }
    }
}

#[derive(Debug)]
pub struct UnsafeMultiMap<K: Hash + Eq + Debug, V: Debug> {
    data: HashMap<K, VecDeque<V>>,
    size: usize,
}

impl<K: Hash + Eq + Debug, V: Debug> UnsafeMultiMap<K, V> {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            size: 0,
        }
    }

    pub fn pop(&mut self, key: &K) -> Option<V> {
        let h = &mut self.data;
        match h.get_mut(key) {
            Some(vc) => {
                let r = vc.pop_back();
                if vc.is_empty() {
                    h.remove(key);
                }
                self.size -= 1;
                r
            }
            None => None,
        }
    }
    pub fn insert(&mut self, key: K, value: V) {
        let h = &mut self.data;
        match h.get_mut(&key) {
            Some(vc) => {
                vc.push_front(value);
            }
            None => {
                let mut vc = VecDeque::with_capacity(1);
                vc.push_front(value);
                h.insert(key, vc);
            }
        }
        self.size += 1;
    }

    pub fn len(&self) -> usize {
        self.size
    }
}

#[derive(Debug)]
struct UnsafeDoubleTypeMultiMap<K: Hash + Eq + Debug, V1: Debug, V2: Debug> {
    v1map: UnsafeMultiMap<K, V1>,
    v2map: UnsafeMultiMap<K, V2>,
}

impl<K: Hash + Eq + Debug, V1: Debug, V2: Debug> UnsafeDoubleTypeMultiMap<K, V1, V2> {
    // We return V2 to give back ownership
    fn pop1_or_insert2(&mut self, key: K, v2: V2) -> Option<(V1, V2)> {
        if let Some(v1) = self.v1map.pop(&key) {
            Some((v1, v2))
        } else {
            self.v2map.insert(key, v2);
            None
        }
    }
    fn insert1_or_pop2(&mut self, key: K, v1: V1) -> Option<(V1, V2)> {
        if let Some(v2) = self.v2map.pop(&key) {
            Some((v1, v2))
        } else {
            self.v1map.insert(key, v1);
            None
        }
    }

    fn new() -> Self {
        Self {
            v1map: UnsafeMultiMap::new(),
            v2map: UnsafeMultiMap::new(),
        }
    }
}

pub struct DoubleTypeMultiMap<K: Hash + Eq + Debug, V1: Debug, V2: Debug> {
    data: Mutex<UnsafeDoubleTypeMultiMap<K, V1, V2>>,
}

impl<K: Hash + Eq + Debug, V1: Debug, V2: Debug> DoubleTypeMultiMap<K, V1, V2> {
    pub fn pop1_or_insert2(&self, key: K, v: V2) -> Option<(V1, V2)> {
        self.data.lock().unwrap().pop1_or_insert2(key, v)
    }

    pub fn insert1_or_pop2(&self, key: K, v: V1) -> Option<(V1, V2)> {
        self.data.lock().unwrap().insert1_or_pop2(key, v)
    }

    pub fn new() -> Self {
        Self {
            data: Mutex::new(UnsafeDoubleTypeMultiMap::new()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.lock().unwrap().v1map.len() == 0
    }
}

impl<K: Hash + Eq + Debug, V1: Debug, V2: Debug> Debug for DoubleTypeMultiMap<K, V1, V2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", *self.data.lock().unwrap())
    }
}
