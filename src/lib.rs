pub mod swiss;

use std::{
    hash::{BuildHasher, Hash, RandomState},
    iter, mem,
};

pub struct Map<K: Hash + Eq, V> {
    slots: Vec<Slot<Entry<K, V>>>,
    count: usize,
    hasher: RandomState,
}

enum Slot<T> {
    Empty,
    Deleted,
    Occupied(T),
}

struct Entry<K, V> {
    key: K,
    value: V,
}

const INITIAL_SIZE: usize = 64;
const LOAD_FACTOR: f64 = 0.9;
const EXPANSION_FACTOR: usize = 2;

impl<K: Hash + Eq, V> Map<K, V> {
    pub fn new() -> Self {
        Map {
            slots: iter::repeat_with(|| Slot::Empty)
                .take(INITIAL_SIZE)
                .collect(),
            count: 0,
            hasher: RandomState::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.find_index(&key) {
            Some(i) => Some(mem::replace(
                &mut self.slots[i].as_mut().unwrap().value,
                value,
            )),
            None => {
                self.expand();
                let i = self.find_empty(&key);
                self.slots[i] = Slot::Occupied(Entry { key, value });
                self.count += 1;
                None
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let i = self.find_index(key)?;
        Some(&self.slots[i].as_ref().unwrap().value)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let i = self.find_index(key)?;
        Some(&mut self.slots[i].as_mut().unwrap().value)
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        let i = self.find_index(key)?;
        self.count -= 1;
        Some(self.slots[i].delete().value)
    }

    fn find_index(&self, key: &K) -> Option<usize> {
        let mut i = self.hash(key);
        let start_index = i;
        loop {
            match &self.slots[i] {
                Slot::Empty => return None,
                Slot::Occupied(entry) if entry.key == *key => return Some(i),
                _ => {}
            }
            i = (i + 1) % self.slots.len();
            if i == start_index {
                return None;
            }
        }
    }

    fn find_empty(&self, key: &K) -> usize {
        let mut i = self.hash(key);
        let start_index = i;
        loop {
            match self.slots[i] {
                Slot::Empty | Slot::Deleted => return i,
                _ => {}
            }
            i = (i + 1) % self.slots.len();
            if i == start_index {
                unreachable!("The map should always has empty slots.")
            }
        }
    }

    fn expand(&mut self) {
        if ((self.count as f64) / (self.slots.len() as f64)) < LOAD_FACTOR {
            return;
        }
        let new_slots = iter::repeat_with(|| Slot::Empty)
            .take(self.slots.len() * EXPANSION_FACTOR)
            .collect();
        let old_slots = mem::replace(&mut self.slots, new_slots);
        self.count = 0;
        old_slots
            .into_iter()
            .filter(|slot| slot.is_occupied())
            .map(|slot| slot.unwrap())
            .for_each(|entry| {
                self.insert(entry.key, entry.value);
            });
    }

    fn hash(&self, key: &K) -> usize {
        self.hasher.hash_one(key) as usize % self.slots.len()
    }
}

impl<T> Slot<T> {
    fn unwrap(self) -> T {
        if let Self::Occupied(value) = self {
            value
        } else {
            panic!("called `Slot::unwrap()` on a not `Occupied` value")
        }
    }

    fn as_ref(&self) -> Slot<&T> {
        match self {
            Self::Empty => Slot::Empty,
            Self::Deleted => Slot::Deleted,
            Self::Occupied(value) => Slot::Occupied(value),
        }
    }

    fn as_mut(&mut self) -> Slot<&mut T> {
        match self {
            Self::Empty => Slot::Empty,
            Self::Deleted => Slot::Deleted,
            Self::Occupied(value) => Slot::Occupied(value),
        }
    }

    fn delete(&mut self) -> T {
        mem::replace(self, Self::Deleted).unwrap()
    }

    fn is_occupied(&self) -> bool {
        matches!(self, Self::Occupied(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    #[test]
    fn test_new() {
        let map: Map<String, i32> = Map::new();
        assert_eq!(map.count, 0);
        assert_eq!(map.slots.len(), INITIAL_SIZE);
        assert!(map.slots.iter().all(|s| !s.is_occupied()));
    }

    #[test]
    fn test_insert_and_get() {
        let mut map = Map::new();

        // 插入新值，应返回 None
        assert_eq!(map.insert("one".to_string(), 1), None);
        assert_eq!(map.count, 1);
        assert_eq!(map.get(&"one".to_string()), Some(&1));
        assert_eq!(map.get(&"two".to_string()), None);

        // 更新现有值，应返回旧值
        assert_eq!(map.insert("one".to_string(), 11), Some(1));
        assert_eq!(map.count, 1); // count 不应该改变
        assert_eq!(map.get(&"one".to_string()), Some(&11));
    }

    #[test]
    fn test_get_mut() {
        let mut map = Map::new();
        map.insert("value".to_string(), 100);

        // 获取可变引用并修改
        let val = map.get_mut(&"value".to_string());
        assert!(val.is_some());
        *val.unwrap() += 1;

        assert_eq!(map.get(&"value".to_string()), Some(&101));
    }

    #[test]
    fn test_delete() {
        let mut map = Map::new();
        map.insert("one".to_string(), 1);
        map.insert("two".to_string(), 2);
        assert_eq!(map.count, 2);

        // 删除存在的键
        assert_eq!(map.delete(&"one".to_string()), Some(1));
        assert_eq!(map.count, 1);
        assert_eq!(map.get(&"one".to_string()), None); // 确认已删除
        assert_eq!(map.get(&"two".to_string()), Some(&2)); // 确认其他键不受影响

        // 删除一个不存在的键
        assert_eq!(map.delete(&"three".to_string()), None);
        assert_eq!(map.count, 1);
    }

    #[test]
    fn test_delete_and_probe() {
        let mut map: Map<i32, i32> = Map::new();

        let len = map.slots.len();
        let key1 = 1;
        let key2 = key1 + len as i32;

        map.insert(key1, 10);
        map.insert(key2, 20); // key2 会被放在 key1 后面的槽位

        assert_eq!(map.get(&key1), Some(&10));
        assert_eq!(map.get(&key2), Some(&20));

        // 删除 key1，留下墓碑
        map.delete(&key1);

        // 确认 key2 仍然可以被找到，证明探查越过了墓碑
        assert_eq!(map.get(&key2), Some(&20));
    }

    #[test]
    fn test_expansion() {
        let mut map = Map::new();
        // 确保你的 insert 方法在插入新元素时会增加 count，否则这个测试会失败
        let num_items = (INITIAL_SIZE as f64 * LOAD_FACTOR) as usize + 5;

        // 插入足够多的元素以触发扩容
        for i in 0..num_items {
            map.insert(i.to_string(), i);
        }

        // 确认容量已增加
        assert_eq!(map.count, num_items);
        assert_eq!(map.slots.len(), INITIAL_SIZE * EXPANSION_FACTOR);

        // 确认扩容后所有数据仍然可以访问
        for i in 0..num_items {
            assert_eq!(
                map.get(&i.to_string()),
                Some(&i),
                "Failed to get item {} after expansion",
                i
            );
        }
    }

    #[test]
    fn test_stress_and_correctness() {
        let mut map = Map::new();
        let mut std_map = StdHashMap::new();
        let num_items = 100000i64;

        // 大量插入
        for i in 0..num_items {
            let key = i.to_string();
            let value = i * i;
            let map_ret = map.insert(key.clone(), value);
            let std_map_ret = std_map.insert(key, value);
            assert_eq!(map_ret, std_map_ret, "Mismatch on insert for key {}", i);
        }

        assert_eq!(map.count, std_map.len());

        // 验证所有插入的数据
        for (key, value) in &std_map {
            assert_eq!(map.get(key), Some(value));
        }

        // 随机删除一半的数据
        for i in (0..num_items).filter(|x| x % 2 == 0) {
            let key = i.to_string();
            let map_ret = map.delete(&key);
            let std_map_ret = std_map.remove(&key);
            assert_eq!(map_ret, std_map_ret, "Mismatch on delete for key {}", i);
        }

        assert_eq!(map.count, std_map.len());

        // 再次验证剩余数据
        for (key, value) in &std_map {
            assert_eq!(map.get(key), Some(value));
        }
    }

    #[test]
    fn test_insert_after_delete() {
        let mut map: Map<i32, i32> = Map::new();

        // 填充 map，然后删除一些
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..5 {
            map.delete(&i);
        }
        assert_eq!(map.count, 5);

        // 重新插入之前删除的元素，应该被视为新插入
        assert_eq!(map.insert(0, 100), None);
        assert_eq!(map.get(&0), Some(&100));
        assert_eq!(map.count, 6);

        // 插入一个全新的元素，它应该能复用被删除的槽位
        assert_eq!(map.insert(100, 100), None);
        assert_eq!(map.get(&100), Some(&100));
        assert_eq!(map.count, 7);
    }
}
