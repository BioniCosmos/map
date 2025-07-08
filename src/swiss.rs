use std::{
    hash::{BuildHasher, Hash, RandomState},
    iter, mem,
};

pub struct Map<K: Hash + Eq, V> {
    slots: Vec<Option<Entry<K, V>>>,
    count: usize,
    group_count: usize,
    ctrl: Vec<Ctrl>,
    hasher: RandomState,
}

struct Entry<K, V> {
    key: K,
    value: V,
}

#[derive(Copy, Clone)]
struct Ctrl([u8; 8]);

enum Slot {
    Deleted,
    Occupied(u8),
}

const GROUP_SIZE: usize = 8;

impl<K: Hash + Eq, V> Map<K, V> {
    pub fn new() -> Self {
        const INITIAL_GROUP_COUNT: usize = 8;
        const INITIAL_SIZE: usize = INITIAL_GROUP_COUNT * GROUP_SIZE;
        Self {
            slots: iter::repeat_with(|| None).take(INITIAL_SIZE).collect(),
            count: 0,
            group_count: INITIAL_GROUP_COUNT,
            ctrl: vec![Ctrl::new(); INITIAL_GROUP_COUNT],
            hasher: RandomState::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.is_overloaded() {
            self.expand();
        }
        let (group_index, h2) = self.hash(&key);
        if let Some(slot_index) = self.find_slot_index(&key, group_index, h2) {
            return Some(mem::replace(
                &mut self.slots[slot_index].as_mut().unwrap().value,
                value,
            ));
        }
        let slot_index = self.find_empty_slot_index(group_index);
        let (group_index, ctrl_index) = self.get_group_and_ctrl_indices(slot_index);
        self.ctrl[group_index].set(ctrl_index, Slot::Occupied(h2));
        self.count += 1;
        self.slots[slot_index] = Some(Entry { key, value });
        None
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let (group_index, h2) = self.hash(key);
        let slot_index = self.find_slot_index(key, group_index, h2)?;
        Some(&self.slots[slot_index].as_ref().unwrap().value)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let (group_index, h2) = self.hash(key);
        let slot_index = self.find_slot_index(key, group_index, h2)?;
        Some(&mut self.slots[slot_index].as_mut().unwrap().value)
    }

    pub fn contains(&self, key: &K) -> bool {
        let (group_index, h2) = self.hash(key);
        self.find_slot_index(key, group_index, h2).is_some()
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        let (group_index, h2) = self.hash(key);
        let slot_index = self.find_slot_index(key, group_index, h2)?;
        let (group_index, ctrl_index) = self.get_group_and_ctrl_indices(slot_index);
        self.ctrl[group_index].set(ctrl_index, Slot::Deleted);
        self.count -= 1;
        Some(self.slots[slot_index].take().unwrap().value)
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter { map: self, i: 0 }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        IterMut { map: self, i: 0 }
    }

    fn find_slot_index(&self, key: &K, group_index: usize, h2: u8) -> Option<usize> {
        let mut i = group_index;
        loop {
            let ctrl = &self.ctrl[i];
            let (matches, found_empty) = ctrl.find_h2(h2);
            for ctrl_index in matches {
                let slot_index = self.get_slot_index(i, ctrl_index);
                if let Some(entry) = self.slots[slot_index].as_ref() {
                    if entry.key == *key {
                        return Some(slot_index);
                    }
                }
            }
            if found_empty {
                return None;
            }
            i = (i + 1) % self.group_count;
            if i == group_index {
                return None;
            }
        }
    }

    fn find_empty_slot_index(&self, group_index: usize) -> usize {
        let mut i = group_index;
        loop {
            if let Some(ctrl_index) = self.ctrl[i].find_empty_and_deleted() {
                return self.get_slot_index(i, ctrl_index);
            }
            i = (i + 1) % self.group_count;
            if i == group_index {
                unreachable!(
                    "The map should always have empty slots because we expand when overloaded."
                );
            }
        }
    }

    const fn get_slot_index(&self, group_index: usize, ctrl_index: usize) -> usize {
        group_index * GROUP_SIZE + ctrl_index
    }

    const fn get_group_and_ctrl_indices(&self, slot_index: usize) -> (usize, usize) {
        (slot_index / GROUP_SIZE, slot_index % GROUP_SIZE)
    }

    const fn is_overloaded(&self) -> bool {
        self.count as f64 / self.slots.len() as f64 >= 0.9
    }

    fn expand(&mut self) {
        const EXPANSION_FACTOR: usize = 2;
        let new_group_count = self.group_count * EXPANSION_FACTOR;
        let new_size = new_group_count * GROUP_SIZE;
        let mut new_map = Self {
            slots: iter::repeat_with(|| None).take(new_size).collect(),
            count: 0,
            group_count: new_group_count,
            ctrl: vec![Ctrl::new(); new_group_count],
            hasher: RandomState::new(),
        };
        for entry in mem::take(&mut self.slots).into_iter() {
            if let Some(entry) = entry {
                new_map.insert(entry.key, entry.value);
            }
        }
        *self = new_map;
    }

    fn hash(&self, key: &K) -> (usize, u8) {
        let h = self.hasher.hash_one(key);
        const H2_LEN: usize = 7;
        const H2_MASK: u8 = 0b0111_1111;
        let h1 = h >> H2_LEN;
        let h2 = h as u8 & H2_MASK;
        let group_index = (h1 % self.group_count as u64) as usize;
        (group_index, h2)
    }
}

impl Ctrl {
    const SLOT_EMPTY: u8 = 0b1000_0000;
    const SLOT_DELETED: u8 = 0b1111_1110;

    const fn new() -> Self {
        Self([Self::SLOT_EMPTY; GROUP_SIZE])
    }

    fn find_h2(self, h2: u8) -> (Vec<usize>, bool) {
        let mut matches = Vec::new();
        for (i, &c) in self.0.iter().enumerate() {
            if c == Self::SLOT_EMPTY {
                return (matches, true);
            }
            if c == h2 {
                matches.push(i);
            }
        }
        (matches, false)
    }

    fn find_empty_and_deleted(self) -> Option<usize> {
        for (i, &c) in self.0.iter().enumerate() {
            if c == Self::SLOT_EMPTY || c == Self::SLOT_DELETED {
                return Some(i);
            }
        }
        None
    }

    fn set(&mut self, i: usize, slot: Slot) {
        self.0[i] = match slot {
            Slot::Deleted => Self::SLOT_DELETED,
            Slot::Occupied(h2) => h2,
        }
    }
}

pub struct Iter<'a, K: Hash + Eq, V> {
    map: &'a Map<K, V>,
    i: usize,
}

impl<'a, K: Hash + Eq, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.map.slots.len() {
            if let Some(entry) = &self.map.slots[self.i] {
                self.i += 1;
                return Some((&entry.key, &entry.value));
            }
            self.i += 1;
        }
        None
    }
}

pub struct IterMut<'a, K: Hash + Eq, V> {
    map: &'a mut Map<K, V>,
    i: usize,
}

impl<'a, K: Hash + Eq, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.map.slots.len() {
            if let Some(entry) = &mut self.map.slots[self.i] {
                self.i += 1;
                let entry: *mut Entry<K, V> = entry;
                return Some(unsafe { (&(*entry).key, &mut (*entry).value) });
            }
            self.i += 1;
        }
        None
    }
}

pub struct IntoIter<K: Hash + Eq, V> {
    map: Map<K, V>,
    i: usize,
}

impl<K: Hash + Eq, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.map.slots.len() {
            if let Some(entry) = self.map.slots[self.i].take() {
                self.i += 1;
                return Some((entry.key, entry.value));
            }
            self.i += 1;
        }
        None
    }
}

impl<K: Hash + Eq, V> IntoIterator for Map<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { map: self, i: 0 }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;

    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut map = Map::new();
        map.insert("key1".to_string(), 1);
        map.insert("key2".to_string(), 2);

        assert_eq!(map.get(&"key1".to_string()), Some(&1));
        assert_eq!(map.get(&"key2".to_string()), Some(&2));
        assert_eq!(map.get(&"key3".to_string()), None);
    }

    #[test]
    fn test_insert_overwrite() {
        let mut map = Map::new();
        map.insert("key1".to_string(), 1);
        assert_eq!(map.get(&"key1".to_string()), Some(&1));

        let old_value = map.insert("key1".to_string(), 2);
        assert_eq!(old_value, Some(1));
        assert_eq!(map.get(&"key1".to_string()), Some(&2));
    }

    #[test]
    fn test_get_mut() {
        let mut map = Map::new();
        map.insert("key1".to_string(), 1);

        let value = map.get_mut(&"key1".to_string()).unwrap();
        *value = 10;

        assert_eq!(map.get(&"key1".to_string()), Some(&10));
    }

    #[test]
    fn test_delete() {
        let mut map = Map::new();
        map.insert("key1".to_string(), 1);
        map.insert("key2".to_string(), 2);

        assert_eq!(map.delete(&"key1".to_string()), Some(1));
        assert_eq!(map.get(&"key1".to_string()), None);
        assert_eq!(map.get(&"key2".to_string()), Some(&2));
        assert_eq!(map.delete(&"key3".to_string()), None);
    }

    #[test]
    fn test_expansion() {
        let mut map = Map::new();
        for i in 0..100 {
            map.insert(i.to_string(), i);
        }

        assert!(map.slots.len() > 64); // Check that expansion happened
        for i in 0..100 {
            assert_eq!(map.get(&i.to_string()), Some(&i));
        }
    }

    #[test]
    fn test_delete_and_reinsert() {
        let mut map = Map::new();
        map.insert("key1".to_string(), 1);
        assert_eq!(map.delete(&"key1".to_string()), Some(1));
        assert_eq!(map.get(&"key1".to_string()), None);
        map.insert("key1".to_string(), 2);
        assert_eq!(map.get(&"key1".to_string()), Some(&2));
    }

    #[test]
    fn test_many_insertions_and_deletions() {
        let mut map = Map::new();
        for i in 0..1000 {
            map.insert(i, i);
        }
        for i in 0..1000 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in (0..1000).step_by(2) {
            assert_eq!(map.delete(&i), Some(i));
        }
        for i in 0..1000 {
            if i % 2 == 0 {
                assert_eq!(map.get(&i), None);
            } else {
                assert_eq!(map.get(&i), Some(&i));
            }
        }
    }

    #[test]
    fn test_contains() {
        let mut map = Map::new();
        map.insert("a".to_string(), 1);
        assert!(map.contains(&"a".to_string()));
        assert!(!map.contains(&"b".to_string()));
    }

    #[test]
    fn test_iter() {
        let mut map = Map::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        let mut std_map = StdHashMap::new();
        std_map.insert("a".to_string(), 1);
        std_map.insert("b".to_string(), 2);

        let mut count = 0;
        for (k, v) in map.iter() {
            assert_eq!(std_map.get(k), Some(v));
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn test_iter_mut() {
        let mut map = Map::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);

        for (_, v) in map.iter_mut() {
            *v *= 2;
        }

        assert_eq!(map.get(&"a".to_string()), Some(&2));
        assert_eq!(map.get(&"b".to_string()), Some(&4));
    }

    #[test]
    fn test_into_iter() {
        let mut map = Map::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        let mut std_map = StdHashMap::new();
        std_map.insert("a".to_string(), 1);
        std_map.insert("b".to_string(), 2);

        let mut count = 0;
        for (k, v) in map.into_iter() {
            assert_eq!(std_map.get(&k), Some(&v));
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
