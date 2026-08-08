use maker_domain::ClientOrderId;

pub(crate) const LIFECYCLE_CAPACITY: usize = 256;
pub(crate) const MAX_LEVELS_PER_SIDE: usize = 64;
pub(crate) const MAX_GRID_LEVELS: usize = MAX_LEVELS_PER_SIDE * 2;

#[derive(Debug)]
struct IdEntry<T> {
    id: ClientOrderId,
    value: T,
}

/// Direct-addressed lifecycle storage. The low client-ID byte selects a slot;
/// every access validates the complete ID so a wrapped sequence can never
/// alias stale state.
#[derive(Debug)]
pub(crate) struct IdMap<T> {
    slots: [Option<IdEntry<T>>; LIFECYCLE_CAPACITY],
    len: usize,
}

impl<T> Default for IdMap<T> {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
            len: 0,
        }
    }
}

impl<T> IdMap<T> {
    const fn index(id: &ClientOrderId) -> usize {
        (id.get() & 0xff) as usize
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn contains_key(&self, id: &ClientOrderId) -> bool {
        self.get(id).is_some()
    }

    pub(crate) fn get(&self, id: &ClientOrderId) -> Option<&T> {
        self.slots[Self::index(id)]
            .as_ref()
            .filter(|entry| entry.id == *id)
            .map(|entry| &entry.value)
    }

    pub(crate) fn get_mut(&mut self, id: &ClientOrderId) -> Option<&mut T> {
        self.slots[Self::index(id)]
            .as_mut()
            .filter(|entry| entry.id == *id)
            .map(|entry| &mut entry.value)
    }

    pub(crate) fn insert(&mut self, id: ClientOrderId, value: T) -> Result<Option<T>, ()> {
        let slot = &mut self.slots[Self::index(&id)];
        match slot {
            Some(entry) if entry.id == id => Ok(Some(std::mem::replace(&mut entry.value, value))),
            Some(_) => Err(()),
            None => {
                *slot = Some(IdEntry { id, value });
                self.len += 1;
                Ok(None)
            }
        }
    }

    pub(crate) fn remove(&mut self, id: &ClientOrderId) -> Option<T> {
        let slot = &mut self.slots[Self::index(id)];
        if slot.as_ref().is_some_and(|entry| entry.id == *id) {
            self.len -= 1;
            slot.take().map(|entry| entry.value)
        } else {
            None
        }
    }

    pub(crate) fn clear(&mut self) {
        for slot in &mut self.slots {
            *slot = None;
        }
        self.len = 0;
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().map(|(_, value)| value)
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.iter_mut().map(|(_, value)| value)
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = ClientOrderId> + '_ {
        self.iter().map(|(id, _)| id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (ClientOrderId, &T)> {
        self.slots
            .iter()
            .filter_map(|slot| slot.as_ref().map(|entry| (entry.id, &entry.value)))
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (ClientOrderId, &mut T)> {
        self.slots
            .iter_mut()
            .filter_map(|slot| slot.as_mut().map(|entry| (entry.id, &mut entry.value)))
    }
}

#[derive(Debug, Default)]
pub(crate) struct IdSet(IdMap<()>);

impl IdSet {
    pub(crate) fn contains(&self, id: &ClientOrderId) -> bool {
        self.0.contains_key(id)
    }

    pub(crate) fn insert(&mut self, id: ClientOrderId) -> Result<bool, ()> {
        self.0.insert(id, ()).map(|previous| previous.is_none())
    }

    pub(crate) fn remove(&mut self, id: &ClientOrderId) -> bool {
        self.0.remove(id).is_some()
    }

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ClientOrderId> + '_ {
        self.0.iter().map(|(id, ())| id)
    }
}

/// Inline ordered scratch storage with explicit overflow and no eviction.
#[derive(Debug)]
pub(crate) struct FixedVec<T, const N: usize> {
    entries: [Option<T>; N],
    len: usize,
}

impl<T, const N: usize> Default for FixedVec<T, N> {
    fn default() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
            len: 0,
        }
    }
}

impl<T, const N: usize> FixedVec<T, N> {
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn push(&mut self, value: T) -> Result<(), ()> {
        if self.len == N {
            return Err(());
        }
        self.entries[self.len] = Some(value);
        self.len += 1;
        Ok(())
    }

    pub(crate) fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        self.entries[self.len].take()
    }

    pub(crate) fn sort_by(&mut self, mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering) {
        for right in 1..self.len {
            let mut left = right;
            while left > 0 {
                let ordering = compare(
                    self.entries[left - 1].as_ref().expect("initialized prefix"),
                    self.entries[left].as_ref().expect("initialized prefix"),
                );
                if ordering != std::cmp::Ordering::Greater {
                    break;
                }
                self.entries.swap(left - 1, left);
                left -= 1;
            }
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        (index < self.len)
            .then(|| self.entries[index].as_ref())
            .flatten()
    }

    pub(crate) fn remove(&mut self, index: usize) -> Option<T> {
        if index >= self.len {
            return None;
        }
        let removed = self.entries[index].take();
        for position in index..self.len - 1 {
            self.entries[position] = self.entries[position + 1].take();
        }
        self.len -= 1;
        removed
    }

    pub(crate) fn clear(&mut self) {
        while self.pop().is_some() {}
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.entries[..self.len]
            .iter()
            .map(|entry| entry.as_ref().expect("initialized fixed-vector prefix"))
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.entries[..self.len]
            .iter_mut()
            .map(|entry| entry.as_mut().expect("initialized fixed-vector prefix"))
    }

    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut index = 0;
        while index < self.len {
            if keep(self.get(index).expect("index is inside initialized prefix")) {
                index += 1;
            } else {
                self.remove(index);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maker_domain::ClientOrderId;

    use super::*;

    #[test]
    fn lifecycle_slots_validate_full_id_and_never_evict() {
        let first = ClientOrderId::new(1).unwrap();
        let stale_collision = ClientOrderId::new(257).unwrap();
        let mut map = IdMap::default();

        assert_eq!(map.insert(first, 11), Ok(None));
        assert_eq!(map.get(&first), Some(&11));
        assert_eq!(map.get(&stale_collision), None);
        assert_eq!(map.insert(stale_collision, 22), Err(()));
        assert_eq!(map.get(&first), Some(&11));
        assert_eq!(map.remove(&stale_collision), None);
        assert_eq!(map.remove(&first), Some(11));
        assert_eq!(map.insert(stale_collision, 22), Ok(None));
        assert_eq!(map.get(&stale_collision), Some(&22));
    }

    #[test]
    fn fixed_vector_reports_full_without_eviction() {
        let mut values = FixedVec::<u8, 2>::default();
        assert_eq!(values.push(1), Ok(()));
        assert_eq!(values.push(2), Ok(()));
        assert_eq!(values.push(3), Err(()));
        assert_eq!(values.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
    }
}
