use anybytes::Bytes;
pub use blake3::Hasher as Blake3;
use digest::Digest;
use hex_literal::hex;
use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::ptr::slice_from_raw_parts;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, io::Write};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

const MAGIC_MARKER: [u8; 16] = hex!("1E08B022FF2F47B6EBACF1D68EB35D96");
//TODO: Do we want to use different magic markers for different versions of the format?
// Or to distinguish between little and big endian header fields?
// Or do we want to have a "Head" type that encodes a `tribles::Head` and a
// short human readable name.

pub type Hash = [u8; 32];

struct AppendFile {
    file: File,
    length: usize,
}

#[derive(Debug, Clone, Copy)]
enum ValidationState {
    Unvalidated,
    Validated,
    Invalid,
}

struct IndexEntry {
    bytes: Bytes,
    state: ValidationState,
}

#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic_marker: [u8; 16],
    timestamp: u64,
    length: u64,
    hash: Hash,
}

impl Header {
    fn new(timestamp: u64, length: u64, hash: Hash) -> Self {
        Self {
            magic_marker: MAGIC_MARKER,
            timestamp,
            length,
            hash,
        }
    }
}

pub struct Pile<const MAX_PILE_SIZE: usize> {
    file: Mutex<AppendFile>,
    mmap: Arc<memmap2::MmapRaw>,
    index: RwLock<HashMap<Hash, Mutex<IndexEntry>>>,
}

#[derive(Debug)]
pub enum LoadError {
    IoError(std::io::Error),
    MagicMarkerError,
    HeaderError,
    UnexpectedEndOfFile,
    FileLengthError,
    PileTooLarge,
}

impl From<std::io::Error> for LoadError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

#[derive(Debug)]
pub enum InsertError {
    IoError(std::io::Error),
    PoisonError,
    PileTooLarge,
}

impl From<std::io::Error> for InsertError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl<T> From<PoisonError<T>> for InsertError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}

#[derive(Debug)]
pub enum GetError {
    PoisonError,
    ValidationError(Bytes),
}

impl<T> From<PoisonError<T>> for GetError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}

#[derive(Debug)]
pub enum FlushError {
    IoError(std::io::Error),
    PoisonError,
}

impl From<std::io::Error> for FlushError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl<T> From<PoisonError<T>> for FlushError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}

impl<const MAX_PILE_SIZE: usize> Pile<MAX_PILE_SIZE> {
    pub fn load(path: &Path) -> Result<Self, LoadError> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len > MAX_PILE_SIZE {
            return Err(LoadError::PileTooLarge);
        }
        let mmap = MmapOptions::new()
            .len(MAX_PILE_SIZE)
            .map_raw_read_only(&file)?;
        let mmap = Arc::new(mmap);
        let mut bytes = unsafe {
            let written_slice = slice_from_raw_parts(mmap.as_ptr(), file_len)
                .as_ref()
                .unwrap();
            Bytes::from_raw_parts(written_slice, mmap.clone())
        };
        if bytes.len() % 64 != 0 {
            return Err(LoadError::FileLengthError);
        }

        let mut index = HashMap::new();

        while let Ok(header) = bytes.view_prefix::<Header>() {
            if header.magic_marker != MAGIC_MARKER {
                return Err(LoadError::MagicMarkerError);
            }
            let hash = header.hash;
            let length = header.length as usize;
            let Some(blob_bytes) = bytes.take_prefix(length) else {
                return Err(LoadError::UnexpectedEndOfFile);
            };

            let Some(_) = bytes.take_prefix(64 - (length % 64)) else {
                return Err(LoadError::UnexpectedEndOfFile);
            };

            let blob = IndexEntry {
                state: ValidationState::Unvalidated,
                bytes: blob_bytes,
            };
            index.insert(hash, Mutex::new(blob));
        }

        let index = RwLock::new(index);

        let file = Mutex::new(AppendFile {
            file,
            length: file_len,
        });

        Ok(Self { file, mmap, index })
    }

    #[must_use]
    fn insert_raw(
        &mut self,
        hash: Hash,
        validation: ValidationState,
        value: &Bytes,
    ) -> Result<Bytes, InsertError> {
        let mut append = self.file.lock().unwrap();

        let old_length = append.length;
        let padding = 64 - (value.len() % 64);

        let new_length = old_length + 64 + value.len() + padding;
        if new_length > MAX_PILE_SIZE {
            return Err(InsertError::PileTooLarge);
        }

        append.length = new_length;

        let now_in_sys = SystemTime::now();
        let now_since_epoch = now_in_sys
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards");
        let now_in_ms = now_since_epoch.as_millis();

        let header = Header::new(now_in_ms as u64, value.len() as u64, hash);

        append.file.write_all(header.as_bytes())?;
        append.file.write_all(&value)?;
        append.file.write_all(&[0; 64][0..padding])?;

        let written_bytes = unsafe {
            let written_slice =
                slice_from_raw_parts(self.mmap.as_ptr().offset(old_length as _), value.len())
                    .as_ref()
                    .unwrap();
            Bytes::from_raw_parts(written_slice, self.mmap.clone())
        };

        let mut index = self.index.write()?;
        index.insert(
            hash,
            Mutex::new(IndexEntry {
                state: validation,
                bytes: written_bytes.clone(),
            }),
        );

        Ok(written_bytes)
    }

    pub fn insert(&mut self, value: &Bytes) -> Result<Hash, InsertError> {
        let hash: Hash = Blake3::digest(&value).into();

        let _bytes = self.insert_raw(hash, ValidationState::Validated, value)?;

        Ok(hash)
    }

    pub fn insert_validated(&mut self, hash: Hash, value: &Bytes) -> Result<Bytes, InsertError> {
        self.insert_raw(hash, ValidationState::Validated, value)
    }

    pub fn insert_unvalidated(&mut self, hash: Hash, value: &Bytes) -> Result<Bytes, InsertError> {
        self.insert_raw(hash, ValidationState::Unvalidated, value)
    }

    pub fn get(&self, hash: &Hash) -> Result<Option<Bytes>, GetError> {
        let index = self.index.read().unwrap();
        let Some(blob) = index.get(hash) else {
            return Ok(None);
        };
        let mut entry = blob.lock().unwrap();
        match entry.state {
            ValidationState::Validated => {
                return Ok(Some(entry.bytes.clone()));
            }
            ValidationState::Invalid => {
                return Err(GetError::ValidationError(entry.bytes.clone()));
            }
            ValidationState::Unvalidated => {
                let computed_hash: Hash = Blake3::digest(&entry.bytes).into();
                if computed_hash != *hash {
                    entry.state = ValidationState::Invalid;
                    return Err(GetError::ValidationError(entry.bytes.clone()));
                } else {
                    entry.state = ValidationState::Validated;
                    return Ok(Some(entry.bytes.clone()));
                }
            }
        }
    }

    pub fn flush(&self) -> Result<(), FlushError> {
        let append = self.file.lock()?;
        append.file.sync_data()?;
        Ok(())
    }
}

impl<const MAX_PILE_SIZE: usize> Extend<Bytes> for Pile<MAX_PILE_SIZE> {
    fn extend<T: IntoIterator<Item = Bytes>>(&mut self, iter: T) {
        for bytes in iter {
            let _ = self.insert(&bytes);
        }
    }
}

impl<const MAX_PILE_SIZE: usize> Extend<(Hash, Bytes)> for Pile<MAX_PILE_SIZE> {
    fn extend<T: IntoIterator<Item = (Hash, Bytes)>>(&mut self, iter: T) {
        for (hash, bytes) in iter {
            let _ = self.insert_unvalidated(hash, &bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::RngCore;
    use tempfile;

    #[test]
    fn load() {
        const RECORD_LEN: usize = 1 << 10; // 1k
        const RECORD_COUNT: usize = 1 << 20; // 1M
        const MAX_PILE_SIZE: usize = 1 << 30; // 100GB

        let mut rng = rand::thread_rng();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_pile = tmp_dir.path().join("test.pile");
        let mut pile: Pile<MAX_PILE_SIZE> = Pile::load(&tmp_pile).unwrap();

        (0..RECORD_COUNT).for_each(|_| {
            let mut record = Vec::with_capacity(RECORD_LEN);
            rng.fill_bytes(&mut record);

            let data = Bytes::from_source(record);
            pile.insert(&data).unwrap();
        });

        pile.flush().unwrap();

        drop(pile);

        let _pile: Pile<MAX_PILE_SIZE> = Pile::load(&tmp_pile).unwrap();
    }
}
