use anybytes::Bytes;
pub use blake3::Hasher as Blake3;
use digest::Digest;
use hex_literal::hex;
use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::ptr::slice_from_raw_parts;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::{collections::HashMap, io::Write};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

const MAGIC_MARKER: [u8; 16] = hex!("1E08B022FF2F47B6EBACF1D68EB35D96");

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
struct BlobHeader {
    magic_marker: [u8; 16],
    padding: [u8; 8],
    length: u64,
    hash: [u8; 32],
}

pub struct Pile<const MAX_PILE_SIZE: usize> {
    file: Mutex<AppendFile>,
    mmap: Arc<memmap2::MmapRaw>,
    index: RwLock<HashMap<[u8; 32], Mutex<IndexEntry>>>,
}

pub enum LoadError {
    IoError(std::io::Error),
    MagicMarkerError,
    HeaderError,
    UnexpectedEndOfFile,
    FileLengthError,
}

impl From<std::io::Error> for LoadError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

//TODO: Add db recovery if the last blob is not complete
fn load_index(bytes: Bytes) -> Result<HashMap<[u8; 32], Mutex<IndexEntry>>, LoadError> {
    if bytes.len() % 64 != 0 {
        return Err(LoadError::FileLengthError);
    }

    let mut bytes = bytes;
    let mut index = HashMap::new();

    while let Ok(header) = bytes.view_prefix::<BlobHeader>() {
        if header.magic_marker != MAGIC_MARKER {
            return Err(LoadError::MagicMarkerError);
        }
        if header.padding != [0; 8] {
            return Err(LoadError::HeaderError);
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
    Ok(index)
}

pub enum InsertError {
    IoError(std::io::Error),
    PoisonError,
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

pub enum GetError {
    PoisonError,
    ValidationError(Bytes),
}

impl<T> From<PoisonError<T>> for GetError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}

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
    pub fn new(path: &Path) -> Result<Self, LoadError> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file_len = file.metadata()?.len() as usize;
        let mmap = MmapOptions::new()
            .len(MAX_PILE_SIZE)
            .map_raw_read_only(&file)?;
        let mmap = Arc::new(mmap);
        let all_bytes = unsafe {
            let written_slice = slice_from_raw_parts(mmap.as_ptr(), file_len)
                .as_ref()
                .unwrap();
            Bytes::from_raw_parts(written_slice, mmap.clone())
        };
        let index = load_index(all_bytes)?;
        let index = RwLock::new(index);
        let file = Mutex::new(AppendFile {
            file,
            length: file_len,
        });
        Ok(Self { file, mmap, index })
    }

    pub fn insert(&mut self, value: &Bytes) -> Result<([u8; 32], Bytes), InsertError> {
        let mut append = self.file.lock().unwrap();

        let hash: [u8; 32] = Blake3::digest(&value).into();

        let mut header: [u8; 64] = [0; 64];
        header[32..64].copy_from_slice(&hash);

        let padding = 64 - (value.len() % 64);

        append.file.write_all(&header)?;
        append.file.write_all(&value)?;
        append.file.write_all(&[0; 64][0..padding])?;

        let old_offset = append.length;
        append.length = old_offset + 64 + value.len() + padding;

        let written_bytes = unsafe {
            let written_slice =
                slice_from_raw_parts(self.mmap.as_ptr().offset(old_offset as _), value.len())
                    .as_ref()
                    .unwrap();
            Bytes::from_raw_parts(written_slice, self.mmap.clone())
        };

        let mut index = self.index.write()?;
        index.insert(
            hash,
            Mutex::new(IndexEntry {
                state: ValidationState::Validated,
                bytes: written_bytes.clone(),
            }),
        );

        // TODO: do we want to introduce a paranoid mode here ^,
        // this would be Unvalidated, and we would validate on first access

        Ok((hash, written_bytes))
    }

    pub fn get(&self, hash: &[u8; 32]) -> Result<Option<Bytes>, GetError> {
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
                let computed_hash: [u8; 32] = Blake3::digest(&entry.bytes).into();
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

#[cfg(test)]
mod tests {
    use std::ptr::slice_from_raw_parts;

    use super::*;

    use tempfile::tempfile;

    #[test]
    fn it_works() {
        const MAX_PILE_SIZE: usize = 1 << 40;

        let tmp = tempfile().unwrap();
        let mmap = MmapOptions::new()
            .len(MAX_PILE_SIZE)
            .map_raw_read_only(&tmp)
            .unwrap();

        let slice = unsafe { slice_from_raw_parts(mmap.as_ptr(), 9).as_ref().unwrap() };
        let bytes = Bytes::from(&slice[..]);

        assert_eq!(b"# memmap2", &bytes[..]);
    }
}
