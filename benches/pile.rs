use anybytes::Bytes;
pub use blake3::Hasher as Blake3;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use digest::Digest;
use rand::RngCore;
use tempfile::TempDir;
use trible_pile::{Hash, Pile};

fn pile(c: &mut Criterion) {
    const RECORD_LEN: usize = 1 << 10; // 1k
    const RECORD_COUNT: usize = 1 << 20; // 1M
    const MAX_PILE_SIZE: usize = 1 << 35; // 100GB

    let mut group = c.benchmark_group("pile");
    group.sample_size(10);

    group.throughput(Throughput::Bytes(RECORD_COUNT as u64 * RECORD_LEN as u64));
    group.bench_function(BenchmarkId::new("insert_validated", RECORD_COUNT), |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();
                (0..RECORD_COUNT)
                    .map(|_| {
                        let mut record = vec![0u8; RECORD_LEN];
                        rng.fill_bytes(&mut record);

                        Bytes::from_source(record)
                    })
                    .collect()
            },
            |data: Vec<Bytes>| {
                let tmp_dir = tempfile::tempdir().unwrap();
                let tmp_pile = tmp_dir.path().join("test.pile");
                let mut pile: Pile<MAX_PILE_SIZE> = Pile::load(&tmp_pile).unwrap();
                data.iter().for_each(|data| {
                    pile.insert(data).unwrap();
                });
            },
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Bytes(RECORD_COUNT as u64 * RECORD_LEN as u64));
    group.bench_function(BenchmarkId::new("insert_unvalidated", RECORD_COUNT), |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();
                (0..RECORD_COUNT)
                    .map(|_| {
                        let mut record = vec![0u8; RECORD_LEN];
                        rng.fill_bytes(&mut record);

                        let bytes = Bytes::from_source(record);

                        let hash: Hash = Blake3::digest(&bytes).into();

                        (hash, bytes)
                    })
                    .collect()
            },
            |data: Vec<(Hash, Bytes)>| {
                let tmp_dir = tempfile::tempdir().unwrap();
                let tmp_pile = tmp_dir.path().join("test.pile");
                let mut pile: Pile<MAX_PILE_SIZE> = Pile::load(&tmp_pile).unwrap();
                data.iter().for_each(|(hash, data)| {
                    pile.insert_unvalidated(*hash, data).unwrap();
                });
            },
            BatchSize::PerIteration,
        );
    });

    const FLUSHED_RECORD_COUNT: usize = 1 << 10; // 1k
    group.throughput(Throughput::Bytes(FLUSHED_RECORD_COUNT as u64 * 1000 as u64));
    group.bench_function(BenchmarkId::new("insert flushed", RECORD_COUNT), |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();
                (0..FLUSHED_RECORD_COUNT)
                    .map(|_| {
                        let mut record = vec![0u8; RECORD_LEN];
                        rng.fill_bytes(&mut record);

                        Bytes::from_source(record)
                    })
                    .collect()
            },
            |data: Vec<Bytes>| {
                let tmp_dir = tempfile::tempdir().unwrap();
                let tmp_pile = tmp_dir.path().join("test.pile");
                let mut pile: Pile<MAX_PILE_SIZE> = Pile::load(&tmp_pile).unwrap();
                data.iter().for_each(|data| {
                    pile.insert(data).unwrap();
                    pile.flush().unwrap();
                });
            },
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Bytes(RECORD_COUNT as u64 * RECORD_LEN as u64));
    group.bench_function(BenchmarkId::new("load", RECORD_COUNT), |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();
                let tmp_dir = tempfile::tempdir().unwrap();
                let tmp_pile = tmp_dir.path().join("test.pile");
                let mut pile: Pile<MAX_PILE_SIZE> = Pile::load(&tmp_pile).unwrap();

                (0..RECORD_COUNT).for_each(|_| {
                    let mut record = vec![0u8; RECORD_LEN];
                    rng.fill_bytes(&mut record);

                    let data = Bytes::from_source(record);
                    pile.insert(&data).unwrap();
                });

                pile.flush().unwrap();

                tmp_dir
            },
            |tmp_dir: TempDir| {
                let tmp_pile = tmp_dir.path().join("test.pile");
                let _pile: Pile<MAX_PILE_SIZE> = Pile::load(&tmp_pile).unwrap();
                drop(tmp_dir)
            },
            BatchSize::PerIteration,
        );
    });

    /*
    group.throughput(Throughput::Elements(1));
    group.bench_function("read random records", |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();

                let tmp = tempfile::tempdir().unwrap();
                let db = Database::file(tmp.path()).unwrap();

                let records: Vec<_> = (0..RECORD_COUNT)
                    .map(|_| {
                        let mut record = vec![0u8; RECORD_LEN];
                        rng.fill_bytes(&mut record);

                        record
                    }).collect();
                let records: Vec<_> = records.iter().map(|data| data.as_ref()).collect();
                db.append(&records).unwrap();


                db
            },
            |db| {
                let mut rng = rand::thread_rng();

                let i = (rng.next_u64() as usize) % RECORD_COUNT;
                let _record = db.get_by_seqno(i).unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.throughput(Throughput::Elements(RECORD_COUNT as u64));
    group.bench_function("read consecutive records", |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();

                let tmp = tempfile::tempdir().unwrap();
                let pile = Pile::load(tmp.path()).unwrap();

                let records: Vec<_> = (0..RECORD_COUNT)
                    .map(|_| {
                        let mut record = vec![0u8; RECORD_LEN];
                        rng.fill_bytes(&mut record);

                        record
                    }).collect();
                let records: Vec<_> = records.iter().map(|data| data.as_ref()).collect();
                pile.insert(&records).unwrap();

                pile
            },
            |db| {
                let _maybe_record = db.iter_from_seqno(0).unwrap().for_each(|e| {
                    black_box(e);
                });
            },
            BatchSize::LargeInput,
        );
    });

    */
}

criterion_group!(benches, pile);
criterion_main!(benches);
