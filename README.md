# trible-pile
a tiny mmap-ed on-disk blob store

> Like LMDB, no DB,
> Key-value store, no key,
> Just a blob store, see.
- Me & ChatGPT

## What?

A `trible_pile` is a collection of blobs stored in a file.

The file is memory-mapped for very fast reads, and written to with an append-only `write` system call.

The blobs are stored in a very simple format: a magic number, some padding, a length of the blob, the bytes of the blob, and some padding again.

The padding is to ensure that all blobs are 64-byte aligned, so that they can be deserialized via `zerocopy`.

Every entry has between 64 and 127 bytes of overhead, depending on the length of the blob.

## Why?

I wanted a _simple_ way to store blobs in a single file. I didn't need a key-value store, I just needed a blob store.

I wanted the ability to memory-map the file, so that I could read the deserialized blobs directly from the file, without having to copy them into memory.

File systems and disks are notoriously bad at persistence and durability, so I wanted to keep the writes as simple as possible. I didn't want to have to worry about corruption or data loss, so everything is append-only, and (lazily) validated on (first) read.

## But don't you know that `mmap` is bad?

Yes, I've read ["Are You Sure You Want to Use MMAP in Your Database Management System?"](https://db.cs.cmu.edu/mmap-cidr2022/).

Look, I respect the folks that wrote it tremendously, and I'm not saying that they're wrong. But I'm not using `mmap` for the reasons that they say it's bad. I'm not using it for performance, I'm using it for simplicity. I'm not using it for durability, I'm using it for convenience. I'm not using it for throughput, I'm using it for low latency, and lazy zero-copy deserialization.

- I'm not building a database management system, I'm building a blob store.
- I'm not building a high-performance system, I'm building a simple system.
- I'm not building a distributed system, I'm building a single-node system.