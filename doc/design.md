# Design

## Data Model

Key value store. Keys are UTF8 strings, values are arbitrary bytes.

## Properties

* Consistent. Read your own writes.
* Durable. Mutations that have been acknowledged should not be lost.

## Limits

* Max key size of 4KiB.
* Max value size of 512KiB.

## Endian-ness

Unless otherwise stated, all on-disk formats use little-endian encoding.

## Write Ahead Log

The write ahead log consists of log segments. Each log segment consists of a
sequence of records:

```
record:
  length: uint32      // length of data section
  checksum: uint32    // CRC32 checksum of data
  data: byte[length]  // serialized proto
```

The `data` portion of a record is a serialized `ddb.internal.LogRecord` proto.

At any given time, there will be exactly one log segment that is being appended
to. All other segments are purely for data recovery purposes.
