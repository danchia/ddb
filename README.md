# Daniel's Distributed Database (DDB)

## Overview

This is code for my learning project *Writing A Distributed Database*. The project is meant to solidify my understanding of distributed databases, and is by no means production ready.

Those looking for production-quality code should probably look to projects like [etcd](https://github.com/coreos/etcd) instead.

## Medium Posts

I will be documenting my thoughts and reflections as I go along on Medium.

1. [Learning By Doing](https://medium.com/@daniel.chia/writing-a-database-learning-by-doing-72480647b978)
2. [Skeleton Implementation](https://medium.com/@daniel.chia/ddb-part-1-skeleton-implementation-f92ccec3e8e4)
3. [Write Ahead Log](https://medium.com/@daniel.chia/writing-a-database-part-2-write-ahead-log-2463f5cec67a)

## Roadmap

### Storage Engine
 - [x] WAL rotation / truncation
 - [x] SSTable indexes
 - [ ] SSTable key compression
 - [ ] SSTable block compression
 - [ ] Bloom filters
 - [ ] Compactions
 - [ ] Block cache

### API
 - [ ] Scans
 - [ ] Transactions
 - [ ] Hybrid logical clocks (or some other timestamping system)

### Replication
 - [ ] Replicate vs Paxos or Raft.

### Debug / Monitoring
 - [ ] Add stats framework
 - [x] Add tracing framework (Census or other)
 - [ ] Add debug z pages.