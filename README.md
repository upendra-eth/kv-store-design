# KV Database Engine Demo 🚀

A step-by-step implementation of a key-value database engine like LevelDB, built to learn database internals.

## Quick Start

```bash
# Run the complete demo
node step5/demo.js

# Or run individual steps
node step1/store.js      # In-Memory Store
node step2/store.js      # WAL + Durability
node step3/skiplist.js   # Skip List
node step3/memtable.js   # MemTable
node step4/sstable-reader.js  # SSTable Read/Write
```

## Learning Path

| Step | Concept | File | What You'll Learn |
|------|---------|------|-------------------|
| 1 | In-Memory Store | `step1/store.js` | HashMap, O(1) operations |
| 2 | Write-Ahead Log | `step2/wal.js` | Durability, crash recovery |
| 3 | MemTable | `step3/skiplist.js` | Skip List, O(log n) sorted ops |
| 4 | SSTable | `step4/sstable-*.js` | Disk persistence, binary search |
| 5 | LSM Tree | `step5/lsm-tree.js` | Levels, compaction |

## Architecture

```
               ┌─────────────────────┐
   Write  ───→ │     MemTable        │ ←── Read (check first)
               │   (Skip List)       │
               └──────────┬──────────┘
                          │ flush when full
               ┌──────────▼──────────┐
               │   Level 0 SSTables  │ ←── Read (check second)
               └──────────┬──────────┘
                          │ compact when too many
               ┌──────────▼──────────┐
               │   Level 1 SSTables  │ ←── Read (check third)
               └──────────┬──────────┘
                          │
                         ...
```

## Key Concepts

- **WAL**: Write-Ahead Log ensures every write is durable before returning
- **MemTable**: In-memory sorted buffer using Skip List
- **SSTable**: Sorted String Table - immutable sorted file on disk
- **LSM Tree**: Log-Structured Merge Tree - organizes SSTables into levels
- **Compaction**: Merge SSTables to reclaim space and maintain performance

## Further Reading

- [LevelDB Documentation](https://github.com/google/leveldb)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [LSM Tree Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
