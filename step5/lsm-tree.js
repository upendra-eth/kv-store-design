/**
 * ============================================================================
 * STEP 5: LSM Tree - The Complete Picture
 * ============================================================================
 * 
 * 🎯 LEARNING GOAL:
 * Understand the LSM (Log-Structured Merge) Tree architecture that powers
 * LevelDB, RocksDB, Cassandra, HBase, and many other modern databases.
 * 
 * 📚 KEY CONCEPTS:
 * 
 * 1. THE PROBLEM
 *    After Steps 1-4, we have:
 *    - MemTable (in-memory, sorted)
 *    - WAL (for durability)
 *    - SSTables (on disk, sorted, immutable)
 *    
 *    But what happens when we have MANY SSTables?
 *    - Reading becomes slow: must check each SSTable
 *    - Disk space grows: deleted keys still exist in old SSTables
 *    - Key ranges overlap: same key might be in multiple files
 * 
 * 2. THE LSM TREE SOLUTION
 *    Organize SSTables into LEVELS:
 *    
 *    ┌─────────────────────────────────────────────────────────┐
 *    │  MemTable (4MB) - Active writes                         │
 *    ├─────────────────────────────────────────────────────────┤
 *    │  Level 0: Up to 4 SSTables (may overlap!)               │
 *    ├─────────────────────────────────────────────────────────┤
 *    │  Level 1: Up to 10 SSTables (non-overlapping)           │
 *    ├─────────────────────────────────────────────────────────┤
 *    │  Level 2: Up to 100 SSTables (non-overlapping)          │
 *    ├─────────────────────────────────────────────────────────┤
 *    │  Level 3: Up to 1000 SSTables (non-overlapping)         │
 *    └─────────────────────────────────────────────────────────┘
 * 
 * 3. READ PATH
 *    To find a key:
 *    1. Check MemTable (newest data)
 *    2. Check Level 0 SSTables (newest to oldest)
 *    3. Check Level 1, 2, 3... (each level has non-overlapping keys)
 *    
 *    Stop at first match! Newer data always wins.
 * 
 * 4. COMPACTION - The Heart of LSM Trees
 *    When a level gets too full:
 *    1. Pick SSTables to compact
 *    2. Merge them together (like merge sort!)
 *    3. Write to next level
 *    4. Delete old SSTables
 *    
 *    This is how we:
 *    - Remove tombstones (actually delete data)
 *    - Merge duplicate keys (keep newest only)
 *    - Maintain sorted, non-overlapping structure
 * 
 * 🔗 HOW THIS RELATES TO LEVELDB:
 *    This IS LevelDB's architecture!
 *    - "LevelDB" is named after these "levels"
 *    - RocksDB adds more tuning options
 *    - Cassandra uses similar "Tiered" or "Leveled" compaction
 */

const fs = require('fs');
const path = require('path');
const { MemTable } = require('../step3/memtable');
const { SSTableWriter } = require('../step4/sstable-writer');
const { SSTableReader, TOMBSTONE } = require('../step4/sstable-reader');
const { WriteAheadLog } = require('../step2/wal');

class LSMTree {
    /**
     * @param {string} dataDir - Directory for all data files
     * @param {object} options - Configuration options
     */
    constructor(dataDir, options = {}) {
        this.dataDir = dataDir;

        // Configuration (LevelDB-like defaults)
        this.memtableMaxSize = options.memtableMaxSize || 4 * 1024 * 1024; // 4MB
        this.level0MaxFiles = options.level0MaxFiles || 4;
        this.levelSizeMultiplier = options.levelSizeMultiplier || 10;
        this.maxLevels = options.maxLevels || 7;

        // Create directories
        if (!fs.existsSync(dataDir)) {
            fs.mkdirSync(dataDir, { recursive: true });
        }

        // Initialize components
        this.wal = new WriteAheadLog(path.join(dataDir, 'wal.log'));
        this.memtable = new MemTable(this.memtableMaxSize);

        // Track SSTables per level
        // levels[0] = array of Level 0 SSTables (may overlap)
        // levels[1] = array of Level 1 SSTables (non-overlapping)
        // etc.
        this.levels = [];
        for (let i = 0; i < this.maxLevels; i++) {
            this.levels.push([]);
        }

        // SSTable counter for unique filenames
        this.sstableCounter = 0;

        // Recover from existing data
        this._recover();

        console.log(`🌳 LSM Tree initialized at ${dataDir}`);
    }

    /**
     * SET a key-value pair
     * 
     * Write path:
     * 1. Write to WAL (durability)
     * 2. Write to MemTable
     * 3. If MemTable full, flush to Level 0
     */
    set(key, value) {
        if (typeof key !== 'string') {
            throw new Error('Key must be a string');
        }

        // 1. WAL first for durability
        this.wal.logSet(key, value);

        // 2. Write to MemTable
        const shouldFlush = this.memtable.set(key, value);

        // 3. Flush if needed
        if (shouldFlush) {
            this._flushMemtable();
        }

        return value;
    }

    /**
     * DELETE a key
     * 
     * Writes a tombstone marker.
     */
    delete(key) {
        this.wal.logDelete(key);
        const shouldFlush = this.memtable.delete(key);

        if (shouldFlush) {
            this._flushMemtable();
        }

        return true;
    }

    /**
     * GET a value
     * 
     * Read path (check in order, newest wins):
     * 1. MemTable
     * 2. Level 0 (newest to oldest, may have overlaps)
     * 3. Level 1, 2, 3... (binary search, no overlaps)
     */
    get(key) {
        // 1. Check MemTable first (newest data)
        const memValue = this.memtable.get(key);
        if (memValue !== undefined) {
            if (memValue === MemTable.TOMBSTONE) {
                return undefined; // Key was deleted
            }
            return memValue;
        }

        // 2. Check Level 0 (newest to oldest)
        // Level 0 SSTables may have overlapping key ranges!
        for (let i = this.levels[0].length - 1; i >= 0; i--) {
            const sstable = this.levels[0][i];
            const value = sstable.get(key);
            if (value !== undefined) {
                if (value === TOMBSTONE) {
                    return undefined;
                }
                return value;
            }
        }

        // 3. Check other levels (non-overlapping, so one file per level max)
        for (let level = 1; level < this.maxLevels; level++) {
            for (const sstable of this.levels[level]) {
                // Quick range check (optimization)
                const stats = sstable.stats();
                if (key >= stats.minKey && key <= stats.maxKey) {
                    const value = sstable.get(key);
                    if (value !== undefined) {
                        if (value === TOMBSTONE) {
                            return undefined;
                        }
                        return value;
                    }
                }
            }
        }

        return undefined;
    }

    /**
     * RANGE QUERY
     * 
     * Must merge results from all levels!
     */
    range(startKey, endKey) {
        const results = new Map();

        // Collect from all levels (oldest to newest, so newer overwrites)
        // Start from deepest level and work up
        for (let level = this.maxLevels - 1; level >= 0; level--) {
            for (const sstable of this.levels[level]) {
                for (const { key, value } of sstable.range(startKey, endKey)) {
                    results.set(key, value);
                }
            }
        }

        // MemTable last (newest)
        for (const { key, value } of this.memtable.range(startKey, endKey)) {
            results.set(key, value);
        }

        // Filter out tombstones and sort
        const output = [];
        for (const [key, value] of results) {
            if (value !== TOMBSTONE && value !== MemTable.TOMBSTONE) {
                output.push({ key, value });
            }
        }

        return output.sort((a, b) => a.key.localeCompare(b.key));
    }

    /**
     * FLUSH MEMTABLE TO LEVEL 0
     * 
     * Called when MemTable is full.
     * @private
     */
    _flushMemtable() {
        console.log('\n💾 Flushing MemTable to Level 0...');

        // Create new SSTable
        const sstablePath = path.join(
            this.dataDir,
            `level0_${this.sstableCounter++}.sst`
        );

        const writer = new SSTableWriter(sstablePath);
        writer.write(this.memtable.entries());

        // Add reader to Level 0
        const reader = new SSTableReader(sstablePath);
        this.levels[0].push(reader);

        // Create new MemTable
        this.memtable = new MemTable(this.memtableMaxSize);

        // Clear WAL (MemTable data is now on disk)
        this.wal.close();
        fs.unlinkSync(path.join(this.dataDir, 'wal.log'));
        this.wal = new WriteAheadLog(path.join(this.dataDir, 'wal.log'));

        // Check if compaction needed
        if (this.levels[0].length >= this.level0MaxFiles) {
            this._compactLevel(0);
        }
    }

    /**
     * COMPACT A LEVEL
     * 
     * This is the key operation of LSM Trees!
     * Merges SSTables from level N into level N+1.
     * 
     * @private
     */
    _compactLevel(level) {
        console.log(`\n🔧 Compacting Level ${level}...`);

        if (level >= this.maxLevels - 1) {
            console.log('   Already at max level, skipping');
            return;
        }

        // Collect all entries from this level
        const allEntries = new Map();
        const oldSstables = [...this.levels[level]];

        for (const sstable of oldSstables) {
            for (const { key, value } of sstable) {
                allEntries.set(key, value);
            }
        }

        // Same for next level (we're merging into it)
        const nextLevelOldSstables = [...this.levels[level + 1]];
        for (const sstable of nextLevelOldSstables) {
            for (const { key, value } of sstable) {
                // Only add if not already present (current level is newer)
                if (!allEntries.has(key)) {
                    allEntries.set(key, value);
                }
            }
        }

        // Sort and filter tombstones at deepest level
        const entries = [];
        for (const [key, value] of allEntries) {
            // At deepest level, we can actually remove tombstones
            if (level + 1 === this.maxLevels - 1 && value === TOMBSTONE) {
                continue;
            }
            entries.push({ key, value });
        }
        entries.sort((a, b) => a.key.localeCompare(b.key));

        // Write new SSTable at next level
        const sstablePath = path.join(
            this.dataDir,
            `level${level + 1}_${this.sstableCounter++}.sst`
        );

        if (entries.length > 0) {
            const writer = new SSTableWriter(sstablePath);
            writer.write(entries);
            const reader = new SSTableReader(sstablePath);
            this.levels[level + 1] = [reader];
        } else {
            this.levels[level + 1] = [];
        }

        // Clear old level
        this.levels[level] = [];

        // Delete old SSTable files
        for (const sstable of oldSstables) {
            fs.unlinkSync(sstable.filePath);
        }
        for (const sstable of nextLevelOldSstables) {
            fs.unlinkSync(sstable.filePath);
        }

        console.log(`   Merged ${oldSstables.length + nextLevelOldSstables.length} files → 1 file at Level ${level + 1}`);
    }

    /**
     * RECOVER FROM EXISTING DATA
     * 
     * On startup:
     * 1. Scan for existing SSTable files
     * 2. Replay WAL into MemTable
     * 
     * @private
     */
    _recover() {
        // Find existing SSTables
        if (fs.existsSync(this.dataDir)) {
            const files = fs.readdirSync(this.dataDir);
            for (const file of files) {
                if (file.endsWith('.sst')) {
                    const match = file.match(/level(\d+)_(\d+)\.sst/);
                    if (match) {
                        const level = parseInt(match[1]);
                        const counter = parseInt(match[2]);

                        const reader = new SSTableReader(path.join(this.dataDir, file));
                        this.levels[level].push(reader);

                        if (counter >= this.sstableCounter) {
                            this.sstableCounter = counter + 1;
                        }
                    }
                }
            }
        }

        // Replay WAL
        const walData = this.wal.recover();
        for (const [key, value] of walData) {
            if (typeof value === 'symbol') {
                this.memtable.delete(key);
            } else {
                this.memtable.set(key, value);
            }
        }
    }

    /**
     * GET STATS
     */
    stats() {
        const levelStats = this.levels.map((level, i) => ({
            level: i,
            sstables: level.length,
            entries: level.reduce((sum, sst) => sum + sst.stats().entryCount, 0),
        }));

        return {
            memtable: this.memtable.stats(),
            levels: levelStats.filter(l => l.sstables > 0),
            totalSSTables: this.levels.reduce((sum, l) => sum + l.length, 0),
        };
    }

    /**
     * CLOSE - Clean shutdown
     */
    close() {
        this.wal.close();
        console.log('👋 LSM Tree closed');
    }
}

module.exports = { LSMTree };
