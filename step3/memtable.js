/**
 * ============================================================================
 * STEP 3: MemTable - The Write Buffer
 * ============================================================================
 * 
 * 🎯 LEARNING GOAL:
 * Understand how MemTable works as a write buffer with size limits.
 * 
 * 📚 KEY CONCEPTS:
 * 
 * 1. WHAT IS A MEMTABLE?
 *    - An in-memory sorted buffer for incoming writes
 *    - Uses Skip List internally for fast, sorted operations
 *    - Has a size limit (e.g., 4MB in LevelDB)
 *    - When full, it's flushed to disk as an SSTable
 * 
 * 2. WHY BUFFER WRITES?
 *    Direct disk writes for every operation would be too slow:
 *    - Random writes on disk: ~5ms each
 *    - Sequential writes (SSTable): ~0.05ms per entry
 *    
 *    By buffering many writes and flushing together,
 *    we convert random writes into sequential writes!
 * 
 * 3. THE FLOW
 *    ┌────────────┐     ┌────────────┐     ┌────────────┐
 *    │   Write    │ ──→ │  MemTable  │ ──→ │  SSTable   │
 *    │  Request   │     │ (in memory)│     │  (on disk) │
 *    └────────────┘     └────────────┘     └────────────┘
 *                            │
 *                       When full!
 */

const { SkipList } = require('./skiplist');

class MemTable {
    /**
     * Create a new MemTable
     * 
     * @param {number} maxSizeBytes - Maximum size before flush (default: 4MB)
     * 
     * LevelDB default: 4MB
     * RocksDB allows tuning this based on workload
     */
    constructor(maxSizeBytes = 4 * 1024 * 1024) {
        this.skipList = new SkipList();
        this.maxSizeBytes = maxSizeBytes;
        this.currentSizeBytes = 0;
    }

    /**
     * SET a key-value pair
     * 
     * @param {string} key
     * @param {any} value
     * @returns {boolean} True if memtable should be flushed
     */
    set(key, value) {
        // Estimate size of this entry
        const entrySize = this._estimateSize(key, value);

        // Check if already exists (update vs insert)
        const existingValue = this.skipList.get(key);
        if (existingValue !== undefined) {
            // Remove old size estimate
            this.currentSizeBytes -= this._estimateSize(key, existingValue);
        }

        // Insert into skip list
        this.skipList.set(key, value);
        this.currentSizeBytes += entrySize;

        // Return true if we've exceeded size limit
        return this.shouldFlush();
    }

    /**
     * DELETE a key (using tombstone marker)
     * 
     * 🤔 IMPORTANT: We don't actually delete!
     * 
     * In a MemTable, a "delete" is recorded as a special TOMBSTONE value.
     * Why? Because:
     * 1. The key might exist in older SSTables on disk
     * 2. We need to remember "this key is deleted"
     * 3. The actual cleanup happens during compaction (Step 5)
     * 
     * LevelDB uses a special value type to mark deletions.
     * We'll use a special symbol for simplicity.
     */
    delete(key) {
        // Use a symbol as tombstone marker
        return this.set(key, MemTable.TOMBSTONE);
    }

    /**
     * GET a value
     * 
     * @param {string} key
     * @returns {any} Value, TOMBSTONE (means deleted), or undefined (not found)
     */
    get(key) {
        return this.skipList.get(key);
    }

    /**
     * RANGE QUERY
     * 
     * @param {string} startKey
     * @param {string} endKey
     * @returns {Array} Entries in range (may include tombstones!)
     */
    range(startKey, endKey) {
        return this.skipList.range(startKey, endKey);
    }

    /**
     * CHECK if memtable should be flushed
     * 
     * When true, the caller should:
     * 1. Convert this memtable to "immutable"
     * 2. Start flushing to SSTable
     * 3. Create a new empty memtable for new writes
     */
    shouldFlush() {
        return this.currentSizeBytes >= this.maxSizeBytes;
    }

    /**
     * GET ALL ENTRIES for flushing to SSTable
     * 
     * Returns entries in sorted order - perfect for SSTable!
     */
    *entries() {
        yield* this.skipList;
    }

    /**
     * ESTIMATE SIZE of an entry in bytes
     * 
     * This is a rough estimate. Real databases track this more precisely.
     * 
     * @private
     */
    _estimateSize(key, value) {
        // Key size (2 bytes per char in JS)
        let size = key.length * 2;

        // Value size (rough JSON estimate)
        if (value === MemTable.TOMBSTONE) {
            size += 8; // Fixed size for tombstone
        } else if (typeof value === 'string') {
            size += value.length * 2;
        } else {
            // For objects, estimate via JSON
            size += JSON.stringify(value).length * 2;
        }

        // Overhead for skip list node (pointers, etc.)
        size += 64;

        return size;
    }

    /**
     * GET STATS
     */
    stats() {
        return {
            entries: this.skipList.size,
            sizeBytes: this.currentSizeBytes,
            maxSizeBytes: this.maxSizeBytes,
            usage: ((this.currentSizeBytes / this.maxSizeBytes) * 100).toFixed(1) + '%',
        };
    }
}

// Special marker for deleted keys
MemTable.TOMBSTONE = Symbol('TOMBSTONE');

module.exports = { MemTable };

/**
 * ============================================================================
 * 🧪 DEMO
 * ============================================================================
 */
if (require.main === module) {
    console.log('🚀 Step 3: MemTable Demo\n');
    console.log('='.repeat(50));

    // Create a small memtable (1KB limit for demo)
    const memtable = new MemTable(1024);

    console.log('\n📝 Writing entries until memtable is full...\n');

    let i = 0;
    while (!memtable.shouldFlush() && i < 100) {
        const key = `user:${String(i).padStart(3, '0')}`;
        const value = { name: `User ${i}`, data: 'x'.repeat(20) };

        memtable.set(key, value);
        i++;

        if (i % 5 === 0) {
            console.log(`   Added ${i} entries, stats:`, memtable.stats());
        }
    }

    console.log('\n⚠️ MemTable full! Should flush:', memtable.shouldFlush());

    // Demonstrate the key-value functionality
    console.log('\n📖 Reading entries:');
    console.log('   get("user:000") =>', memtable.get('user:000'));
    console.log('   get("user:010") =>', memtable.get('user:010'));

    // Demonstrate delete (tombstone)
    console.log('\n🗑️ Deleting user:005...');
    memtable.delete('user:005');
    const deleted = memtable.get('user:005');
    console.log('   get("user:005") =>', deleted === MemTable.TOMBSTONE ? 'TOMBSTONE (deleted!)' : deleted);

    // Range query
    console.log('\n🎯 Range query (user:010 to user:015):');
    const rangeResults = memtable.range('user:010', 'user:015');
    for (const { key, value } of rangeResults) {
        console.log(`   ${key} => ${value === MemTable.TOMBSTONE ? 'TOMBSTONE' : JSON.stringify(value)}`);
    }

    console.log('\n' + '='.repeat(50));
    console.log('✅ Step 3 Complete!\n');
    console.log('📚 What you learned:');
    console.log('   - MemTable buffers writes in sorted order');
    console.log('   - Size limits trigger flush to disk');
    console.log('   - Deletes are tombstones (not actual removal)');
    console.log('   - Range queries work on sorted data');
    console.log('\n⚠️ Next: Step 4 will flush this to SSTable on disk!');
}
