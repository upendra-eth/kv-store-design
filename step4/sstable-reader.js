/**
 * ============================================================================
 * STEP 4: SSTable Reader - Reading from Disk Efficiently
 * ============================================================================
 * 
 * 🎯 LEARNING GOAL:
 * Understand how to read sorted data from disk with binary search.
 * 
 * 📚 KEY CONCEPTS:
 * 
 * 1. READING STRATEGY
 *    We DON'T load the entire file into memory!
 *    Instead:
 *    1. Load footer (small, fixed position at end)
 *    2. Load index block (medium, one per SSTable)
 *    3. Load only the data block we need (on-demand)
 * 
 * 2. BINARY SEARCH ON INDEX
 *    The index block tells us which data block contains our key.
 *    Since keys are sorted, we can binary search the index!
 *    
 *    Example:
 *    Index: [
 *      { startKey: "a", endKey: "m", offset: 0 },
 *      { startKey: "n", endKey: "z", offset: 4096 },
 *    ]
 *    
 *    Looking for "user"? → Between "n" and "z" → Read block at offset 4096
 * 
 * 3. CACHING
 *    Real databases cache:
 *    - Index blocks (always in memory)
 *    - Frequently accessed data blocks
 *    - Bloom filters (to skip SSTables that don't have the key)
 *    
 *    We keep it simple: cache the index, read blocks on demand.
 */

const fs = require('fs');

// Tombstone marker
const TOMBSTONE = Symbol.for('TOMBSTONE');

/**
 * SSTable Reader
 * 
 * Efficiently reads key-value pairs from an SSTable file.
 */
class SSTableReader {
    /**
     * @param {string} filePath - Path to the SSTable file
     */
    constructor(filePath) {
        this.filePath = filePath;
        this.footer = null;
        this.index = null;

        // Load metadata on construction
        this._loadMetadata();
    }

    /**
     * LOAD METADATA (Footer + Index)
     * 
     * This is done once when the reader is created.
     * The index stays in memory for fast lookups.
     * 
     * @private
     */
    _loadMetadata() {
        const fd = fs.openSync(this.filePath, 'r');
        const stats = fs.fstatSync(fd);
        const fileSize = stats.size;

        // Read footer size (last 4 bytes)
        const footerSizeBuffer = Buffer.alloc(4);
        fs.readSync(fd, footerSizeBuffer, 0, 4, fileSize - 4);
        const footerSize = footerSizeBuffer.readUInt32LE();

        // Read footer
        const footerBuffer = Buffer.alloc(footerSize);
        fs.readSync(fd, footerBuffer, 0, footerSize, fileSize - 4 - footerSize);
        this.footer = JSON.parse(footerBuffer.toString());

        // Read index block
        const indexBuffer = Buffer.alloc(this.footer.indexSize);
        fs.readSync(fd, indexBuffer, 0, this.footer.indexSize, this.footer.indexOffset);
        this.index = JSON.parse(indexBuffer.toString());

        fs.closeSync(fd);
    }

    /**
     * GET a value by key
     * 
     * @param {string} key - Key to look up
     * @returns {any} Value, undefined if not found, or TOMBSTONE if deleted
     * 
     * Time complexity: O(log B + log E)
     * where B = number of blocks, E = entries per block
     */
    get(key) {
        // Quick check: is key in our range?
        if (key < this.footer.minKey || key > this.footer.maxKey) {
            return undefined;
        }

        // Find the right block using binary search on index
        const blockIndex = this._findBlockIndex(key);
        if (blockIndex === -1) {
            return undefined;
        }

        // Load and search that block
        const blockInfo = this.index[blockIndex];
        const entries = this._loadBlock(blockInfo);

        // Binary search within the block
        const entry = this._binarySearchBlock(entries, key);

        if (entry === null) {
            return undefined;
        }

        // Convert tombstone marker back to symbol
        if (entry.value === '__TOMBSTONE__') {
            return TOMBSTONE;
        }

        return entry.value;
    }

    /**
     * FIND BLOCK INDEX using binary search
     * 
     * Returns the index of the block that might contain the key.
     * 
     * @private
     */
    _findBlockIndex(key) {
        let left = 0;
        let right = this.index.length - 1;
        let result = -1;

        while (left <= right) {
            const mid = Math.floor((left + right) / 2);
            const block = this.index[mid];

            if (key >= block.startKey && key <= block.endKey) {
                return mid; // Key is definitely in this block
            } else if (key < block.startKey) {
                right = mid - 1;
            } else {
                result = mid; // Key might be in a later block
                left = mid + 1;
            }
        }

        return result;
    }

    /**
     * LOAD A DATA BLOCK from disk
     * 
     * In a real database, this would use caching!
     * 
     * @private
     */
    _loadBlock(blockInfo) {
        const fd = fs.openSync(this.filePath, 'r');
        const buffer = Buffer.alloc(blockInfo.size);
        fs.readSync(fd, buffer, 0, blockInfo.size, blockInfo.offset);
        fs.closeSync(fd);

        return JSON.parse(buffer.toString());
    }

    /**
     * BINARY SEARCH within a block
     * 
     * @private
     */
    _binarySearchBlock(entries, key) {
        let left = 0;
        let right = entries.length - 1;

        while (left <= right) {
            const mid = Math.floor((left + right) / 2);
            const entry = entries[mid];

            if (entry.key === key) {
                return entry;
            } else if (entry.key < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return null;
    }

    /**
     * RANGE QUERY
     * 
     * @param {string} startKey
     * @param {string} endKey
     * @returns {Array} Entries in range
     */
    range(startKey, endKey) {
        const results = [];

        // Find starting block
        let blockIdx = this._findBlockIndex(startKey);
        if (blockIdx === -1) blockIdx = 0;

        // Iterate through blocks until we pass endKey
        while (blockIdx < this.index.length) {
            const blockInfo = this.index[blockIdx];

            // If block's start key is past our end, we're done
            if (blockInfo.startKey > endKey) {
                break;
            }

            // Load and filter entries
            const entries = this._loadBlock(blockInfo);
            for (const entry of entries) {
                if (entry.key >= startKey && entry.key <= endKey) {
                    const value = entry.value === '__TOMBSTONE__' ? TOMBSTONE : entry.value;
                    results.push({ key: entry.key, value });
                }
            }

            blockIdx++;
        }

        return results;
    }

    /**
     * ITERATE ALL ENTRIES
     * 
     * Yields all entries in sorted order.
     */
    *[Symbol.iterator]() {
        for (const blockInfo of this.index) {
            const entries = this._loadBlock(blockInfo);
            for (const entry of entries) {
                const value = entry.value === '__TOMBSTONE__' ? TOMBSTONE : entry.value;
                yield { key: entry.key, value };
            }
        }
    }

    /**
     * GET STATS
     */
    stats() {
        return {
            path: this.filePath,
            entryCount: this.footer.entryCount,
            blockCount: this.footer.blockCount,
            minKey: this.footer.minKey,
            maxKey: this.footer.maxKey,
        };
    }
}

module.exports = { SSTableReader, TOMBSTONE };

/**
 * ============================================================================
 * 🧪 DEMO: Complete SSTable read/write cycle
 * ============================================================================
 */
if (require.main === module) {
    const { SSTableWriter } = require('./sstable-writer');
    const path = require('path');

    console.log('🚀 Step 4: SSTable Demo\n');
    console.log('='.repeat(50));

    const dataDir = './step4-demo-data';
    const sstablePath = path.join(dataDir, 'test.sst');

    // Clean up
    if (fs.existsSync(dataDir)) {
        fs.rmSync(dataDir, { recursive: true });
    }

    // Create test data (simulating MemTable flush)
    console.log('\n📝 Creating test data (simulating MemTable)...');
    const entries = [];
    for (let i = 0; i < 100; i++) {
        const key = `user:${String(i).padStart(3, '0')}`;
        const value = i === 50 ? Symbol.for('TOMBSTONE') : { name: `User ${i}`, score: i * 10 };
        entries.push({ key, value });
    }
    console.log(`   Created ${entries.length} entries`);
    console.log(`   Key range: [${entries[0].key}, ${entries[entries.length - 1].key}]`);

    // Write SSTable
    console.log('\n📁 Writing SSTable...');
    const writer = new SSTableWriter(sstablePath, 1024); // Small blocks for demo
    writer.write(entries);

    // Read SSTable
    console.log('\n📖 Reading SSTable...');
    const reader = new SSTableReader(sstablePath);
    console.log('   Stats:', reader.stats());

    // Point queries
    console.log('\n🔍 Point Queries:');
    console.log('   get("user:000") =>', reader.get('user:000'));
    console.log('   get("user:050") =>', reader.get('user:050') === TOMBSTONE ? 'TOMBSTONE' : reader.get('user:050'));
    console.log('   get("user:099") =>', reader.get('user:099'));
    console.log('   get("user:999") =>', reader.get('user:999')); // Not found

    // Range query
    console.log('\n🎯 Range Query (user:010 to user:015):');
    const rangeResults = reader.range('user:010', 'user:015');
    for (const { key, value } of rangeResults) {
        console.log(`   ${key} => ${JSON.stringify(value)}`);
    }

    // Show file structure
    console.log('\n📊 SSTable Structure:');
    console.log(`   Total file size: ${fs.statSync(sstablePath).size} bytes`);
    console.log(`   Index entries: ${reader.index.length}`);
    for (let i = 0; i < Math.min(3, reader.index.length); i++) {
        const block = reader.index[i];
        console.log(`   Block ${i}: keys [${block.startKey} - ${block.endKey}], offset=${block.offset}, size=${block.size}`);
    }
    if (reader.index.length > 3) {
        console.log(`   ... (${reader.index.length - 3} more blocks)`);
    }

    console.log('\n' + '='.repeat(50));
    console.log('✅ Step 4 Complete!\n');
    console.log('📚 What you learned:');
    console.log('   - SSTable = Sorted String Table (immutable!)');
    console.log('   - Block-based organization for efficient reads');
    console.log('   - Binary search on index → O(log n) lookups');
    console.log('   - Footer contains metadata for quick access');
    console.log('\n⚠️ Problem: What if we have many SSTables?');
    console.log('   → Step 5 will add LSM Tree + Compaction!');

    // Cleanup
    fs.rmSync(dataDir, { recursive: true });
}
