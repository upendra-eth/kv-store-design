/**
 * ============================================================================
 * STEP 4: SSTable Writer - Flushing MemTable to Disk
 * ============================================================================
 * 
 * 🎯 LEARNING GOAL:
 * Understand how sorted data is persisted to disk efficiently.
 * 
 * 📚 KEY CONCEPTS:
 * 
 * 1. WHAT IS AN SSTABLE?
 *    SSTable = "Sorted String Table"
 *    - An immutable file containing sorted key-value pairs
 *    - Created by flushing a MemTable to disk
 *    - Never modified after creation (immutable!)
 *    - Enables efficient binary search on disk
 * 
 * 2. SSTABLE STRUCTURE
 *    ┌────────────────────────────────────────────────────┐
 *    │                    SSTable File                     │
 *    ├────────────────────────────────────────────────────┤
 *    │  Data Block 1: [key1:val1, key2:val2, ...]         │
 *    │  Data Block 2: [key3:val3, key4:val4, ...]         │
 *    │  ...                                                │
 *    │  Data Block N: [keyM:valM, ...]                    │
 *    ├────────────────────────────────────────────────────┤
 *    │  Index Block: [block1_start_key, offset],          │
 *    │               [block2_start_key, offset], ...      │
 *    ├────────────────────────────────────────────────────┤
 *    │  Footer: [index_offset, index_size, magic_number]  │
 *    └────────────────────────────────────────────────────┘
 * 
 * 3. WHY BLOCKS?
 *    - Reading entire file for one key is wasteful
 *    - Blocks allow us to load just the relevant portion
 *    - Typical block size: 4KB (matches disk sector size)
 *    - Index block tells us which data block to read
 * 
 * 4. BINARY SEARCH ON DISK
 *    To find key "user:500":
 *    1. Read footer → find index block location
 *    2. Read index block → binary search for correct data block
 *    3. Read data block → binary search for exact key
 *    
 *    Total disk reads: 2-3 (instead of scanning everything!)
 * 
 * 🔗 HOW THIS RELATES TO LEVELDB:
 *    - LevelDB's .ldb files are SSTables
 *    - Also has bloom filters (skip blocks that definitely don't have key)
 *    - Uses block compression (snappy) to save space
 */

const fs = require('fs');
const path = require('path');

// Tombstone marker - same as MemTable
const TOMBSTONE = Symbol.for('TOMBSTONE');

/**
 * SSTable Writer
 * 
 * Writes a MemTable to disk as an SSTable file.
 * The file format is designed for efficient reads.
 */
class SSTableWriter {
    /**
     * @param {string} filePath - Where to write the SSTable
     * @param {number} blockSize - Target size for data blocks (default: 4KB)
     */
    constructor(filePath, blockSize = 4096) {
        this.filePath = filePath;
        this.blockSize = blockSize;

        // Create directory if needed
        const dir = path.dirname(filePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
    }

    /**
     * WRITE ENTRIES TO SSTABLE
     * 
     * @param {Iterable} entries - Iterator of {key, value} pairs (must be sorted!)
     * @returns {object} Metadata about the written SSTable
     * 
     * This is the main method - converts in-memory data to disk format.
     */
    write(entries) {
        // Collect all entries into array (for our simple implementation)
        const allEntries = [];
        for (const entry of entries) {
            // Convert tombstone symbol to string for serialization
            const value = entry.value === Symbol.for('TOMBSTONE') ? '__TOMBSTONE__' : entry.value;
            allEntries.push({ key: entry.key, value });
        }

        if (allEntries.length === 0) {
            throw new Error('Cannot write empty SSTable');
        }

        // Build data blocks
        const dataBlocks = this._buildDataBlocks(allEntries);

        // Calculate final positions FIRST
        let currentOffset = 0;
        for (const block of dataBlocks) {
            block.offset = currentOffset;
            currentOffset += block.data.length;
        }
        const indexOffset = currentOffset;

        // Build index block AFTER offsets are set
        const indexEntries = dataBlocks.map(block => ({
            startKey: block.startKey,
            endKey: block.endKey,
            offset: block.offset,
            size: block.data.length,
        }));

        // Serialize index block
        const indexData = Buffer.from(JSON.stringify(indexEntries));
        currentOffset += indexData.length;

        // Create footer
        const footer = {
            indexOffset,
            indexSize: indexData.length,
            blockCount: dataBlocks.length,
            entryCount: allEntries.length,
            minKey: allEntries[0].key,
            maxKey: allEntries[allEntries.length - 1].key,
            magic: 'SSTABLE_V1',
        };
        const footerData = Buffer.from(JSON.stringify(footer));

        // Write everything to file
        const fd = fs.openSync(this.filePath, 'w');

        // Write data blocks
        for (const block of dataBlocks) {
            fs.writeSync(fd, block.data);
        }

        // Write index block
        fs.writeSync(fd, indexData);

        // Write footer THEN footer size (so reader can find size at end)
        fs.writeSync(fd, footerData);
        const footerSizeBuffer = Buffer.alloc(4);
        footerSizeBuffer.writeUInt32LE(footerData.length);
        fs.writeSync(fd, footerSizeBuffer);

        fs.closeSync(fd);

        console.log(`📁 Wrote SSTable: ${this.filePath}`);
        console.log(`   Entries: ${allEntries.length}`);
        console.log(`   Blocks: ${dataBlocks.length}`);
        console.log(`   Size: ${fs.statSync(this.filePath).size} bytes`);
        console.log(`   Key range: [${footer.minKey}, ${footer.maxKey}]`);

        return footer;
    }

    /**
     * BUILD DATA BLOCKS
     * 
     * Groups entries into blocks of ~blockSize bytes.
     * Each block contains multiple key-value pairs.
     * 
     * @private
     */
    _buildDataBlocks(entries) {
        const blocks = [];
        let currentBlockEntries = [];
        let currentBlockSize = 0;

        for (const entry of entries) {
            const entrySize = this._estimateEntrySize(entry);

            // If adding this entry would exceed block size, finalize current block
            if (currentBlockSize + entrySize > this.blockSize && currentBlockEntries.length > 0) {
                blocks.push(this._finalizeBlock(currentBlockEntries));
                currentBlockEntries = [];
                currentBlockSize = 0;
            }

            currentBlockEntries.push(entry);
            currentBlockSize += entrySize;
        }

        // Finalize last block
        if (currentBlockEntries.length > 0) {
            blocks.push(this._finalizeBlock(currentBlockEntries));
        }

        return blocks;
    }

    /**
     * FINALIZE A DATA BLOCK
     * 
     * Converts entries to binary format and adds metadata.
     * 
     * @private
     */
    _finalizeBlock(entries) {
        const data = Buffer.from(JSON.stringify(entries));
        return {
            startKey: entries[0].key,
            endKey: entries[entries.length - 1].key,
            data,
            offset: 0, // Will be set later
        };
    }

    /**
     * ESTIMATE ENTRY SIZE
     * 
     * @private
     */
    _estimateEntrySize(entry) {
        return JSON.stringify(entry).length;
    }
}

module.exports = { SSTableWriter, TOMBSTONE };
