/**
 * ============================================================================
 * STEP 2: Write-Ahead Log (WAL)
 * ============================================================================
 * 
 * 🎯 LEARNING GOAL:
 * Understand how databases achieve DURABILITY - never losing data, even on crash.
 * 
 * 📚 KEY CONCEPTS:
 * 
 * 1. THE PROBLEM
 *    In Step 1, if your program crashes or restarts, ALL data is lost!
 *    - In-memory data = volatile (disappears when process stops)
 *    - We need a way to persist data to disk
 * 
 * 2. WHY "WRITE-AHEAD"?
 *    The rule is simple: Write to the log BEFORE updating memory.
 *    
 *    Why this order?
 *    - If we update memory first, then crash before writing disk → data lost!
 *    - If we write disk first, then crash before updating memory → we can recover!
 * 
 *    Timeline:
 *    ┌─────────────────────────────────────────────────────────────────┐
 *    │  1. User: set("key", "value")                                   │
 *    │  2. WAL: Append to log file                                     │
 *    │  3. WAL: fsync() - force write to physical disk                 │
 *    │  4. Memory: Update in-memory Map                                │
 *    │  5. User: Return success                                        │
 *    └─────────────────────────────────────────────────────────────────┘
 *    If crash at step 3, we replay the log on restart.
 *    If crash at step 4, log has the data, we replay on restart.
 * 
 * 3. LOG FORMAT
 *    We use a simple line-based JSON format:
 *    
 *    {"op":"SET","key":"user:1","value":{"name":"Alice"},"ts":1234567890}
 *    {"op":"SET","key":"user:2","value":{"name":"Bob"},"ts":1234567891}
 *    {"op":"DELETE","key":"user:1","ts":1234567892}
 *    
 *    Each line is a complete, parseable JSON object.
 *    This is called "append-only log" or "journal".
 * 
 * 🔗 HOW THIS RELATES TO LEVELDB:
 *    - LevelDB uses WAL for the same reason
 *    - RocksDB (LevelDB's successor) has multiple WAL options
 *    - PostgreSQL, MySQL, MongoDB - all use similar logging
 *    - This is THE fundamental pattern for database durability
 */

const fs = require('fs');
const path = require('path');

class WriteAheadLog {
    /**
     * Create a new WAL instance
     * 
     * @param {string} logPath - Path to the log file
     * 
     * The log file is append-only:
     * - We never modify existing entries
     * - We only add new entries at the end
     * - This is very fast! Sequential writes are much faster than random writes
     */
    constructor(logPath) {
        this.logPath = logPath;
        this.fd = null; // File descriptor for the log file

        // Create directory if it doesn't exist
        const dir = path.dirname(logPath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        // Open file in append mode ('a' = append)
        // This ensures we always write at the end
        this.fd = fs.openSync(this.logPath, 'a');
    }

    /**
     * LOG A SET OPERATION
     * 
     * @param {string} key - The key being set
     * @param {any} value - The value being stored
     * 
     * Format: {"op":"SET","key":"...","value":...,"ts":...}
     */
    logSet(key, value) {
        const entry = {
            op: 'SET',
            key,
            value,
            ts: Date.now() // Timestamp helps with debugging and ordering
        };
        this._appendEntry(entry);
    }

    /**
     * LOG A DELETE OPERATION
     * 
     * @param {string} key - The key being deleted
     * 
     * 🤔 INTERESTING:
     * We log deletes too! This is important because:
     * - On recovery, we need to know which keys were deleted
     * - Without this, deleted keys would "come back"
     * 
     * This "delete marker" is called a TOMBSTONE in database terminology.
     * We'll see this again in Step 5 with SSTables!
     */
    logDelete(key) {
        const entry = {
            op: 'DELETE',
            key,
            ts: Date.now()
        };
        this._appendEntry(entry);
    }

    /**
     * APPEND AN ENTRY TO THE LOG
     * 
     * This is the core of WAL:
     * 1. Convert entry to JSON
     * 2. Write to file (with newline)
     * 3. fsync to ensure it's on disk
     * 
     * @private
     */
    _appendEntry(entry) {
        // Convert to JSON and add newline
        const line = JSON.stringify(entry) + '\n';

        // Write to file
        fs.writeSync(this.fd, line);

        // CRITICAL: fsync forces the OS to flush to disk!
        // Without this, data might still be in OS buffer and lost on power failure.
        // 
        // Trade-off: fsync is SLOW (5-10ms on SSD, 10-20ms on HDD)
        // Some databases batch writes or use group commit to improve performance.
        fs.fsyncSync(this.fd);
    }

    /**
     * RECOVER: REPLAY THE LOG
     * 
     * @returns {Map} A Map containing the recovered state
     * 
     * On startup, we read the entire log and replay all operations.
     * This reconstructs the exact state before the crash.
     * 
     * Time complexity: O(n) where n = number of log entries
     * This is why we eventually need SSTable compaction (Step 5)!
     */
    recover() {
        const store = new Map();

        // Check if log file exists
        if (!fs.existsSync(this.logPath)) {
            console.log('📝 No WAL found, starting fresh');
            return store;
        }

        console.log('🔄 Recovering from WAL...');

        // Read the entire log file
        const content = fs.readFileSync(this.logPath, 'utf8');
        const lines = content.split('\n').filter(line => line.trim());

        let setCount = 0;
        let deleteCount = 0;

        // Replay each entry
        for (const line of lines) {
            try {
                const entry = JSON.parse(line);

                if (entry.op === 'SET') {
                    store.set(entry.key, entry.value);
                    setCount++;
                } else if (entry.op === 'DELETE') {
                    store.delete(entry.key);
                    deleteCount++;
                }
            } catch (err) {
                // If a line is corrupt, log and skip
                // In production, you'd want better error handling
                console.error(`⚠️ Skipping corrupt log entry: ${line}`);
            }
        }

        console.log(`✅ Recovered ${setCount} SETs, ${deleteCount} DELETEs`);
        return store;
    }

    /**
     * CLOSE THE LOG
     * 
     * Always close files when done to prevent resource leaks!
     */
    close() {
        if (this.fd !== null) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
    }

    /**
     * GET LOG FILE SIZE
     * 
     * Useful for knowing when the log is getting too big.
     * In production, you'd "checkpoint" and truncate the log periodically.
     */
    size() {
        if (fs.existsSync(this.logPath)) {
            return fs.statSync(this.logPath).size;
        }
        return 0;
    }
}

module.exports = { WriteAheadLog };
