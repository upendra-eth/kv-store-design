/**
 * ============================================================================
 * STEP 2: Durable Key-Value Store (with WAL)
 * ============================================================================
 * 
 * This is Step 1's store + WAL = Durability!
 * 
 * Now your data survives crashes and restarts.
 */

const { WriteAheadLog } = require('./wal');

class DurableKVStore {
    /**
     * Create a durable KV store
     * 
     * @param {string} dataDir - Directory to store data
     * 
     * Unlike Step 1, this store:
     * 1. Writes every operation to disk FIRST (via WAL)
     * 2. Then updates memory
     * 3. On restart, recovers state from WAL
     */
    constructor(dataDir = './data') {
        this.dataDir = dataDir;

        // Initialize WAL
        this.wal = new WriteAheadLog(`${dataDir}/wal.log`);

        // Recover any existing data from WAL
        this._data = this.wal.recover();

        console.log(`📦 Store initialized with ${this._data.size} keys`);
    }

    /**
     * SET with durability
     * 
     * Order matters!
     * 1. Write to WAL (disk)
     * 2. Update memory
     * 
     * If crash after step 1, we recover from WAL.
     * If crash after step 2, we're fine.
     */
    set(key, value) {
        if (typeof key !== 'string') {
            throw new Error('Key must be a string');
        }

        // FIRST: Write to WAL (ensures durability)
        this.wal.logSet(key, value);

        // THEN: Update in-memory store
        this._data.set(key, value);

        return value;
    }

    /**
     * DELETE with durability
     * 
     * Same principle: WAL first, then memory.
     */
    delete(key) {
        // FIRST: Log the delete
        this.wal.logDelete(key);

        // THEN: Delete from memory
        return this._data.delete(key);
    }

    /**
     * GET - Reads are fast (memory only)
     * 
     * No WAL needed for reads because:
     * - We're not modifying anything
     * - Memory is always up-to-date
     */
    get(key) {
        return this._data.get(key);
    }

    has(key) {
        return this._data.has(key);
    }

    keys() {
        return Array.from(this._data.keys());
    }

    size() {
        return this._data.size;
    }

    /**
     * CLOSE - Clean shutdown
     * 
     * Always close the store when done!
     * This ensures WAL is properly flushed.
     */
    close() {
        this.wal.close();
        console.log('👋 Store closed');
    }

    /**
     * GET WAL STATS
     * 
     * Useful for monitoring
     */
    stats() {
        return {
            keys: this._data.size,
            walSize: this.wal.size(),
        };
    }
}

module.exports = { DurableKVStore };

/**
 * ============================================================================
 * 🧪 DEMO: Testing durability with simulated crash
 * ============================================================================
 */
if (require.main === module) {
    const fs = require('fs');
    const dataDir = './step2-demo-data';

    console.log('🚀 Step 2: Durable Key-Value Store Demo\n');
    console.log('='.repeat(50));

    // Clean up from previous runs
    if (fs.existsSync(dataDir)) {
        fs.rmSync(dataDir, { recursive: true });
    }

    // First "session" - write some data
    console.log('\n📝 Session 1: Writing data...');
    let store = new DurableKVStore(dataDir);

    store.set('user:1', { name: 'Alice', role: 'admin' });
    store.set('user:2', { name: 'Bob', role: 'user' });
    store.set('counter', 42);

    console.log('   Wrote 3 key-value pairs');
    console.log('   Stats:', store.stats());

    // Simulating "crash" - just close without cleanup
    store.close();
    console.log('\n💥 Simulating crash (process restart)...\n');

    // Second "session" - recover and verify
    console.log('📝 Session 2: Recovering after crash...');
    store = new DurableKVStore(dataDir);

    console.log('\n🔍 Verifying recovered data:');
    console.log('   user:1 =>', store.get('user:1'));
    console.log('   user:2 =>', store.get('user:2'));
    console.log('   counter =>', store.get('counter'));

    // Now delete something and verify
    console.log('\n🗑️ Deleting user:2 and adding user:3...');
    store.delete('user:2');
    store.set('user:3', { name: 'Charlie', role: 'guest' });
    store.close();

    console.log('\n💥 Simulating another crash...\n');

    // Third "session" - verify deletes persisted too
    console.log('📝 Session 3: Verifying deletes persisted...');
    store = new DurableKVStore(dataDir);

    console.log('\n🔍 Current state:');
    console.log('   user:1 =>', store.get('user:1'));
    console.log('   user:2 =>', store.get('user:2'), '(should be undefined - deleted!)');
    console.log('   user:3 =>', store.get('user:3'));
    console.log('   All keys:', store.keys());

    store.close();

    // Show WAL contents
    console.log('\n📜 WAL file contents:');
    console.log('-'.repeat(50));
    const walContent = fs.readFileSync(`${dataDir}/wal.log`, 'utf8');
    walContent.split('\n').filter(l => l).forEach((line, i) => {
        const entry = JSON.parse(line);
        console.log(`   ${i + 1}. ${entry.op} ${entry.key}`);
    });

    console.log('\n' + '='.repeat(50));
    console.log('✅ Step 2 Complete!\n');
    console.log('📚 What you learned:');
    console.log('   - Write-Ahead Log pattern for durability');
    console.log('   - fsync() to force writes to disk');
    console.log('   - Crash recovery by replaying the log');
    console.log('   - Tombstones for delete operations');
    console.log('\n⚠️ Limitation: Log grows forever, no range queries!');
    console.log('   → Step 3 will add sorted MemTable for range queries');
    console.log('   → Step 5 will add compaction to reclaim space');

    // Cleanup demo data
    fs.rmSync(dataDir, { recursive: true });
}
