/**
 * ============================================================================
 * STEP 5: Demo - Complete KV Database Engine
 * ============================================================================
 * 
 * This demo shows the complete system working together:
 * - WAL for durability (Step 2)
 * - MemTable with skip list (Step 3)
 * - SSTables on disk (Step 4)
 * - LSM Tree with compaction (Step 5)
 * 
 * Run this to see everything in action!
 */

const fs = require('fs');
const { LSMTree } = require('./lsm-tree');

const dataDir = './kv-demo-data';

console.log('🚀 Complete KV Database Engine Demo\n');
console.log('='.repeat(60));
console.log('This demo shows all 5 components working together!');
console.log('='.repeat(60));

// Clean up from previous runs
if (fs.existsSync(dataDir)) {
    fs.rmSync(dataDir, { recursive: true });
}

// Create database with small settings for demo
const db = new LSMTree(dataDir, {
    memtableMaxSize: 2048, // 2KB (small for demo)
    level0MaxFiles: 2,     // Compact after 2 files
});

// 1. BASIC OPERATIONS
console.log('\n📝 1. Basic Operations');
console.log('-'.repeat(40));

db.set('user:001', { name: 'Alice', role: 'admin' });
db.set('user:002', { name: 'Bob', role: 'engineer' });
db.set('user:003', { name: 'Charlie', role: 'designer' });

console.log('Set 3 users');
console.log('get("user:001") =>', db.get('user:001'));
console.log('get("user:002") =>', db.get('user:002'));

// 2. BULK WRITES (triggers MemTable flush)
console.log('\n📝 2. Bulk Writes (triggering flush)');
console.log('-'.repeat(40));

console.log('Writing 50 entries to trigger MemTable flush...');
for (let i = 10; i < 60; i++) {
    const key = `data:${String(i).padStart(4, '0')}`;
    const value = { index: i, payload: 'x'.repeat(30) };
    db.set(key, value);
}

console.log('\nAfter bulk writes:');
console.log(JSON.stringify(db.stats(), null, 2));

// 3. RANGE QUERY
console.log('\n🎯 3. Range Query');
console.log('-'.repeat(40));

const rangeResults = db.range('data:0015', 'data:0020');
console.log('range("data:0015", "data:0020"):');
for (const { key, value } of rangeResults) {
    console.log(`  ${key} => index: ${value.index}`);
}

// 4. DELETE AND TOMBSTONES
console.log('\n🗑️ 4. Delete and Tombstones');
console.log('-'.repeat(40));

console.log('Before delete: get("user:002") =>', db.get('user:002'));
db.delete('user:002');
console.log('After delete: get("user:002") =>', db.get('user:002'));

// 5. MORE WRITES (trigger compaction)
console.log('\n🔧 5. More Writes (triggering compaction)');
console.log('-'.repeat(40));

console.log('Writing more entries to trigger compaction...');
for (let i = 100; i < 150; i++) {
    const key = `batch2:${String(i).padStart(4, '0')}`;
    const value = { index: i, timestamp: Date.now() };
    db.set(key, value);
}

console.log('\nAfter more writes:');
console.log(JSON.stringify(db.stats(), null, 2));

// 6. CRASH RECOVERY DEMO
console.log('\n💥 6. Crash Recovery Demo');
console.log('-'.repeat(40));

console.log('Adding some data before "crash"...');
db.set('important:001', { critical: 'data', value: 42 });
db.set('important:002', { critical: 'more data', value: 100 });

console.log('get("important:001") =>', db.get('important:001'));
console.log('get("important:002") =>', db.get('important:002'));

// "Crash" - close without cleanup
db.close();
console.log('\n💥 Simulating crash...\n');

// Recover
console.log('🔄 Recovering after crash...');
const db2 = new LSMTree(dataDir, {
    memtableMaxSize: 2048,
    level0MaxFiles: 2,
});

console.log('\n🔍 Verifying recovery:');
console.log('get("user:001") =>', db2.get('user:001'));
console.log('get("user:002") =>', db2.get('user:002'), '(should be undefined - was deleted!)');
console.log('get("important:001") =>', db2.get('important:001'));
console.log('get("important:002") =>', db2.get('important:002'));

console.log('\n📊 Recovered database stats:');
console.log(JSON.stringify(db2.stats(), null, 2));

db2.close();

// Summary
console.log('\n' + '='.repeat(60));
console.log('✅ Complete Demo Finished!\n');
console.log('📚 Your KV Database Engine includes:');
console.log('');
console.log('   Step 1: In-Memory Store');
console.log('           └── HashMap for O(1) operations');
console.log('');
console.log('   Step 2: Write-Ahead Log');
console.log('           └── Append-only log with fsync for durability');
console.log('');
console.log('   Step 3: MemTable');
console.log('           └── Skip List for O(log n) sorted operations');
console.log('');
console.log('   Step 4: SSTable');
console.log('           └── Sorted String Table with binary search on disk');
console.log('');
console.log('   Step 5: LSM Tree');
console.log('           └── Log-Structured Merge Tree with compaction');
console.log('');
console.log('🎉 Congratulations! You now understand how LevelDB works!\n');
console.log('='.repeat(60));

// Show files on disk
console.log('\n📁 Files created:');
if (fs.existsSync(dataDir)) {
    const files = fs.readdirSync(dataDir);
    for (const file of files) {
        const stats = fs.statSync(`${dataDir}/${file}`);
        console.log(`   ${file}: ${stats.size} bytes`);
    }
}

// Cleanup
fs.rmSync(dataDir, { recursive: true });
console.log('\n🧹 Demo data cleaned up.\n');
