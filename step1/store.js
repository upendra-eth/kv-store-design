/**
 * ============================================================================
 * STEP 1: In-Memory Key-Value Store
 * ============================================================================
 * 
 * 🎯 LEARNING GOAL:
 * Understand the foundation of all KV databases - a simple in-memory store.
 * 
 * 📚 KEY CONCEPTS:
 * 
 * 1. WHAT IS A KEY-VALUE STORE?
 *    - Think of it like a JavaScript object or a dictionary
 *    - You store data with a unique "key" and retrieve it later
 *    - Example: key = "user:123", value = { name: "John", age: 30 }
 * 
 * 2. WHY USE A MAP INSTEAD OF AN OBJECT?
 *    - JavaScript Map has better performance for frequent add/delete
 *    - Keys can be any type (not just strings)
 *    - Maintains insertion order
 *    - Has a .size property (objects need Object.keys().length)
 * 
 * 3. TIME COMPLEXITY (Big O):
 *    - get():    O(1) - constant time, super fast!
 *    - set():    O(1) - constant time
 *    - delete(): O(1) - constant time
 *    
 *    This is why KV stores are popular - they're incredibly fast for
 *    simple lookups. But they have limitations (no range queries... yet!)
 * 
 * 🔗 HOW THIS RELATES TO LEVELDB:
 *    LevelDB also has this in-memory component (called MemTable),
 *    but it uses a sorted structure (Skip List) instead of a hash map.
 *    We'll learn why in Step 3!
 */

class KVStore {
  constructor() {
    /**
     * The heart of our store - a JavaScript Map
     * 
     * Why Map?
     * - HashMap internally (O(1) operations)
     * - Better than plain object for dynamic keys
     * 
     * In real databases:
     * - Redis uses hash tables (like Map)
     * - LevelDB uses Skip Lists (sorted, we'll learn in Step 3)
     */
    this._data = new Map();
  }

  /**
   * SET - Store a value with a key
   * 
   * @param {string} key - Unique identifier for the data
   * @param {any} value - The data to store
   * 
   * Example:
   *   store.set("user:1", { name: "Alice" })
   *   store.set("config:theme", "dark")
   * 
   * Common patterns for keys:
   *   - "namespace:id" (e.g., "user:123", "order:456")
   *   - "namespace:subkey:id" (e.g., "user:email:123")
   */
  set(key, value) {
    // Validate key - must be a string
    if (typeof key !== 'string') {
      throw new Error('Key must be a string');
    }

    // Store the value
    // If key exists, it gets overwritten (upsert behavior)
    this._data.set(key, value);

    // Return the value for chaining convenience
    return value;
  }

  /**
   * GET - Retrieve a value by its key
   * 
   * @param {string} key - The key to look up
   * @returns {any} The stored value, or undefined if not found
   * 
   * This is what makes KV stores fast:
   * - No scanning through tables
   * - No index lookups
   * - Direct hash lookup: O(1)
   */
  get(key) {
    return this._data.get(key);
  }

  /**
   * DELETE - Remove a key-value pair
   * 
   * @param {string} key - The key to delete
   * @returns {boolean} True if key existed and was deleted
   * 
   * 🤔 INTERESTING NOTE:
   * In LevelDB, delete doesn't actually remove data immediately!
   * It writes a "tombstone" marker. We'll learn why in Step 5.
   */
  delete(key) {
    return this._data.delete(key);
  }

  /**
   * HAS - Check if a key exists
   * 
   * @param {string} key - The key to check
   * @returns {boolean} True if key exists
   * 
   * Useful for checking existence without getting the value
   */
  has(key) {
    return this._data.has(key);
  }

  /**
   * GET ALL KEYS - Return all stored keys
   * 
   * @returns {string[]} Array of all keys
   * 
   * ⚠️ WARNING: Expensive operation!
   * - Creates a new array with all keys
   * - O(n) time and space complexity
   * - In production, use iterators for large datasets
   */
  keys() {
    return Array.from(this._data.keys());
  }

  /**
   * SIZE - Get the number of stored items
   * 
   * @returns {number} Count of key-value pairs
   */
  size() {
    return this._data.size;
  }

  /**
   * CLEAR - Remove all data
   * 
   * ⚠️ DANGER: This is irreversible!
   * In Step 2, we'll add WAL (Write-Ahead Log) so we can recover data.
   */
  clear() {
    this._data.clear();
  }
}

// Export for use in other files
module.exports = { KVStore };

/**
 * ============================================================================
 * 🧪 TRY IT YOURSELF!
 * ============================================================================
 * 
 * Run this file directly to see the store in action:
 *   node step1/store.js
 */
if (require.main === module) {
  console.log('🚀 Step 1: In-Memory Key-Value Store Demo\n');
  console.log('='.repeat(50));

  const store = new KVStore();

  // Demo: Basic operations
  console.log('\n📝 Setting values...');
  store.set('user:1', { name: 'Alice', role: 'admin' });
  store.set('user:2', { name: 'Bob', role: 'user' });
  store.set('config:theme', 'dark');

  console.log('   store.set("user:1", { name: "Alice", role: "admin" })');
  console.log('   store.set("user:2", { name: "Bob", role: "user" })');
  console.log('   store.set("config:theme", "dark")');

  console.log('\n📖 Getting values...');
  console.log('   store.get("user:1") =>', store.get('user:1'));
  console.log('   store.get("user:2") =>', store.get('user:2'));
  console.log('   store.get("config:theme") =>', store.get('config:theme'));
  console.log('   store.get("nonexistent") =>', store.get('nonexistent'));

  console.log('\n🔍 Checking existence...');
  console.log('   store.has("user:1") =>', store.has('user:1'));
  console.log('   store.has("user:999") =>', store.has('user:999'));

  console.log('\n📊 Store stats...');
  console.log('   store.size() =>', store.size());
  console.log('   store.keys() =>', store.keys());

  console.log('\n🗑️ Deleting...');
  console.log('   store.delete("user:2") =>', store.delete('user:2'));
  console.log('   store.size() =>', store.size());

  console.log('\n' + '='.repeat(50));
  console.log('✅ Step 1 Complete!\n');
  console.log('📚 What you learned:');
  console.log('   - Basic KV operations: get, set, delete');
  console.log('   - O(1) time complexity with hash maps');
  console.log('   - Key naming conventions (namespace:id)');
  console.log('\n⚠️ Limitation: Data is lost when the process stops!');
  console.log('   → Step 2 will fix this with Write-Ahead Log (WAL)');
}
