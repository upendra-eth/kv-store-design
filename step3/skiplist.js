/**
 * ============================================================================
 * STEP 3: Skip List - The Secret Behind LevelDB's Speed
 * ============================================================================
 * 
 * 🎯 LEARNING GOAL:
 * Understand Skip Lists - the data structure LevelDB uses for its MemTable.
 * 
 * 📚 KEY CONCEPTS:
 * 
 * 1. THE PROBLEM WITH HASH MAPS
 *    HashMap (from Step 1) is great for point lookups O(1), but:
 *    - Keys are unordered - can't do range queries efficiently
 *    - "Give me all users from user:100 to user:200" → must scan ALL keys!
 * 
 * 2. WHY NOT JUST SORT AN ARRAY?
 *    Sorted array gives O(log n) search (binary search), but:
 *    - Insertions are O(n) - must shift all elements!
 *    - For a database with constant writes, this is too slow.
 * 
 * 3. ENTER THE SKIP LIST!
 *    A Skip List is a probabilistic data structure that gives us:
 *    - O(log n) search (like binary search)
 *    - O(log n) insert (unlike arrays!)
 *    - O(log n) delete
 *    - Easy iteration in sorted order
 * 
 * 4. HOW IT WORKS (VISUALIZATION)
 *    
 *    Imagine a linked list, but with "express lanes":
 *    
 *    Level 3:  HEAD ─────────────────────────────────→ 50 ──────────────→ NULL
 *    Level 2:  HEAD ─────────→ 20 ──────────────────→ 50 ──────────────→ NULL
 *    Level 1:  HEAD ──→ 10 ──→ 20 ──→ 30 ──────────→ 50 ──→ 60 ─────→ NULL
 *    Level 0:  HEAD ──→ 10 ──→ 20 ──→ 30 ──→ 40 ──→ 50 ──→ 60 ──→ 70 → NULL
 *    
 *    To find 40:
 *    1. Start at HEAD, Level 3 → 50 is too big, go down
 *    2. Level 2 → 20, then 50 is too big, go down from 20
 *    3. Level 1 → 30, then 50 is too big, go down from 30
 *    4. Level 0 → 40 found!
 *    
 *    We "skip" over many nodes using higher levels = O(log n)!
 * 
 * 5. THE PROBABILISTIC PART
 *    When inserting, we flip a coin to decide the height:
 *    - 50% chance: height 1 (only bottom level)
 *    - 25% chance: height 2
 *    - 12.5% chance: height 3
 *    - etc.
 *    
 *    This random approach gives O(log n) on average!
 * 
 * 🔗 HOW THIS RELATES TO LEVELDB:
 *    - LevelDB's MemTable IS a Skip List
 *    - All writes go to this sorted structure first
 *    - When it's full, it's flushed to disk as an SSTable (Step 4)
 */

/**
 * A node in the Skip List
 */
class SkipListNode {
    constructor(key, value, level) {
        this.key = key;
        this.value = value;
        // Array of forward pointers, one for each level
        // forward[i] points to the next node at level i
        this.forward = new Array(level).fill(null);
    }
}

/**
 * Skip List Implementation
 * 
 * This is the core data structure used by LevelDB's MemTable.
 */
class SkipList {
    /**
     * @param {number} maxLevel - Maximum height of the skip list
     * @param {number} probability - Probability of going up a level (usually 0.5)
     */
    constructor(maxLevel = 16, probability = 0.5) {
        this.maxLevel = maxLevel;
        this.probability = probability;
        this.level = 1; // Current highest level in use

        // Sentinel head node - doesn't hold actual data
        // Has pointers for all possible levels
        this.head = new SkipListNode(null, null, maxLevel);

        this.size = 0;
    }

    /**
     * RANDOM LEVEL GENERATION
     * 
     * This is the "magic" of skip lists!
     * Each level has 50% chance of being included.
     * 
     * Why this works:
     * - Level 1 has all nodes
     * - Level 2 has ~50% of nodes
     * - Level 3 has ~25% of nodes
     * - This creates a balanced "express lane" structure
     */
    _randomLevel() {
        let level = 1;
        while (Math.random() < this.probability && level < this.maxLevel) {
            level++;
        }
        return level;
    }

    /**
     * INSERT or UPDATE a key-value pair
     * 
     * Time Complexity: O(log n) average
     * 
     * @param {string} key - Key to insert
     * @param {any} value - Value to store
     */
    set(key, value) {
        // Track nodes that need updating at each level
        const update = new Array(this.maxLevel).fill(null);
        let current = this.head;

        // Start from highest level and work down
        // This is like using express lanes first, then local stops
        for (let i = this.level - 1; i >= 0; i--) {
            // Move forward while next node's key is less than target
            while (current.forward[i] !== null && current.forward[i].key < key) {
                current = current.forward[i];
            }
            // Remember this node - we might need to update its forward pointer
            update[i] = current;
        }

        // Move to the actual position (level 0)
        current = current.forward[0];

        // Key already exists - update value
        if (current !== null && current.key === key) {
            current.value = value;
            return;
        }

        // Key doesn't exist - insert new node
        const newLevel = this._randomLevel();

        // If new node is taller than current max, update head
        if (newLevel > this.level) {
            for (let i = this.level; i < newLevel; i++) {
                update[i] = this.head;
            }
            this.level = newLevel;
        }

        // Create new node
        const newNode = new SkipListNode(key, value, newLevel);

        // Insert node at all levels up to its height
        for (let i = 0; i < newLevel; i++) {
            newNode.forward[i] = update[i].forward[i];
            update[i].forward[i] = newNode;
        }

        this.size++;
    }

    /**
     * GET a value by key
     * 
     * Time Complexity: O(log n) average
     * 
     * @param {string} key - Key to look up
     * @returns {any} Value or undefined
     */
    get(key) {
        let current = this.head;

        // Start from top level and work down
        for (let i = this.level - 1; i >= 0; i--) {
            while (current.forward[i] !== null && current.forward[i].key < key) {
                current = current.forward[i];
            }
        }

        // Check if we found the key at level 0
        current = current.forward[0];
        if (current !== null && current.key === key) {
            return current.value;
        }
        return undefined;
    }

    /**
     * DELETE a key
     * 
     * Time Complexity: O(log n) average
     * 
     * @param {string} key - Key to delete
     * @returns {boolean} True if key was deleted
     */
    delete(key) {
        const update = new Array(this.maxLevel).fill(null);
        let current = this.head;

        for (let i = this.level - 1; i >= 0; i--) {
            while (current.forward[i] !== null && current.forward[i].key < key) {
                current = current.forward[i];
            }
            update[i] = current;
        }

        current = current.forward[0];

        if (current !== null && current.key === key) {
            // Remove node from all levels
            for (let i = 0; i < this.level; i++) {
                if (update[i].forward[i] !== current) {
                    break;
                }
                update[i].forward[i] = current.forward[i];
            }

            // Decrease level if needed
            while (this.level > 1 && this.head.forward[this.level - 1] === null) {
                this.level--;
            }

            this.size--;
            return true;
        }
        return false;
    }

    /**
     * CHECK if a key exists
     * 
     * @param {string} key
     * @returns {boolean}
     */
    has(key) {
        return this.get(key) !== undefined;
    }

    /**
     * RANGE QUERY - This is why we use Skip List!
     * 
     * Get all key-value pairs where startKey <= key <= endKey
     * 
     * This is IMPOSSIBLE to do efficiently with a HashMap!
     * With Skip List, it's O(log n + k) where k = number of results.
     * 
     * @param {string} startKey - Start of range (inclusive)
     * @param {string} endKey - End of range (inclusive)
     * @returns {Array} Array of {key, value} pairs
     */
    range(startKey, endKey) {
        const results = [];
        let current = this.head;

        // Find the starting position
        for (let i = this.level - 1; i >= 0; i--) {
            while (current.forward[i] !== null && current.forward[i].key < startKey) {
                current = current.forward[i];
            }
        }

        // Move to first node in range
        current = current.forward[0];

        // Iterate through range
        while (current !== null && current.key <= endKey) {
            results.push({ key: current.key, value: current.value });
            current = current.forward[0];
        }

        return results;
    }

    /**
     * ITERATE ALL ENTRIES in sorted order
     * 
     * This is O(n) - visits every node exactly once.
     * 
     * @returns {Generator} Yields {key, value} pairs
     */
    *[Symbol.iterator]() {
        let current = this.head.forward[0];
        while (current !== null) {
            yield { key: current.key, value: current.value };
            current = current.forward[0];
        }
    }

    /**
     * GET ALL KEYS in sorted order
     */
    keys() {
        return Array.from(this).map(entry => entry.key);
    }

    /**
     * VISUAL REPRESENTATION
     * 
     * Useful for understanding the structure!
     */
    visualize() {
        console.log('\n📊 Skip List Structure:');
        console.log('='.repeat(60));

        for (let level = this.level - 1; level >= 0; level--) {
            let line = `Level ${level}: HEAD`;
            let current = this.head.forward[level];

            while (current !== null) {
                line += ` ──→ ${current.key}`;
                current = current.forward[level];
            }
            line += ' ──→ NULL';
            console.log(line);
        }
        console.log('='.repeat(60));
    }
}

module.exports = { SkipList, SkipListNode };

/**
 * ============================================================================
 * 🧪 DEMO
 * ============================================================================
 */
if (require.main === module) {
    console.log('🚀 Step 3: Skip List Demo\n');
    console.log('='.repeat(50));

    const skipList = new SkipList();

    // Insert some users (notice: inserted out of order!)
    console.log('\n📝 Inserting users (out of order):');
    const users = [
        ['user:150', { name: 'Oscar' }],
        ['user:050', { name: 'Eve' }],
        ['user:200', { name: 'Alice' }],
        ['user:100', { name: 'John' }],
        ['user:175', { name: 'Max' }],
        ['user:025', { name: 'Zoe' }],
    ];

    for (const [key, value] of users) {
        skipList.set(key, value);
        console.log(`   set("${key}", ${JSON.stringify(value)})`);
    }

    // Visualize the structure
    skipList.visualize();

    // Point query
    console.log('\n🔍 Point Query:');
    console.log('   get("user:100") =>', skipList.get('user:100'));
    console.log('   get("user:999") =>', skipList.get('user:999'));

    // RANGE QUERY - the killer feature!
    console.log('\n🎯 RANGE QUERY (the killer feature!):');
    console.log('   range("user:050", "user:150"):');
    const rangeResults = skipList.range('user:050', 'user:150');
    for (const { key, value } of rangeResults) {
        console.log(`      ${key} => ${JSON.stringify(value)}`);
    }

    // Iterate all in order
    console.log('\n📋 All entries in sorted order:');
    for (const { key, value } of skipList) {
        console.log(`   ${key} => ${JSON.stringify(value)}`);
    }

    console.log('\n' + '='.repeat(50));
    console.log('✅ Step 3 Complete!\n');
    console.log('📚 What you learned:');
    console.log('   - Skip Lists: O(log n) insert, search, delete');
    console.log('   - Keys are always sorted');
    console.log('   - Range queries are now efficient!');
    console.log('   - This is what LevelDB uses for MemTable');
    console.log('\n⚠️ Current limitation: Still in-memory only!');
    console.log('   → Step 4 will flush this to disk as SSTable');
}
