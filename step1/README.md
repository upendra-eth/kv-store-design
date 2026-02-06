# Step 1: In-Memory Key-Value Store

## 🎯 Goal
Build the simplest possible key-value store - the foundation all databases start from.

## 📚 What You'll Learn

### 1. What is a Key-Value Store?
A key-value store is like a giant dictionary:
- **Key**: A unique identifier (like a word in a dictionary)
- **Value**: The data you want to store (like the definition)

```javascript
// Real-world examples:
store.set("user:123", { name: "Alice", email: "alice@example.com" });
store.set("session:abc", { userId: 123, expires: "2024-12-31" });
store.set("cache:homepage", "<html>...</html>");
```

### 2. Why HashMap?
We use JavaScript's `Map` which is a hash table:

| Operation | Time Complexity | What it means |
|-----------|----------------|---------------|
| get()     | O(1)           | Always fast, regardless of data size |
| set()     | O(1)           | Always fast |
| delete()  | O(1)           | Always fast |

**How hashing works:**
```
key "user:123" 
    → hash function → 
    bucket #42 
    → direct access!
```

### 3. Limitations (We'll Fix Later)

| Problem | Solution | Step |
|---------|----------|------|
| Data lost on crash | Write-Ahead Log | Step 2 |
| No range queries | Sorted MemTable | Step 3 |
| Memory limits | SSTable on disk | Step 4 |
| Multiple files | LSM Compaction | Step 5 |

## 🚀 Run the Demo

```bash
node step1/store.js
```

## 🧪 Try It Yourself

Create a file `step1/playground.js`:

```javascript
const { KVStore } = require('./store');

const db = new KVStore();

// Store some users
db.set('user:1', { name: 'Alice' });
db.set('user:2', { name: 'Bob' });

// Get them back
console.log(db.get('user:1')); // { name: 'Alice' }

// Check if exists
console.log(db.has('user:3')); // false

// Your experiments here!
```

## 🔗 Connection to LevelDB

LevelDB's architecture:
```
Write → [MemTable] → [WAL] → [SSTable Level 0] → [SSTable Level 1] → ...
         ↑
    We're here!
    (In-memory)
```

Our simple Map is like LevelDB's MemTable, but:
- LevelDB uses a **Skip List** (sorted) instead of HashMap
- This enables range queries like "get all users from user:100 to user:200"
- We'll implement this in Step 3!

## ✅ Checklist

- [x] Understand what a key-value store is
- [x] Know why O(1) operations matter
- [x] See the limitation: no persistence
- [ ] Ready for Step 2: Write-Ahead Log!
