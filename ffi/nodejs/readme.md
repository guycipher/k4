# K4 Node.JS FFI
This is an example library that demonstrates how to use a K4 FFI in Node.JS using the shared K4 C library.

## Examples
```javascript
const k4 = require('./k4');

const db = k4.db_open('data', 1024, 60, 1, 1);
if (db.isNull()) {
    console.error('Failed to open database');
    process.exit(1);
}

const key = 'key1';
const value = 'value1';
if (k4.db_put(db, key, key.length, value, value.length, -1) !== 0) { // ttl = -1 means no expiration
    console.error('Failed to put key-value pair');
    libk4.db_close(db);
    process.exit(1);
}

const retrievedValue = k4.db_get(db, key, key.length);
if (retrievedValue.isNull()) {
    console.error('Failed to get value');
    libk4.db_close(db);
    process.exit(1);
}

console.log('Retrieved value:', retrievedValue);

if (k4.db_delete(db, key, key.length) !== 0) {
    console.error('Failed to delete key-value pair');
    libk4.db_close(db);
    process.exit(1);
}

if (k4.db_close(db) !== 0) {
    console.error('Failed to close database');
    process.exit(1);
}
```