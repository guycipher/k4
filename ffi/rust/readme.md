# K4 Rust FFI
This is an example library that demonstrates how to use a K4 FFI in Rust using the shared K4 C library.

## Example
An example of using the FFI to interact with a K4 database is shown below.
```rust
use std::ffi::CString;
use std::ptr;

fn main() {
    unsafe {
        // Open a database
        let directory = CString::new("data").expect("CString::new failed");
        let db = db_open(directory.as_ptr(), 1024, 60, 1, 1);
        if db.is_null() {
            eprintln!("Failed to open database");
            return;
        }

        // Put a key-value pair
        let key = CString::new("key1").expect("CString::new failed");
        let value = CString::new("value1").expect("CString::new failed");
        if db_put(db, key.as_ptr(), key.as_bytes().len() as c_int, value.as_ptr(), value.as_bytes().len() as c_int, -1) != 0 {
            eprintln!("Failed to put key-value pair");
            db_close(db);
            return;
        }

        // Get the value for the key
        let retrieved_value_ptr = db_get(db, key.as_ptr(), key.as_bytes().len() as c_int);
        if retrieved_value_ptr.is_null() {
            eprintln!("Failed to get value");
            db_close(db);
            return;
        }

        let retrieved_value = CStr::from_ptr(retrieved_value_ptr).to_string_lossy().into_owned();
        println!("Retrieved value: {}", retrieved_value);

        // Delete the key-value pair
        if db_delete(db, key.as_ptr(), key.as_bytes().len() as c_int) != 0 {
            eprintln!("Failed to delete key-value pair");
            db_close(db);
            return;
        }

        // Close the database
        if db_close(db) != 0 {
            eprintln!("Failed to close database");
        }
    }
}