# K4 Python FFI
This is an example library that demonstrates how to use a K4 FFI in Python using the shared K4 C library.


## Example
```
def main():
    # Open the database
    db = k4.db_open(b"data", 1024, 60, 1, 1)
    if not db:
        print("Failed to open database")
        return

    # Put a key-value pair
    key = b"key1"
    value = b"value1"

    if k4.db_put(db, key, len(key), value, len(value), -1) != 0: # -1 means no expiration
        print("Failed to put key-value pair")
        k4.db_close(db)
        return

    # Get the value for the key
    retrieved_value = k4.db_get(db, key, len(key))
    if not retrieved_value:
        print("Failed to get value")
        k4.db_close(db)
        return

    print("Retrieved value:", retrieved_value.decode('utf-8'))

    # Delete the key-value pair
    if k4.db_delete(db, key, len(key)) != 0:
        print("Failed to delete key-value pair")
        k4.db_close(db)
        return

    # Close the database
    if k4.db_close(db) != 0:
        print("Failed to close database")

if __name__ == "__main__":
    main()
```