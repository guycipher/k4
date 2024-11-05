# K4 Lua FFI
This is an example library that demonstrates how to use a K4 FFI in Lua using the shared K4 C library.

## Example
```
-- Open the database
local db = k4.db_open("data", 1024, 60, 1, 1)
if db == nil then
    print("Failed to open database")
    return
end

-- Put a key-value pair
local key = "key1"
local value = "value1"
if k4.db_put(db, key, #key, value, #value, -1) ~= 0 then -- -1 means no expiration
    print("Failed to put key-value pair")
    k4.db_close(db)
    return
end

-- Get the value for the key
local retrieved_value = k4.db_get(db, key, #key)
if retrieved_value == nil then
    print("Failed to get value")
    k4.db_close(db)
    return
end

print("Retrieved value:", ffi.string(retrieved_value))

-- Delete the key-value pair
if k4.db_delete(db, key, #key) ~= 0 then
    print("Failed to delete key-value pair")
    k4.db_close(db)
    return
end

-- Close the database
if k4.db_close(db) ~= 0 then
    print("Failed to close database")
end
```