# K4 Ruby FFI
This is an example library that demonstrates how to use a K4 FFI in Ruby using the shared K4 C library.

## Example
```
db = K4.db_open("data", 1024, 60, 1, 1)
if db.null?
    puts "Failed to open database"
    exit
end

key = "key1"
value = "value1"
if K4.db_put(db, key, key.length, value, value.length, -1) != 0
    puts "Failed to put key-value pair"
    K4.db_close(db)
    exit
end

retrieved_value = K4.db_get(db, key, key.length)
if retrieved_value.null?
    puts "Failed to get value"
    K4.db_close(db)
    exit
end

puts "Retrieved value: #{retrieved_value.read_string}"

if K4.db_delete(db, key, key.length) != 0
    puts "Failed to delete key-value pair"
    K4.db_close(db)
    exit
end

if K4.db_close(db) != 0
    puts "Failed to close database"
end
```