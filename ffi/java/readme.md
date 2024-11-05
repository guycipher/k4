# K4 Java FFI
This is an example library that demonstrates how to use a K4 FFI in Java using the shared K4 C library.

## Example
```java
public class ExampleUsage {
    public static void main(String[] args) {
        Pointer db = K4.INSTANCE.db_open("data", 1024, 60, 1, 1);
        if (db == null) {
            System.out.println("Failed to open database");
            return;
        }

        String key = "key1";
        String value = "value1";
        if (K4.INSTANCE.db_put(db, key, key.length(), value, value.length(), -1) != 0) {
            System.out.println("Failed to put key-value pair");
            K4.INSTANCE.db_close(db);
            return;
        }

        Pointer retrievedValue = K4.INSTANCE.db_get(db, key, key.length());
        if (retrievedValue == null) {
            System.out.println("Failed to get value");
            K4.INSTANCE.db_close(db);
            return;
        }

        System.out.println("Retrieved value: " + retrievedValue.getString(0));

        if (K4.INSTANCE.db_delete(db, key, key.length()) != 0) {
            System.out.println("Failed to delete key-value pair");
            K4.INSTANCE.db_close(db);
            return;
        }

        if (K4.INSTANCE.db_close(db) != 0) {
            System.out.println("Failed to close database");
        }
    }
}

```