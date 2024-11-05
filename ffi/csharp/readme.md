# K4 C# FFI
This is an example library that demonstrates how to use a K4 FFI in C# using the shared K4 C library.

## Example
```csharp
using System;
using System.Runtime.InteropServices;

public class ExampleUsage
{
    public static void Main(string[] args)
    {
        IntPtr db = K4.db_open("data", 1024, 60, 1, 1);
        if (db == IntPtr.Zero)
        {
            Console.WriteLine("Failed to open database");
            return;
        }

        string key = "key1";
        string value = "value1";

        if (K4.db_put(db, key, key.Length, value, value.Length, -1) != 0)
        {
            Console.WriteLine("Failed to put key-value pair");
            K4.db_close(db);
            return;
        }

        IntPtr retrievedValuePtr = K4.db_get(db, key, key.Length);
        if (retrievedValuePtr == IntPtr.Zero)
        {
            Console.WriteLine("Failed to get value");
            K4.db_close(db);
            return;
        }

        string retrievedValue = Marshal.PtrToStringAnsi(retrievedValuePtr);
        Console.WriteLine("Retrieved value: " + retrievedValue);

        if (K4.db_delete(db, key, key.Length) != 0)
        {
            Console.WriteLine("Failed to delete key-value pair");
            K4.db_close(db);
            return;
        }

        if (K4.db_close(db) != 0)
        {
            Console.WriteLine("Failed to close database");
        }
    }
}
```