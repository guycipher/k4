## K4 Server example
This is a basic server example using K4 as a key-value store.  The server will listen on port 8000.

The protocol is simple, it's a simple text protocol with the following commands
```
PUT key value
GET key
DELETE key
```

### Example
```
$ nc localhost 8000
put hello world
OK
get hello
Value: world
```