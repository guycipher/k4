## C Bindings for K4
Building C library
```
go build -o libk4.so -buildmode=c-shared k4.go
```

Once run you should get
``
libk4.so
libk4.h
```

On unix copy the files to /usr/local/lib and /usr/local/include
```
sudo cp libk4.so /usr/local/lib/
sudo cp libk4.h /usr/local/include/
```

On windows copy the files to C:\Program Files\K4
```
copy libk4.so C:\Program Files\K4
copy libk4.h C:\Program Files\K4
```

On unix update the library cache
```
sudo ldconfig
```

Verify install
`
ldconfig -p | grep libk4
```

Should see
```
libk4.so (libc6,x86-64) => /usr/local/lib/libk4.so
```

Now you should be able to compile example
```
cc -o example example.c -L/usr/local/lib -lk4 -I/usr/local/include
```


