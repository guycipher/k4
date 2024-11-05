## K4 Benchmarking Utility
These programs benchmark Put, Get, and Delete operations against RocksDB, LMDB, and K4.

Their written in C and GO.

You must have RocksDB, LMDB, and K4 installed on your system.


### C Bench building
```
gcc -o bench bench.c -lrocksdb -llmdb -lk4 && ./bench
```

### GO Bench building
```
go build -o bench bench.go && ./bench
```

10000 operations are performed for each operation type.
The shared C library isn't as optimized as the Go library, so the results may vary.
```
-- These are results from bench.c on below system --
11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz UBuntu with WDC WDS500G2B0A-00SM50(HDD)
-+ssssssssssssssssssyyssss+-         OS: Ubuntu 23.04 x86_64
.ossssssssssssssssssdMMMNysssso.       Kernel: 6.2.0-39-generic
/ssssssssssshdmmNNmmyNMMMMhssssss/      Uptime: 9 days, 16 hours, 23 mins
+ssssssssshmydMMMMMMMNddddyssssssss+     Packages: 3141 (dpkg), 29 (snap)
/sssssssshNMMMyhhyyyyhmNMMMNhssssssss/    Shell: bash 5.2.15
.ssssssssdMMMNhsssssssssshNMMMdssssssss.   Resolution: 1080x1920, 1920x1080
+sssshhhyNMMNyssssssssssssyNMMMysssssss+   DE: GNOME 44.3
ossyNMMMNyMMhsssssssssssssshmmmhssssssso   WM: Mutter
ossyNMMMNyMMhsssssssssssssshmmmhssssssso   WM Theme: Adwaita
+sssshhhyNMMNyssssssssssssyNMMMysssssss+   Theme: Yaru [GTK2/3]
.ssssssssdMMMNhsssssssssshNMMMdssssssss.   Icons: Yaru [GTK2/3]
/sssssssshNMMMyhhyyyyhdNMMMNhssssssss/    Terminal: gnome-terminal
+sssssssssdmydMMMMMMMMddddyssssssss+     CPU: 11th Gen Intel i7-11700K (16) @ 4.900GH
/ssssssssssshdmNNNNmyNMMMMhssssss/      GPU: AMD ATI Radeon RX 5500/5500M / Pro 5500
.ossssssssssssssssssdMMMNysssso.       GPU: Intel RocketLake-S GT1 [UHD Graphics 75
-+sssssssssssssssssyyyssss+-         GPU: NVIDIA GeForce GT 730
`:+ssssssssssssssssss+:`           Memory: 13541MiB / 47928MiB

------------------------------------
RocksDB Put: 0.027765 seconds
RocksDB Get: 0.009911 seconds
RocksDB Delete: 0.034122 seconds
LMDB Put: 0.308468 seconds
LMDB Get: 0.001778 seconds
LMDB Delete: 0.299799 seconds
K4-C Put: 0.802232 seconds
K4-C Get: 0.671278 seconds
K4-C Delete: 0.877823 seconds
K4-GO Put: 0.007235 seconds
K4-GO Get: 0.003845 seconds
K4-GO Delete: 0.006657 seconds
```