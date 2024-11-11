## K4 Benchmarking
These programs benchmark Put, Get, and Delete operations against RocksDB, LMDB, and K4.

Their written in C and GO.

You must have RocksDB, LMDB, and K4 installed on your system.


### C Bench building
```
gcc -o bench bench.c -lrocksdb -llmdb -lk4 && ./bench
gcc -o bench_random bench_random.c -lrocksdb -llmdb -lk4 && ./bench_random
gcc -o bench_concurrent bench_concurrent.c -lrocksdb -llmdb -lk4 && ./bench_concurrent
```

### GO Bench building
```
go build -o bench bench.go && ./bench -num_ops 10000 -num_threads 4
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

Random reads and writes
------------------------------------
RocksDB Put: 0.037424 seconds
RocksDB Get: 0.011199 seconds
RocksDB Delete: 0.033449 seconds
LMDB Put: 0.281653 seconds
LMDB Get: 0.004544 seconds
LMDB Delete: 0.004241 seconds
K4-C Put: 0.775244 seconds
K4-C Get: 0.618421 seconds
K4-C Delete: 0.865813 seconds
K4-GO Put: 0.008732 seconds
K4-GO Get: 0.005464 seconds
K4-GO Delete: 0.008889 seconds

Concurrent random reads and writes
------------------------------------
RocksDB Put: 0.032743 seconds
RocksDB Get: 0.011951 seconds
RocksDB Delete: 0.030388 seconds
LMDB Put: 0.989418 seconds
LMDB Get: 0.006578 seconds
LMDB Put: 0.985441 seconds
LMDB Get: 0.008586 seconds
LMDB Put: 1.014217 seconds
LMDB Get: 0.016622 seconds
LMDB Put: 1.039773 seconds
LMDB Delete: 0.181927 seconds
LMDB Delete: 0.215883 seconds
LMDB Get: 0.168402 seconds
LMDB Delete: 0.183181 seconds
LMDB Delete: 0.009368 seconds
K4-C Put: 0.705238 seconds
K4-C Put: 0.706127 seconds
K4-C Put: 0.708089 seconds
K4-C Put: 0.758977 seconds
K4-C Get: 0.519570 seconds
K4-C Get: 0.540054 seconds
K4-C Get: 0.600310 seconds
K4-C Get: 0.671604 seconds
K4-C Delete: 0.665265 seconds
K4-C Delete: 0.657199 seconds
K4-C Delete: 0.664664 seconds
K4-C Delete: 0.611774 seconds
K4-GO Put: 0.019521 seconds
K4-GO Put: 0.020370 seconds
K4-GO Put: 0.020771 seconds
K4-GO Put: 0.021061 seconds
K4-GO Get: 0.005092 seconds
K4-GO Get: 0.004579 seconds
K4-GO Get: 0.004092 seconds
K4-GO Get: 0.004963 seconds
K4-GO Delete: 0.010580 seconds
K4-GO Delete: 0.009743 seconds
K4-GO Delete: 0.010905 seconds
K4-GO Delete: 0.011150 seconds



```