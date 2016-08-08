
# [08/02/16 11:43]
Setup: 
* 6 node Type0 cluster on Packet, in New Jersey. These boxes contain 
  * Intel Atom C2550 (4 cores @ 2.4Ghz)
  * 8GB DDR3 RAM
  * 80G SSD
  * 1Gbit Ethernet
* Named: torus1-5, torus-nix (for login)
* Single etcd node on torus-nix, which will also be our mount point. 
* 50GiB storage assigned to Torus on each. 

* Create a 30GiB block volume. Replication level 2. 
* Replication level for all tests is "all" -- all blocks must be answered by both replicas to be considered valid.

Basic Test Commands:

ddwrite:
```
dd if=/dev/zero of=/mnt/torus/testfile bs=1K count=4000000
```
This will measure end-to-end throughput, with no reads or syncs. It asks, how fast can we possibly write? The reason for 4 gigs of data is that we need to break the filesystem cache, which is 1 gig

ddreadback:
```
dd if=/mnt/torus/testfile of=/dev/null bs=1K
```

Similarly, how fast can we read data, under perfect, linear conditions? Ideally this is the full speed of the machine's network interface.

fio:
```
fio --randrepeat=1 --ioengine=libaio --direct=1 --gtod_reduce=1 --name=test --filename=test --bs=64k --iodepth=64 --size=4G --readwrite=randrw --rwmixread=75
```

This is a more standard test. We're using non-buffered IO, a random mix of 75% reads, 25% writes with a repeatable workload, using 64k blocks across a 4Gig file. `fio` will also report the iops we're getting under this environment.

## Over gRPC as the peer-to-peer transport

### nbd

Mount: `mount -o discard,noatime /dev/nbd0 /mnt/`

ddwrite:
```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 114.954 s, 35.6 MB/s
```

ddreadback:
```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 68.0594 s, 60.2 MB/s
```

fio:

```
Jobs: 1 (f=1): [m(1)] [100.0% done] [4800KB/1920KB/0KB /s] [75/30/0 iops] [eta 00m:00s]
test: (groupid=0, jobs=1): err= 0: pid=11846: Tue Aug  2 19:19:50 2016
  read : io=3075.6MB, bw=3917.4KB/s, iops=61, runt=803952msec
    bw (KB  /s): min=  128, max= 9984, per=100.00%, avg=4137.05, stdev=1539.52
  write: io=1020.5MB, bw=1299.8KB/s, iops=20, runt=803952msec
    bw (KB  /s): min=  128, max= 3840, per=100.00%, avg=1378.23, stdev=517.29
  cpu          : usr=0.16%, sys=0.46%, ctx=59077, majf=0, minf=10
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=99.9%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued    : total=r=49209/w=16327/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: io=3075.6MB, aggrb=3917KB/s, minb=3917KB/s, maxb=3917KB/s, mint=803952msec, maxt=803952msec
  WRITE: io=1020.5MB, aggrb=1299KB/s, minb=1299KB/s, maxb=1299KB/s, mint=803952msec, maxt=803952msec

Disk stats (read/write):
  nbd0: ios=49203/16640, merge=0/158, ticks=35785426/12404420, in_queue=48192990, util=100.00%
```

### TCMU

ddwrite:
```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 110.776 s, 37.0 MB/s
```

ddreadback:
```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 78.8019 s, 52.0 MB/s
```

fio:
```
Jobs: 1 (f=1): [m(1)] [99.9% done] [5824KB/1984KB/0KB /s] [91/31/0 iops] [eta 00m:01s] 
test: (groupid=0, jobs=1): err= 0: pid=12559: Tue Aug  2 19:44:18 2016
  read : io=3075.6MB, bw=3753.1KB/s, iops=58, runt=838946msec
    bw (KB  /s): min=  128, max= 8370, per=100.00%, avg=3816.18, stdev=1148.33
  write: io=1020.5MB, bw=1245.6KB/s, iops=19, runt=838946msec
    bw (KB  /s): min=  128, max= 3902, per=100.00%, avg=1272.49, stdev=373.17
  cpu          : usr=0.16%, sys=0.54%, ctx=113630, majf=0, minf=10
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=99.9%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued    : total=r=49209/w=16327/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: io=3075.6MB, aggrb=3753KB/s, minb=3753KB/s, maxb=3753KB/s, mint=838946msec, maxt=838946msec
  WRITE: io=1020.5MB, aggrb=1245KB/s, minb=1245KB/s, maxb=1245KB/s, mint=838946msec, maxt=838946msec

Disk stats (read/write):
  sdb: ios=49162/17087, merge=24/307, ticks=39009063/13989071, in_queue=53026598, util=100.00%
```

## Using experimental TDP as peer-to-peer transport

### nbd


ddwrite:
```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 67.859 s, 60.4 MB/s
```

ddreadback:
```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 30.9295 s, 132 MB/s
```

^ This result seems odd, until you realize that one node (the node that we're mounted on) is faster, in theory than the 1Gbit connection we can pull from the others, and this averages out.

fio:
```
Jobs: 1 (f=1): [m(1)] [100.0% done] [21781KB/7879KB/0KB /s] [340/123/0 iops] [eta 00m:00s]
test: (groupid=0, jobs=1): err= 0: pid=14652: Tue Aug  2 21:10:30 2016
  read : io=3075.6MB, bw=8692.1KB/s, iops=135, runt=362292msec
    bw (KB  /s): min=  128, max=22718, per=100.00%, avg=9005.05, stdev=4669.41
  write: io=1020.5MB, bw=2884.3KB/s, iops=45, runt=362292msec
    bw (KB  /s): min=  128, max= 8048, per=100.00%, avg=2979.01, stdev=1567.69
  cpu          : usr=0.29%, sys=0.99%, ctx=64282, majf=0, minf=10
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=99.9%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued    : total=r=49209/w=16327/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: io=3075.6MB, aggrb=8692KB/s, minb=8692KB/s, maxb=8692KB/s, mint=362292msec, maxt=362292msec
  WRITE: io=1020.5MB, aggrb=2884KB/s, minb=2884KB/s, maxb=2884KB/s, mint=362292msec, maxt=362292msec

Disk stats (read/write):
  nbd0: ios=49186/17277, merge=1/85, ticks=16619499/6098985, in_queue=22720994, util=100.00%
```


### TCMU

ddwrite:

```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 66.5539 s, 61.5 MB/s
```

ddreadback:
```
4000000+0 records in
4000000+0 records out
4096000000 bytes (4.1 GB, 3.8 GiB) copied, 32.5257 s, 126 MB/s
```

fio:
```
Jobs: 1 (f=1): [m(1)] [100.0% done] [15168KB/5056KB/0KB /s] [237/79/0 iops] [eta 00m:00s]
test: (groupid=0, jobs=1): err= 0: pid=14518: Tue Aug  2 20:56:37 2016
  read : io=3075.6MB, bw=8528.6KB/s, iops=133, runt=369274msec
    bw (KB  /s): min=  256, max=18944, per=100.00%, avg=8634.52, stdev=4344.79
  write: io=1020.5MB, bw=2829.7KB/s, iops=44, runt=369274msec
    bw (KB  /s): min=  128, max= 6898, per=100.00%, avg=2852.29, stdev=1454.46
  cpu          : usr=0.35%, sys=1.28%, ctx=128498, majf=0, minf=10
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=99.9%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued    : total=r=49209/w=16327/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: io=3075.6MB, aggrb=8528KB/s, minb=8528KB/s, maxb=8528KB/s, mint=369274msec, maxt=369274msec
  WRITE: io=1020.5MB, aggrb=2829KB/s, minb=2829KB/s, maxb=2829KB/s, mint=369274msec, maxt=369274msec

Disk stats (read/write):
  sdb: ios=49202/16486, merge=0/106, ticks=17240421/6205976, in_queue=23452541, util=100.00%
```
