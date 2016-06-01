Some good benchmarks to run:

Linear write speed

`dd if=/dev/zero of=/mnt/torus/testfile bs=1K count=4000000`

Traditional benchmark

`bonnie++ -d /mnt/torus/test -s 8G -u core`

Painful benchmark

`fio --randrepeat=1 --ioengine=libaio --direct=1 --gtod_reduce=1 --name=test --filename=test --bs=4k --iodepth=64 --size=4G --readwrite=randrw --rwmixread=75`
