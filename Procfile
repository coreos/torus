agro1: ./agro --etcd 127.0.0.1:2379 --debug --debug-mkfs --port 4321 --data-dir local-cluster/agro1  --peer-address http://127.0.0.1:40000 --size 5GiB --auto-join
agro2: ./agro --etcd 127.0.0.1:2379 --debug --debug-mkfs --port 4322 --data-dir local-cluster/agro2 --peer-address adp://127.0.0.1:40001 --size 5GiB --writelevel one --auto-join
agro3: ./agro --etcd 127.0.0.1:2379 --debug --debug-mkfs --port 4323 --data-dir local-cluster/agro3 --peer-address http://127.0.0.1:40002 --size 5GiB --read-cache-size=200MiB --auto-join
#agro4: ./agro --etcd 127.0.0.1:2379 --debug --debug-mkfs --port 4324 --data-dir local-cluster/agro4 --peer-address 127.0.0.1:40003 --size 5GiB --read-cache-size=200MiB --auto-join
#agro5: ./agro --etcd 127.0.0.1:2379 --debug --debug-mkfs --port 4325 --data-dir local-cluster/agro5 --peer-address 127.0.0.1:40004 --size 5GiB --read-cache-size=200MiB --auto-join
#agromount: sudo ./agromount nbd blockvol /dev/nbd2 --write-level local --write-cache-size 1GiB
