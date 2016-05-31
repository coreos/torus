torus1: ./torusd --etcd 127.0.0.1:2379 --debug --debug-init --port 4321 --data-dir local-cluster/torus1  --peer-address http://127.0.0.1:40000 --size 5GiB --auto-join --host 127.0.0.1
torus2: ./torusd --etcd 127.0.0.1:2379 --debug --debug-init --port 4322 --data-dir local-cluster/torus2 --peer-address http://127.0.0.1:40001 --size 5GiB --writelevel one --auto-join --host 127.0.0.1
torus3: ./torusd --etcd 127.0.0.1:2379 --debug --debug-init --port 4323 --data-dir local-cluster/torus3 --peer-address http://127.0.0.1:40002 --size 5GiB --read-cache-size=200MiB --auto-join --host 127.0.0.1
#torus4: ./torusd --etcd 127.0.0.1:2379 --debug --debug-init --port 4324 --data-dir local-cluster/torus4 --peer-address 127.0.0.1:40003 --size 5GiB --read-cache-size=200MiB --auto-join
#torus5: ./torusd --etcd 127.0.0.1:2379 --debug --debug-init --port 4325 --data-dir local-cluster/torus5 --peer-address 127.0.0.1:40004 --size 5GiB --read-cache-size=200MiB --auto-join
#torusblk: sudo ./torusblk nbd blockvol /dev/nbd2 --write-level local --write-cache-size 1GiB
