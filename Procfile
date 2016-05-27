torus1: ./torus --etcd 127.0.0.1:2379 --debug --debug-init --port 4321 --datadir local-cluster/torus1  --peer-address http://127.0.0.1:40000 --size 5GiB --auto-join
torus2: ./torus --etcd 127.0.0.1:2379 --debug --debug-init --port 4322 --datadir local-cluster/torus2 --peer-address adp://127.0.0.1:40001 --size 5GiB --writelevel one --auto-join
torus3: ./torus --etcd 127.0.0.1:2379 --debug --debug-init --port 4323 --datadir local-cluster/torus3 --peer-address http://127.0.0.1:40002 --size 5GiB --read-cache-size=200MiB --auto-join
#torus4: ./torus --etcd 127.0.0.1:2379 --debug --debug-init --port 4324 --datadir local-cluster/torus4 --peer-address 127.0.0.1:40003 --size 5GiB --read-cache-size=200MiB --auto-join
#torus5: ./torus --etcd 127.0.0.1:2379 --debug --debug-init --port 4325 --datadir local-cluster/torus5 --peer-address 127.0.0.1:40004 --size 5GiB --read-cache-size=200MiB --auto-join
#torusblock: sudo ./torusblock nbd blockvol /dev/nbd2 --write-level local --write-cache-size 1GiB
