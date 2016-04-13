agro1: ./agro --etcd 127.0.0.1:2378 --debug --debug-mkfs --port 4321 --datadir local-cluster/agro1  --peer-address 127.0.0.1:40000 --size 5GiB 
agro2: ./agro --etcd 127.0.0.1:2378 --debug --debug-mkfs --port 4322 --datadir local-cluster/agro2 --peer-address 127.0.0.1:40001 --size 5GiB --writelevel one
agro3: ./agro --etcd 127.0.0.1:2378 --debug --debug-mkfs --port 4323 --datadir local-cluster/agro3 --peer-address 127.0.0.1:40002 --size 5GiB --read-cache-size=200MiB
