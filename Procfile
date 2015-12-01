etcd: ./local/etcd/etcd --experimental-v3demo --experimental-gRPC-addr 127.0.0.1:2378 --data-dir /tmp/etcd
agro: ./agro --etcd 127.0.0.1:2378 --debug --debug-mkfs --peer-address 127.0.0.1:40000
agro2: ./agro --etcd 127.0.0.1:2378 --debug --debug-mkfs --port 4322 --datadir /tmp/agro2
