build:
	go build ./cmd/torus
	go build ./cmd/torusctl
	go build ./cmd/torusblock

run:
	./torus --etcd 127.0.0.1:2379 --debug --debug-init --peer-address 127.0.0.1:40000

clean:
	rm -rf ./local-cluster

cleanall: clean
	rm -rf /tmp/etcd

etcdrun:
	./local/etcd/etcd --data-dir /tmp/etcd

run3:
	goreman start

run4: 
	./torus --etcd 127.0.0.1:2379 --debug --port 4324 --datadir /srv/torus4 --peer-address 0.0.0.0:40003

run5: 
	./torus --etcd 127.0.0.1:2379 --debug --port 4325 --datadir /srv/torus5 --peer-address 0.0.0.0:40004

cleanrun: clean run
