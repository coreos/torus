build:
	go build ./cmd/agro
	go build ./cmd/agroctl
	go build ./cmd/agromount

run:
	./agro --etcd 127.0.0.1:2379 --debug --debug-mkfs --peer-address 127.0.0.1:40000

clean:
	rm -rf ./local-cluster

cleanall: clean
	rm -rf /tmp/etcd

etcdrun:
	./local/etcd/etcd --data-dir /tmp/etcd

run3:
	goreman start

run4: 
	./agro --etcd 127.0.0.1:2379 --debug --port 4324 --datadir /srv/agro4 --peer-address 0.0.0.0:40003

run5: 
	./agro --etcd 127.0.0.1:2379 --debug --port 4325 --datadir /srv/agro5 --peer-address 0.0.0.0:40004

cleanrun: clean run
