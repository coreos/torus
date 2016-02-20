build:
	go build ./cmd/agro
	go build ./cmd/agroctl

run:
	./agro --etcd 127.0.0.1:2378 --debug --debug-mkfs --peer-address 127.0.0.1:40000

clean:
	rm -rf /srv/agro
	rm -rf /srv/agro2
	rm -rf /srv/agro3
	rm -rf /srv/agro4
	rm -rf /srv/agro5

cleanall: clean
	rm -rf /tmp/etcd

etcdrun:
	./local/etcd/etcd --experimental-v3demo --experimental-gRPC-addr 127.0.0.1:2378 --data-dir /tmp/etcd

run3:
	goreman start

run4: 
	./agro --etcd 127.0.0.1:2378 --debug --port 4324 --datadir /srv/agro4 --peer-address 0.0.0.0:40003

run5: 
	./agro --etcd 127.0.0.1:2378 --debug --port 4325 --datadir /srv/agro5 --peer-address 0.0.0.0:40004

cleanrun: clean run
