build:
	go build ./cmd/agro
	go build ./cmd/agroctl

run:
	./agro --etcd 127.0.0.1:2378 --debug --debug-mkfs --peer-address 127.0.0.1:40000

clean:
	rm -rf /tmp/agro
	rm -rf /tmp/agro2
	rm -rf /tmp/agro3
	rm -rf /tmp/agro4

cleanall: clean
	rm -rf /tmp/etcd

etcdrun:
	./local/etcd/etcd --experimental-v3demo --experimental-gRPC-addr 127.0.0.1:2378 --data-dir /tmp/etcd

run3:
	goreman start

cleanrun: clean run
