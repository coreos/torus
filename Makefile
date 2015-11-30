build:
	go build ./cmd/agro

run:
	./agro -debug

clean:
	rm -rf /tmp/agro
	rm -rf /tmp/agro2
	rm -rf /tmp/etcd

etcdrun:
	goreman start

cleanrun: clean run
