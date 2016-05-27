ifeq ($(origin VERSION), undefined)
  VERSION != git rev-parse --short HEAD
endif
REPOPATH = github.com/coreos/torus

build:
	go build -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusd 
	go build -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusctl 
	go build -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusblk 

run:
	./torusd --etcd 127.0.0.1:2379 --debug --debug-init --peer-address 127.0.0.1:40000

clean:
	rm -rf ./local-cluster

cleanall: clean
	rm -rf /tmp/etcd

etcdrun:
	./local/etcd/etcd --data-dir /tmp/etcd

run3:
	goreman start

release:
	mkdir -p release
	goxc -d ./release -tasks-=go-vet,go-test -os="linux darwin" -pv=$(VERSION) -build-ldflags="-X $(REPOPATH).Version=$(VERSION)" -resources-include="README.md,docs,LICENSE,contrib" -main-dirs-exclude="vendor,cmd/ringtool"
