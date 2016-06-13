ifeq ($(origin VERSION), undefined)
	VERSION != git rev-parse --short HEAD
endif
GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)
REPOPATH = github.com/coreos/torus

build: vendor
	go build -o bin/torusd -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusd
	go build -o bin/torusctl -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusctl
	go build -o bin/torusblk -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusblk

test: bin/glide
	go test $(shell ./bin/glide novendor)

vet: bin/glide
	go vet $(shell ./bin/glide novendor)

fmt: bin/glide
	go fmt $(shell ./bin/glide novendor)

run:
	./bin/torusd --etcd 127.0.0.1:2379 --debug --debug-init --peer-address http://127.0.0.1:40000

clean:
	rm -rf ./local-cluster ./bin/torus*

cleanall: clean
	rm -rf /tmp/etcd bin vendor

etcdrun:
	./local/etcd/etcd --data-dir /tmp/etcd

run3:
	goreman start

release:
	mkdir -p release
	goxc -d ./release -tasks-=go-vet,go-test -os="linux darwin" -pv=$(VERSION) -build-ldflags="-X $(REPOPATH).Version=$(VERSION)" -resources-include="README.md,docs,LICENSE,contrib" -main-dirs-exclude="vendor,cmd/ringtool"

vendor: bin/glide
	./bin/glide install

bin/glide:
	@echo "Downloading glide"
	mkdir -p bin
	curl -L https://github.com/Masterminds/glide/releases/download/0.10.2/glide-0.10.2-$(GOOS)-$(GOARCH).tar.gz | tar -xz -C bin
	mv bin/$(GOOS)-$(GOARCH)/glide bin/glide
	rm -r bin/$(GOOS)-$(GOARCH)
