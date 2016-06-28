ifeq ($(origin VERSION), undefined)
	VERSION != git rev-parse --short HEAD
endif
HOST_GOOS=$(shell go env GOOS)
HOST_GOARCH=$(shell go env GOARCH)
REPOPATH = github.com/coreos/torus

VERBOSE_1 := -v
VERBOSE_2 := -v -x

build: vendor
	go build $(VERBOSE_$(V)) -o bin/torusd -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusd
	go build $(VERBOSE_$(V)) -o bin/torusctl -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusctl
	go build $(VERBOSE_$(V)) -o bin/torusblk -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/torusblk

test: tools/glide
	go test --race $(shell ./tools/glide novendor)

vet: tools/glide
	go vet $(shell ./tools/glide novendor)

fmt: tools/glide
	go fmt $(shell ./tools/glide novendor)

run:
	./bin/torusd --etcd 127.0.0.1:2379 --debug --debug-init --peer-address http://127.0.0.1:40000

clean:
	rm -rf ./local-cluster ./bin/torus*

cleanall: clean
	rm -rf /tmp/etcd bin tools vendor

etcdrun:
	./local/etcd/etcd --data-dir /tmp/etcd

run3:
	goreman start

release:
	mkdir -p release
	goxc -d ./release -tasks-=go-vet,go-test -os="linux darwin" -pv=$(VERSION) -build-ldflags="-X $(REPOPATH).Version=$(VERSION)" -resources-include="README.md,Documentation,LICENSE,contrib" -main-dirs-exclude="vendor,cmd/ringtool"

vendor: tools/glide
	./tools/glide install

tools/glide:
	@echo "Downloading glide"
	mkdir -p tools
	curl -L https://github.com/Masterminds/glide/releases/download/0.10.2/glide-0.10.2-$(HOST_GOOS)-$(HOST_GOARCH).tar.gz | tar -xz -C tools
	mv tools/$(HOST_GOOS)-$(HOST_GOARCH)/glide tools/glide
	rm -r tools/$(HOST_GOOS)-$(HOST_GOARCH)

help:
	@echo "Influential make variables"
	@echo "  V                 - Build verbosity {0,1,2}."
