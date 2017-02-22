ifeq ($(origin VERSION), undefined)
	VERSION != git rev-parse --short HEAD
endif
HOST_GOOS=$(shell go env GOOS)
HOST_GOARCH=$(shell go env GOARCH)
REPOPATH = github.com/coreos/torus

VERBOSE_1 := -v
VERBOSE_2 := -v -x

WHAT := torusd torusctl torusblk mkfs.torus fsck.torus

build: vendor
	for target in $(WHAT); do \
		echo "building $$target..."; \
		$(BUILD_ENV_FLAGS) go build $(VERBOSE_$(V)) -o bin/$$target -ldflags "-X $(REPOPATH).Version=$(VERSION)" ./cmd/$$target; \
	done

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

release: releasetar
	goxc -d ./release -tasks-=go-vet,go-test -os="linux darwin" -pv=$(VERSION)  -arch="386 amd64 arm arm64" -build-ldflags="-X $(REPOPATH).Version=$(VERSION)" -resources-include="README.md,Documentation,LICENSE,contrib" -main-dirs-exclude="vendor,cmd/ringtool"

releasetar:
	mkdir -p release/$(VERSION)
	glide install --strip-vcs --strip-vendor --update-vendored --delete
	glide-vc --only-code --no-tests --keep="**/*.json.in"
	git ls-files > /tmp/torusbuild
	find vendor >> /tmp/torusbuild
	tar -cvf release/$(VERSION)/torus_$(VERSION)_src.tar -T /tmp/torusbuild --transform 's,^,torus_$(VERSION)/,'
	rm /tmp/torusbuild
	gzip release/$(VERSION)/torus_$(VERSION)_src.tar


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
	@echo "  BUILD_ENV_FLAGS   - Environment added to 'go build'."
	@echo "  WHAT              - Command to build. (e.g. WHAT=torusctl)"
