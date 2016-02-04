package memory

import (
	"testing"

	"github.com/coreos/agro"
)

func TestGetVolumes(t *testing.T) {
	m, _ := newMemoryMetadata(agro.Config{})

	for _, volume := range []string{"foo", "bar", "zoop", "foot"} {
		if err := m.CreateVolume(volume); err != nil {
			t.Fatal(err)
		}
		if err := m.Mkdir(agro.Path{Volume: volume, Path: "/example/"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	actual, err := m.GetVolumes()
	if err != nil {
		t.Fatal(err)
	}
	for i, expected := range []string{"bar", "foo", "foot", "zoop"} {
		if actual[i] != expected {
			t.Fatalf("%q != %q; %v", actual[i], expected, actual)
		}
	}
}

func TestGetdir(t *testing.T) {
	m, _ := newMemoryMetadata(agro.Config{})

	for _, volume := range []string{"foo", "bar", "zoop", "foot"} {
		if err := m.CreateVolume(volume); err != nil {
			t.Fatal(err)
		}
		for _, dir := range []string{"/example/", "/example/first/", "/example/second/", "/example/third/"} {
			if err := m.Mkdir(agro.Path{Volume: volume, Path: dir}, nil); err != nil {
				t.Fatal(err)
			}
		}
	}

	dir, subdirs, err := m.Getdir(agro.Path{Volume: "foo", Path: "/example/"})
	if err != nil {
		t.Fatal(err)
	}
	if dir == nil {
		t.Fatal("dir was nil, should stay nil")
	}
	for i, expected := range []string{"/example/first/", "/example/second/", "/example/third/"} {
		if subdirs[i].Volume != "foo" {
			t.Fatal("wrong volume")
		}
		if subdirs[i].Path != expected {
			t.Fatalf("%q != %q; %v", subdirs[i].Path, expected, subdirs)
		}
	}
}
