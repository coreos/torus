package temp

import (
	"testing"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

func TestGetVolumes(t *testing.T) {
	m, _ := NewTemp(agro.Config{})

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
	m, _ := NewTemp(agro.Config{})

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
	if dir != nil {
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

func TestRebalanceChannels(t *testing.T) {
	cfg := agro.Config{}
	m := NewServer()
	cs := [3]*Client{
		NewClient(cfg, m),
		NewClient(cfg, m),
		NewClient(cfg, m),
	}
	ok := make(chan bool)
	for i := 0; i < len(cs); i++ {
		go func(i int) {
			inOut, elected, _ := cs[i].OpenRebalanceChannels()
			if elected {
				time.Sleep(100 * time.Millisecond)
				inOut[1] <- &models.RebalanceStatus{
					Phase: 3,
				}
				for j := 0; j < len(cs); j++ {
					s := <-inOut[0]
					if s.Phase != 3 {
						t.Fatal("unexpected phase")
						ok <- false
						return
					}
				}
				close(inOut[1])
				ok <- true
			} else {
				d := <-inOut[0]
				inOut[1] <- d
				close(inOut[1])
				ok <- true
			}
		}(i)
	}
	for j := 0; j < len(cs); j++ {
		p := <-ok
		if !p {
			t.Fatal("failed rebalance round")
		}
	}
	close(ok)
}
