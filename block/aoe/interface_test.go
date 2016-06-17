package aoe

import (
	"net"
	"sync"
	"syscall"
	"testing"
)

func TestInterfaceClose(t *testing.T) {
	ai, err := NewInterface(findLoopbackInterface(t))
	if err != nil {
		if err == syscall.EPERM {
			t.Skipf("test must be run as root: %v", err)
		}
		t.Fatalf("%T %+v", err, err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for {
			b := make([]byte, 1)
			_, _, err := ai.ReadFrom(b)
			if err != nil {
				t.Errorf("%T %+v", err, err)
				break
			}
		}
		wg.Done()
	}()

	if err := ai.Close(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}

func findLoopbackInterface(t *testing.T) string {
	for _, name := range []string{"lo", "lo0"} {
		if _, err := net.InterfaceByName(name); err == nil {
			return name
		}
	}

	t.Skip("could not find suitable loopback interface for test")
	return ""
}
