package aoe

import (
	"sync"
	"syscall"
	"testing"
)

func TestInterfaceClose(t *testing.T) {
	ai, err := NewInterface("lo")
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
