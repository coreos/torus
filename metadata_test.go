package agro

import (
	"errors"
	"testing"
)

func BenchmarkGetDepth(b *testing.B) {
	path := Path{
		Path: "/full/path/to/a/thing/in/storage/somewhere/",
	}

	for i := 0; i < b.N; i++ {
		path.GetDepth()
	}
}

func TestGetDepth(t *testing.T) {
	path := Path{
		Path: "/full/path/to/a/thing/in/storage/somewhere/",
	}

	if path.GetDepth() != 8 {
		t.Fatal(errors.New("GetDepth returns incorrect result"))
	}
}
