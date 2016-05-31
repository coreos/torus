package tdp

import (
	"math/rand"
	"testing"
)

func TestZeroLen(t *testing.T) {
	b := bitset{}
	out := b.toBool(3)
	if len(out) != 3 {
		t.Fatal("wrong length")
	}
	for _, x := range out {
		if x {
			t.Fatal("true value")
		}
	}
}

func TestSetOne(t *testing.T) {
	b := bitset{}
	b = b.set(10)
	out := b.toBool(11)
	if out[0] || !out[10] {
		t.Fatal("mismatch")
	}
}

func TestSetRandom(t *testing.T) {
	length := 1000
	truth := make([]bool, 1000)
	for i := 0; i < length; i++ {
		t := rand.Int()%2 == 1
		truth[i] = t
	}
	retruth := bitsetFromBool(truth)
	out := retruth.toBool(length)
	if len(out) != len(truth) {
		t.Fatal("wrong length")
	}
	for i := 0; i < len(out); i++ {
		if truth[i] != out[i] {
			t.Fatal("mismatch")
		}
	}
}
