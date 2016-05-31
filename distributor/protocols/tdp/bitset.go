package tdp

type bitset []byte

func (b bitset) set(i int) bitset {
	off := i / 8
	for len(b) <= off {
		b = append(b, 0x00)
	}
	b[off] = b[off] | (0x01 << uint(i%8))
	return b
}

func (b bitset) unset(i int) bitset {
	off := i / 8
	for len(b) <= off {
		b = append(b, 0x00)
	}
	b[off] = b[off] &^ (0x01 << uint(i%8))
	return b
}

func (b bitset) test(i int) bool {
	off := i / 8
	if len(b) <= off {
		return false
	}
	return (b[off] & (0x01 << uint(i%8))) != 0
}

func bitsetFromBool(in []bool) bitset {
	b := bitset{}
	for i := 0; i < len(in); i++ {
		if in[i] {
			b = b.set(i)
		} else {
			b = b.unset(i)
		}
	}
	return b
}

func (b bitset) toBool(len int) []bool {
	out := make([]bool, len)
	for i := 0; i < len; i++ {
		s := b.test(i)
		out[i] = s
	}
	return out
}
