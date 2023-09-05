package hamt

import (
	"fmt"
	"io"
	"sync/atomic"

	cbg "github.com/whyrusleeping/cbor-gen"
)

// test utilities
// from https://github.com/polydawn/refmt/blob/3d65705ee9f12dc0dfcc0dc6cf9666e97b93f339/cbor/cborFixtures_test.go#L81-L93

func bcat(bss ...[]byte) []byte {
	l := 0
	for _, bs := range bss {
		l += len(bs)
	}
	rbs := make([]byte, 0, l)
	for _, bs := range bss {
		rbs = append(rbs, bs...)
	}
	return rbs
}

func b(b byte) []byte { return []byte{b} }

// A CBOR-marshalable byte array.
type CborByteArray []byte

func (c *CborByteArray) MarshalCBOR(w io.Writer) error {
	if err := cbg.WriteMajorTypeHeader(w, cbg.MajByteString, uint64(len(*c))); err != nil {
		return err
	}
	_, err := w.Write(*c)
	return err
}

func (c *CborByteArray) UnmarshalCBOR(r io.Reader) error {
	maj, extra, err := cbg.CborReadHeader(r)
	if err != nil {
		return err
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	if uint64(cap(*c)) < extra {
		*c = make([]byte, extra)
	}
	if _, err := io.ReadFull(r, *c); err != nil {
		return err
	}
	return nil
}

func cborstr(s string) *CborByteArray {
	v := CborByteArray(s)
	return &v
}

var _ cbg.CBORUnmarshaler = &CBORSinkCounter{}

type CBORSinkCounter struct {
	calledTimes uint64
}

func (c *CBORSinkCounter) UnmarshalCBOR(reader io.Reader) error {
	atomic.AddUint64(&c.calledTimes, 1)
	return nil
}

func uint64Pow(n, m uint64) uint64 {
	if m == 0 {
		return 1
	}
	result := n
	for i := uint64(2); i <= m; i++ {
		result *= n
	}
	return result
}

func toChar(i int) rune {
	return rune('A' + i)
}
