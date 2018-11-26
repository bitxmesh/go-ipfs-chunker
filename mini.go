package chunk

import (
	"io"

	pool "github.com/libp2p/go-buffer-pool"
)

// DefaultMiniBlockCount is the mini chunk count.
var DefaultMiniBlockCount uint32 = 128

// DefaultBlockSize is the mini chunk size.
var DefaultMiniBlockSize uint32 = 8

// Mini implements the Splitter interface.
type Mini struct {
	r    io.Reader
	size uint32
	err  error

	miniCur   uint32
	miniCount uint32
	miniSize  uint32
}

// DefaultMini creates a default Mini splitter.
func DefaultMini(r io.Reader) *Mini {
	return &Mini{
		r:         r,
		size:      uint32(DefaultBlockSize),
		miniCount: DefaultMiniBlockCount,
		miniSize:  DefaultMiniBlockSize,
	}
}

// NewMini creates a new Mini splitter.
func NewMini(r io.Reader, size int64, miniCount uint32, miniSize uint32) *Mini {
	return &Mini{
		r:         r,
		size:      uint32(size),
		miniCount: miniCount,
		miniSize:  miniSize,
	}
}

// NextBytes reads the next bytes from the reader and returns a slice.
func (m *Mini) NextBytes() ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}

	var size uint32
	if m.miniCur < m.miniCount {
		size = m.miniSize

		m.miniCur++
	} else {
		size = m.size
	}

	full := pool.Get(int(size))
	n, err := io.ReadFull(m.r, full)
	switch err {
	case io.ErrUnexpectedEOF:
		m.err = io.EOF
		small := make([]byte, n)
		copy(small, full)
		pool.Put(full)
		return small, nil
	case nil:
		return full, nil
	default:
		pool.Put(full)
		return nil, err
	}
}

// Reader returns the io.Reader associated to this Splitter.
func (m *Mini) Reader() io.Reader {
	return m.r
}
