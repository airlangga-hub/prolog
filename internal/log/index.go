package log

import (
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	
	idx := &index{
		file: f,
		size: uint64(fi.Size()),
	}	
	
	idx.mmap, err = gommap.Map(
		f.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}
	
	return idx, nil
}

func (i *index) Close() error {
	
}