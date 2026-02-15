package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "test_index")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	
	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())
	
	cases := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
	
	for _, c := range cases {
		err := idx.Write(c.Off, c.Pos)
		require.NoError(t, err)
		
		off, pos, err := idx.Read(int64(c.Off))
		require.NoError(t, err)
		require.Equal(t, off, c.Off)
		require.Equal(t, pos, c.Pos)
	}
	
	// index must error if reading out of bound offset
	_, _, err = idx.Read(int64(len(cases)))
	require.Equal(t, err, io.EOF)
	
	err = idx.Close()
	require.NoError(t, err)
	
	// index should be able to read existing file
	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, off, uint32(1))
	require.Equal(t, pos, cases[1].Pos)
}