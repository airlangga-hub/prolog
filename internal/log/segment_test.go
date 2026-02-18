package log

import (
	"io"
	"os"
	"testing"

	log_v1 "github.com/airlangga-hub/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	
	want := &log_v1.Record{Value: []byte("hello world")}
	
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3
	
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, s.nextOffset, uint64(16))
	require.False(t, s.IsMaxed())
	
	for i := range 3 {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, off, uint64(16+i))
		
		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, got.Value, want.Value)
	}
	
	// maxed index
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())
	
	c.Segment.MaxStoreBytes = uint64(len(want.Value)) * 3
	c.Segment.MaxIndexBytes = 1024
	
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// maxed store
	require.True(t, s.IsMaxed())
	
	// remove
	err = s.Remove()
	require.NoError(t, err)
	
	// recreate store and index files
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}