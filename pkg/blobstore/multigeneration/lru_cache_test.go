package multigeneration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyLRUCache(t *testing.T) {
	cache := NewLRUCache(32)
	require.Equal(t, cache.count, uint32(0))
	require.Nil(t, cache.newest, nil)
	require.Nil(t, cache.oldest, nil)
}

func TestOneElementLRUCache(t *testing.T) {
	cache := NewLRUCache(32)
	cache.Add("foo")
	require.Equal(t, cache.count, uint32(1))
	require.NotNil(t, cache.newest, nil)
	require.NotNil(t, cache.oldest, nil)
	require.True(t, cache.Has("foo"))
	require.False(t, cache.Has("bar"))
	cache.Prune()
	require.Equal(t, cache.count, uint32(0))
	require.Nil(t, cache.newest)
	require.Nil(t, cache.oldest)
}

func TestTwoElementsLRUCache(t *testing.T) {
	cache := NewLRUCache(32)
	cache.Add("foo")
	cache.Add("bar")
	require.Equal(t, cache.count, uint32(2))
	require.NotNil(t, cache.newest)
	require.NotNil(t, cache.oldest)
	require.True(t, cache.Has("foo"))
	require.True(t, cache.Has("bar"))
	require.Equal(t, cache.newest.key, "bar")
	require.Equal(t, cache.oldest.key, "foo")

	require.Equal(t, cache.oldest.newer, cache.newest)

	foo := cache.elements["foo"]

	cache.removeFromList(foo)

	require.Equal(t, cache.newest, cache.oldest)
	require.NotNil(t, cache.newest)
	require.NotNil(t, cache.oldest)

	cache.addToFront(foo)

	require.Equal(t, cache.newest.key, "foo")
	require.NotNil(t, foo.older)
	require.Nil(t, foo.newer)
	require.Equal(t, cache.oldest.key, "bar")

	bar := cache.elements["bar"]

	cache.touch(bar)
	require.Equal(t, cache.newest.key, "bar")
	require.Equal(t, cache.oldest.key, "foo")

	cache.Prune()
	require.Equal(t, cache.count, uint32(1))
	require.NotNil(t, cache.newest)
	require.NotNil(t, cache.oldest)
	require.Equal(t, cache.newest.key, "bar")
	require.Equal(t, cache.oldest.key, "bar")

	cache.Prune()
	require.Equal(t, cache.count, uint32(0))
	require.Nil(t, cache.newest)
	require.Nil(t, cache.oldest)

	require.NotPanics(t, cache.Prune)
	require.False(t, cache.Has("foo"))
}

func TestLRUCache(t *testing.T) {
	cache := NewLRUCache(32)

	keys := []string{
		"foo", "bar", "baz", "bax",
	}

	for _, k := range keys {
		cache.Add(k)
	}

	require.Equal(t, "foo", cache.oldest.key)
	require.Equal(t, "bar", cache.oldest.newer.key)
	require.Equal(t, "baz", cache.oldest.newer.newer.key)
	require.Equal(t, "bax", cache.oldest.newer.newer.newer.key)

	require.Equal(t, "bax", cache.newest.key)
	require.Equal(t, "baz", cache.newest.older.key)
	require.Equal(t, "bar", cache.newest.older.older.key)
	require.Equal(t, "foo", cache.newest.older.older.older.key)

	// move "baz" to the newest
	require.True(t, cache.Has("baz"))

	require.Equal(t, "baz", cache.newest.key)
	require.Equal(t, "bax", cache.newest.older.key)
	require.Equal(t, "bar", cache.newest.older.older.key)
	require.Equal(t, "foo", cache.newest.older.older.older.key)

	// touch the newest
	require.True(t, cache.Has("baz"))

	require.Equal(t, "baz", cache.newest.key)
	require.Equal(t, "bax", cache.newest.older.key)
	require.Equal(t, "bar", cache.newest.older.older.key)
	require.Equal(t, "foo", cache.newest.older.older.older.key)

	// touch the oldest
	require.True(t, cache.Has("foo"))
	require.Equal(t, "foo", cache.newest.key)
	require.Equal(t, "baz", cache.newest.older.key)
	require.Equal(t, "bax", cache.newest.older.older.key)
	require.Equal(t, "bar", cache.newest.older.older.older.key)

	require.Equal(t, uint32(4), cache.count)
}

func TestLRUCacheSize(t *testing.T) {
	cache := NewLRUCache(1)

	keys := []string{
		"foo", "bar", "baz", "bax",
	}

	for _, k := range keys {
		cache.Add(k)
		require.Equal(t, uint32(1), cache.count)
	}
}
