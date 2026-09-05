package vfscache

// FIXME need to test async writeback here

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/ranges"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var zeroes = string(make([]byte, 100))

func newItemTestCache(t *testing.T) (r *fstest.Run, c *Cache) {
	opt := vfscommon.Opt

	// Disable the cache cleaner as it interferes with these tests
	opt.CachePollInterval = 0

	// Disable synchronous write
	opt.WriteBack = 0

	// Disable handle caching so existing tests get immediate close behavior
	opt.HandleCaching = 0

	return newTestCacheOpt(t, opt)
}

// Check the object has contents
func checkObject(t *testing.T, r *fstest.Run, remote string, contents string) {
	obj, err := r.Fremote.NewObject(context.Background(), remote)
	require.NoError(t, err)
	in, err := obj.Open(context.Background())
	require.NoError(t, err)
	buf, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	assert.Equal(t, contents, string(buf))
}

func newFileLength(t *testing.T, r *fstest.Run, c *Cache, remote string, length int) (contents string, obj fs.Object, item *Item) {
	contents = random.String(length)
	r.WriteObject(context.Background(), remote, contents, time.Now())
	item, _ = c.get(remote)
	obj, err := r.Fremote.NewObject(context.Background(), remote)
	require.NoError(t, err)
	return
}

func newFile(t *testing.T, r *fstest.Run, c *Cache, remote string) (contents string, obj fs.Object, item *Item) {
	return newFileLength(t, r, c, remote, 100)
}

func TestItemExists(t *testing.T) {
	_, c := newItemTestCache(t)
	item, _ := c.get("potato")

	assert.False(t, item.Exists())
	require.NoError(t, item.Open(nil))
	assert.True(t, item.Exists())
	require.NoError(t, item.Close(nil))
	assert.True(t, item.Exists())
	item.remove("test")
	assert.False(t, item.Exists())
}

func TestItemGetSize(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")
	require.NoError(t, item.Open(nil))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	n, err := item.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	size, err = item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(5), size)

	require.NoError(t, item.Close(nil))
	checkObject(t, r, "potato", "hello")
}

func TestItemDirty(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")
	require.NoError(t, item.Open(nil))

	assert.Equal(t, false, item.IsDirty())

	n, err := item.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	assert.Equal(t, true, item.IsDirty())

	require.NoError(t, item.Close(nil))

	// Sync writeback so expect clean here
	assert.Equal(t, false, item.IsDirty())

	item.Dirty()

	assert.Equal(t, true, item.IsDirty())
	checkObject(t, r, "potato", "hello")
}

func TestItemSync(t *testing.T) {
	_, c := newItemTestCache(t)
	item, _ := c.get("potato")

	require.Error(t, item.Sync())

	require.NoError(t, item.Open(nil))

	require.NoError(t, item.Sync())

	require.NoError(t, item.Close(nil))
}

func TestItemTruncateNew(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")

	require.Error(t, item.Truncate(0))

	require.NoError(t, item.Open(nil))

	require.NoError(t, item.Truncate(100))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(100), size)

	// Check the Close callback works
	callbackCalled := false
	callback := func(o fs.Object) {
		callbackCalled = true
		assert.Equal(t, "potato", o.Remote())
		assert.Equal(t, int64(100), o.Size())
	}
	require.NoError(t, item.Close(callback))
	assert.True(t, callbackCalled)

	checkObject(t, r, "potato", zeroes)
}

func TestItemTruncateExisting(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")

	require.Error(t, item.Truncate(40))
	checkObject(t, r, "existing", contents)

	require.NoError(t, item.Open(obj))

	require.NoError(t, item.Truncate(40))

	require.NoError(t, item.Truncate(60))

	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", contents[:40]+zeroes[:20])
}

func TestItemReadAt(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	buf := make([]byte, 10)

	_, err := item.ReadAt(buf, 10)
	require.Error(t, err)

	require.NoError(t, item.Open(obj))

	n, err := item.ReadAt(buf, 10)
	assert.Equal(t, 10, n)
	require.NoError(t, err)
	assert.Equal(t, contents[10:20], string(buf[:n]))

	n, err = item.ReadAt(buf, 95)
	assert.Equal(t, 5, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, contents[95:], string(buf[:n]))

	n, err = item.ReadAt(buf, 1000)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, contents[:0], string(buf[:n]))

	n, err = item.ReadAt(buf, -1)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, contents[:0], string(buf[:n]))

	require.NoError(t, item.Close(nil))
}

func TestItemWriteAtNew(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")
	buf := make([]byte, 10)

	_, err := item.WriteAt(buf, 10)
	require.Error(t, err)

	require.NoError(t, item.Open(nil))

	assert.Equal(t, int64(0), item.getDiskSize())

	n, err := item.WriteAt([]byte("HELLO"), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// FIXME we account for the sparse data we've "written" to
	// disk here so this is actually 5 bytes higher than expected
	assert.Equal(t, int64(15), item.getDiskSize())

	n, err = item.WriteAt([]byte("THEND"), 20)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	assert.Equal(t, int64(25), item.getDiskSize())

	require.NoError(t, item.Close(nil))

	checkObject(t, r, "potato", zeroes[:10]+"HELLO"+zeroes[:5]+"THEND")
}

func TestItemWriteAtExisting(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")

	require.NoError(t, item.Open(obj))

	n, err := item.WriteAt([]byte("HELLO"), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	n, err = item.WriteAt([]byte("THEND"), 95)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	n, err = item.WriteAt([]byte("THEVERYEND"), 120)
	require.NoError(t, err)
	assert.Equal(t, 10, n)

	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", contents[:10]+"HELLO"+contents[15:95]+"THEND"+zeroes[:20]+"THEVERYEND")
}

func TestItemLoadMeta(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))
	require.NoError(t, item.Close(nil))
	info := item.info

	// Remove the item from the cache
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()

	// Reload the item so we have to load the metadata
	item2, _ := c._get("existing")
	require.NoError(t, item2.Open(obj))
	info2 := item.info
	require.NoError(t, item2.Close(nil))

	// Check that the item is different
	assert.NotEqual(t, item, item2)
	// ... but the info is the same
	assert.Equal(t, info, info2)
}

func TestItemReload(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))

	// Make it dirty
	n, err := item.WriteAt([]byte("THEENDMYFRIEND"), 95)
	require.NoError(t, err)
	assert.Equal(t, 14, n)
	assert.True(t, item.IsDirty())

	// Close the file to pacify Windows, but don't call item.Close()
	item.mu.Lock()
	require.NoError(t, item.fd.Close())
	item.fd = nil
	item.mu.Unlock()

	// Remove the item from the cache
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()

	// Reload the item so we have to load the metadata and restart
	// the transfer
	item2, _ := c._get("existing")
	require.NoError(t, item2.reload(context.Background()))
	assert.False(t, item2.IsDirty())

	// Check that the item is different
	assert.NotEqual(t, item, item2)

	// And check the contents got written back to the remote
	checkObject(t, r, "existing", contents[:95]+"THEENDMYFRIEND")

	// And check that AddVirtual was called
	assert.Equal(t, []avInfo{
		{Remote: "existing", Size: 109, IsDir: false},
	}, avInfos)
}

func TestItemReloadRemoteGone(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(100), size)

	// Read something to instantiate the cache file
	buf := make([]byte, 10)
	_, err = item.ReadAt(buf, 10)
	require.NoError(t, err)

	// Test cache file present
	_, err = os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)

	require.NoError(t, item.Close(nil))

	// Remove the remote object
	require.NoError(t, obj.Remove(context.Background()))

	// Re-open with no object
	require.NoError(t, item.Open(nil))

	// Check size is now 0
	size, err = item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// Test cache file is now empty
	fi, err := os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)
	assert.Equal(t, int64(0), fi.Size())

	require.NoError(t, item.Close(nil))
}

func TestItemReloadCacheStale(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(100), size)

	// Read something to instantiate the cache file
	buf := make([]byte, 10)
	_, err = item.ReadAt(buf, 10)
	require.NoError(t, err)

	// Test cache file present
	_, err = os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)

	require.NoError(t, item.Close(nil))

	// Update the remote to something different
	contents2, obj, item := newFileLength(t, r, c, "existing", 110)
	assert.NotEqual(t, contents, contents2)

	// Re-open with updated object
	oldFingerprint := item.info.Fingerprint
	assert.NotEqual(t, "", oldFingerprint)
	require.NoError(t, item.Open(obj))

	// Make sure fingerprint was updated
	assert.NotEqual(t, oldFingerprint, item.info.Fingerprint)
	assert.NotEqual(t, "", item.info.Fingerprint)

	// Check size is now 110
	size, err = item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(110), size)

	// Test cache file is now correct size
	fi, err := os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)
	assert.Equal(t, int64(110), fi.Size())

	// Write to the file to make it dirty
	// This checks we aren't reusing stale data
	n, err := item.WriteAt([]byte("HELLO"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, true, item.IsDirty())

	require.NoError(t, item.Close(nil))

	// Now check with all that swizzling stuff around that the
	// object is correct

	checkObject(t, r, "existing", "HELLO"+contents2[5:])
}

// TestItemReloadDirtyBeyondRemote checks that reloading a dirty cache
// item whose cache file has grown larger than the remote object
// recovers what it can from the cache file instead of failing with
// "invalid seek position".
func TestItemReloadDirtyBeyondRemote(t *testing.T) {
	r, c := newItemTestCache(t)

	// Small remote object (100 bytes)
	_, obj, item := newFile(t, r, c, "existing")

	// Open it and write over and past the end of the remote object,
	// growing the cache file to 200 bytes and making it dirty.
	require.NoError(t, item.Open(obj))
	newContents := random.String(200)
	n, err := item.WriteAt([]byte(newContents), 0)
	require.NoError(t, err)
	assert.Equal(t, 200, n)
	assert.True(t, item.IsDirty())

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(200), size)

	// Simulate an unclean shutdown: the cache file on disk is fully grown
	// but the final range-metadata update was never persisted, so Rs
	// under-reports the parts of the file that are present. Here the
	// remaining present range [0,150) ends beyond the 100 byte remote
	// object, so the missing range [150,200) starts past the end of the
	// remote object.
	item.mu.Lock()
	item.info.Rs = item.info.Rs.Intersection(ranges.Range{Pos: 0, Size: 150})
	require.NoError(t, item._save())
	require.NoError(t, item.fd.Close())
	item.fd = nil
	item.mu.Unlock()

	// Drop the item so reload has to reload the metadata from disk
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()

	// Reload the dirty item. Before the fix this failed trying to download
	// the missing [150,200) range from the 100 byte remote object with
	// "invalid seek position".
	item2, _ := c._get("existing")
	require.NoError(t, item2.reload(context.Background()))
	assert.False(t, item2.IsDirty())

	// The grown contents are recovered from the cache file and written
	// back to the remote.
	checkObject(t, r, "existing", newContents)
}

func TestItemReadWrite(t *testing.T) {
	r, c := newItemTestCache(t)
	const (
		size     = 50*1024*1024 + 123
		fileName = "large"
	)

	item, _ := c.get(fileName)
	require.NoError(t, item.Open(nil))

	// Create the test file
	in := readers.NewPatternReader(size)
	buf := make([]byte, 1024*1024)
	buf2 := make([]byte, 1024*1024)
	offset := int64(0)
	for {
		n, err := in.Read(buf)
		n2, err2 := item.WriteAt(buf[:n], offset)
		offset += int64(n2)
		require.NoError(t, err2)
		require.Equal(t, n, n2)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Check it is the right size
	readSize, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(size), readSize)

	require.NoError(t, item.Close(nil))

	assert.False(t, item.remove(fileName))

	obj, err := r.Fremote.NewObject(context.Background(), fileName)
	require.NoError(t, err)
	assert.Equal(t, int64(size), obj.Size())

	// read and check a block of size N at offset
	// It returns eof true if the end of file has been reached
	readCheckBuf := func(t *testing.T, in io.ReadSeeker, buf, buf2 []byte, item *Item, offset int64, N int) (n int, eof bool) {
		what := fmt.Sprintf("buf=%p, buf2=%p, item=%p, offset=%d, N=%d", buf, buf2, item, offset, N)
		n, err := item.ReadAt(buf, offset)

		_, err2 := in.Seek(offset, io.SeekStart)
		require.NoError(t, err2, what)
		n2, err2 := in.Read(buf2[:n])
		require.Equal(t, n, n2, what)
		assert.Equal(t, buf[:n], buf2[:n2], what)
		assert.Equal(t, buf[:n], buf2[:n2], what)

		if err == io.EOF {
			return n, true
		}
		require.NoError(t, err, what)
		require.NoError(t, err2, what)
		return n, false
	}
	readCheck := func(t *testing.T, item *Item, offset int64, N int) (n int, eof bool) {
		return readCheckBuf(t, in, buf, buf2, item, offset, N)
	}

	// Read it back sequentially
	t.Run("Sequential", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		offset := int64(0)
		for {
			n, eof := readCheck(t, item, offset, len(buf))
			offset += int64(n)
			if eof {
				break
			}
		}
		assert.Equal(t, int64(size), offset)
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})

	// Read it back randomly
	t.Run("Random", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		for !item.present() {
			blockSize := rand.Intn(len(buf))
			offset := max(rand.Int63n(size+2*int64(blockSize))-int64(blockSize), 0)
			_, _ = readCheck(t, item, offset, blockSize)
		}
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})

	// Read it back randomly concurrently
	t.Run("RandomConcurrent", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				in := readers.NewPatternReader(size)
				buf := make([]byte, 1024*1024)
				buf2 := make([]byte, 1024*1024)
				for !item.present() {
					blockSize := rand.Intn(len(buf))
					offset := max(rand.Int63n(size+2*int64(blockSize))-int64(blockSize), 0)
					_, _ = readCheckBuf(t, in, buf, buf2, item, offset, blockSize)
				}
			})
		}
		wg.Wait()
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})

	// Read it back in reverse which creates the maximum number of
	// downloaders
	t.Run("Reverse", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		offset := int64(size)
		for {
			blockSize := len(buf)
			offset -= int64(blockSize)
			if offset < 0 {
				offset = 0
				blockSize += int(offset)
			}
			_, _ = readCheck(t, item, offset, blockSize)
			if offset == 0 {
				break
			}
		}
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})
}

func newItemTestCacheHandleCaching(t *testing.T, handleCaching time.Duration) (r *fstest.Run, c *Cache) {
	opt := vfscommon.Opt

	// Disable the cache cleaner as it interferes with these tests
	opt.CachePollInterval = 0

	// Disable synchronous write
	opt.WriteBack = 0

	// Set handle caching grace period
	opt.HandleCaching = fs.Duration(handleCaching)

	return newTestCacheOpt(t, opt)
}

func TestItemHandleCaching(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 1*time.Second)

	contents, obj, item := newFile(t, r, c, "existing")

	// Open, read, and close the item
	require.NoError(t, item.Open(obj))

	buf := make([]byte, 10)
	n, err := item.ReadAt(buf, 0)
	assert.Equal(t, 10, n)
	require.NoError(t, err)
	assert.Equal(t, contents[:10], string(buf[:n]))

	require.NoError(t, item.Close(nil))

	// After close, grace period should keep fd and downloaders alive
	item.mu.Lock()
	assert.NotNil(t, item.fd, "fd should still be open during grace period")
	assert.NotNil(t, item.downloaders, "downloaders should still be alive during grace period")
	assert.NotNil(t, item.graceTimer, "grace timer should be set")
	item.mu.Unlock()

	// Re-open the item - should reuse existing fd and downloaders
	require.NoError(t, item.Open(obj))

	// Read data to verify it works
	n, err = item.ReadAt(buf, 10)
	assert.Equal(t, 10, n)
	require.NoError(t, err)
	assert.Equal(t, contents[10:20], string(buf[:n]))

	// Close again
	require.NoError(t, item.Close(nil))

	// Wait for grace period to expire
	time.Sleep(1500 * time.Millisecond)

	// After grace period, fd and downloaders should be cleaned up
	item.mu.Lock()
	assert.Nil(t, item.fd, "fd should be closed after grace period")
	assert.Nil(t, item.downloaders, "downloaders should be closed after grace period")
	assert.Nil(t, item.graceTimer, "grace timer should be nil after expiry")
	item.mu.Unlock()
}

func TestItemHandleCachingDisabled(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 0)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open and close the item
	require.NoError(t, item.Open(obj))
	require.NoError(t, item.Close(nil))

	// With handle caching disabled, fd and downloaders should be immediately closed
	item.mu.Lock()
	assert.Nil(t, item.fd, "fd should be closed immediately when handle caching disabled")
	assert.Nil(t, item.downloaders, "downloaders should be closed immediately when handle caching disabled")
	assert.Nil(t, item.graceTimer, "grace timer should not be set when handle caching disabled")
	item.mu.Unlock()
}

func TestItemHandleCachingReset(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 1*time.Second)

	_, obj, item := newFile(t, r, c, "existing")

	// Open, read (to instantiate cache), and close the item
	require.NoError(t, item.Open(obj))

	buf := make([]byte, 10)
	_, err := item.ReadAt(buf, 0)
	require.NoError(t, err)

	require.NoError(t, item.Close(nil))

	// Grace timer should be active
	item.mu.Lock()
	assert.NotNil(t, item.graceTimer, "grace timer should be set")
	item.mu.Unlock()

	// Reset should skip the item during grace period
	rr, _, err := item.Reset()
	require.NoError(t, err)
	assert.Equal(t, SkippedGrace, rr)

	// Grace timer should still be active
	item.mu.Lock()
	assert.NotNil(t, item.graceTimer, "grace timer should still be set after skipped reset")
	item.mu.Unlock()

	// Wait for grace period to expire then reset should remove the item
	time.Sleep(1500 * time.Millisecond)

	rr, _, err = item.Reset()
	require.NoError(t, err)
	assert.Equal(t, RemovedNotInUse, rr)
}

// TestItemHandleCachingReopenDuringGraceClose reproduces a race between
// reopening an item and the grace-period close firing for it.
//
// closeAfterGrace clears the grace timer and then runs the actual close,
// which temporarily drops item.mu while it tears down the downloaders -
// at that point the file handle is still open. A reopen landing in that
// window used to see no grace timer and a live fd and fail _createFile
// with "internal error: didn't Close file".
func TestItemHandleCachingReopenDuringGraceClose(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 10*time.Second)

	_, obj, item := newFile(t, r, c, "existing")
	buf := make([]byte, 1)

	const iterations = 50
	for i := range iterations {
		// Open, read (to create a downloader) and close so a grace
		// timer is pending with the fd and downloaders still alive.
		require.NoError(t, item.Open(obj))
		_, err := item.ReadAt(buf, 0)
		require.NoError(t, err)
		require.NoError(t, item.Close(nil))

		// Drive the grace close ourselves so we can race it against a
		// reopen. Hold item.mu and park both the close (A) and the
		// reopen (B) on the lock with A queued first. When we release,
		// A runs the close, which drops item.mu to tear down the
		// downloaders, and B - already waiting - grabs it in that
		// window and observes the still-open fd.
		item.mu.Lock()
		require.NotNil(t, item.graceTimer, "grace timer should be set after close")
		item.graceTimer.Stop()

		var openErr error
		var wg sync.WaitGroup
		wg.Add(2)
		startA := make(chan struct{})
		startB := make(chan struct{})
		go func() {
			defer wg.Done()
			<-startA
			item.closeAfterGrace()
		}()
		go func() {
			defer wg.Done()
			<-startB
			openErr = item.Open(obj)
		}()
		close(startA)
		time.Sleep(time.Millisecond) // let A park on item.mu first
		close(startB)
		time.Sleep(time.Millisecond) // let B park on item.mu
		item.mu.Unlock()
		wg.Wait()

		require.NoError(t, openErr, "reopen racing a grace-period close failed on iteration %d", i)

		// Drop the handle from the successful reopen so the next
		// iteration starts from a closed item.
		require.NoError(t, item.Close(nil))
	}

	// Stop the grace timer left pending by the final close.
	item.mu.Lock()
	if item.graceTimer != nil {
		item.graceTimer.Stop()
	}
	item.mu.Unlock()
}

// newConflictTestCache makes a cache for item tests with
// --vfs-conflict-copy set, skipping the test if the remote can't keep
// conflict copies.
func newConflictTestCache(t *testing.T) (r *fstest.Run, c *Cache) {
	r, c = newItemTestCache(t)
	if !operations.CanServerSideMove(r.Fremote) {
		t.Skip("can't keep conflict copies without server-side move")
	}
	c.opt.ConflictCopy = true
	return r, c
}

// conflictCopies returns the objects in the root of the remote whose
// names start with remote, excluding remote itself.
func conflictCopies(t *testing.T, r *fstest.Run, remote string) (copies []fs.Object) {
	entries, err := r.Fremote.List(context.Background(), "")
	require.NoError(t, err)
	entries.ForObject(func(o fs.Object) {
		if o.Remote() != remote && strings.HasPrefix(o.Remote(), remote) {
			copies = append(copies, o)
		}
	})
	return copies
}

// TestItemConflictCopyPreservesRemote checks that when the remote object
// has changed while a local modification is pending, writeback with
// ConflictCopy set uploads the local data under the original name and
// keeps the remote data as a conflict copy, so nothing is lost.
func TestItemConflictCopyPreservesRemote(t *testing.T) {
	r, c := newConflictTestCache(t)

	// Remote object cached locally
	_, obj, item := newFile(t, r, c, "existing")
	require.NoError(t, item.Open(obj))
	localContents := random.String(120)
	n, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)
	assert.Equal(t, 120, n)
	assert.True(t, item.IsDirty())

	// Another client changes the remote object before writeback
	remoteContents := random.String(80)
	r.WriteObject(context.Background(), "existing", remoteContents, time.Now())

	// Writeback: local data wins the name, remote data is kept as a copy
	require.NoError(t, item.Close(nil))
	checkObject(t, r, "existing", localContents)

	copies := conflictCopies(t, r, "existing")
	require.Len(t, copies, 1, "expected exactly one conflict copy")
	assert.Regexp(t, `^existing\.conflict-\d{8}-\d{6}$`, copies[0].Remote())
	checkObject(t, r, copies[0].Remote(), remoteContents)
}

// TestItemConflictCopyUnchangedRemote checks that ConflictCopy leaves a
// plain writeback alone when the remote has not changed.
func TestItemConflictCopyUnchangedRemote(t *testing.T) {
	r, c := newConflictTestCache(t)

	_, obj, item := newFile(t, r, c, "existing")
	require.NoError(t, item.Open(obj))
	localContents := random.String(120)
	_, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)
	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", localContents)
	assert.Empty(t, conflictCopies(t, r, "existing"))
}

// TestItemConflictCopySuffix checks that the conflict copy is named with
// --vfs-conflict-suffix rather than --suffix, honouring
// --suffix-keep-extension.
func TestItemConflictCopySuffix(t *testing.T) {
	ci := fs.GetConfig(context.Background())
	oldSuffix, oldKeep := ci.Suffix, ci.SuffixKeepExtension
	ci.Suffix, ci.SuffixKeepExtension = "-old", true
	t.Cleanup(func() { ci.Suffix, ci.SuffixKeepExtension = oldSuffix, oldKeep })

	r, c := newConflictTestCache(t)
	c.opt.ConflictSuffix = "-conflict"

	_, obj, item := newFile(t, r, c, "existing.txt")
	require.NoError(t, item.Open(obj))
	localContents := random.String(120)
	_, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)
	remoteContents := random.String(80)
	r.WriteObject(context.Background(), "existing.txt", remoteContents, time.Now())
	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing.txt", localContents)
	checkObject(t, r, "existing-conflict.txt", remoteContents)
}

// TestItemConflictCopyTimeGlobs checks that --vfs-conflict-suffix
// expands time globs like bisync's --conflict-suffix does.
func TestItemConflictCopyTimeGlobs(t *testing.T) {
	r, c := newConflictTestCache(t)
	c.opt.ConflictSuffix = ".conflict-{YYYYMMDD}"

	_, obj, item := newFile(t, r, c, "existing")
	require.NoError(t, item.Open(obj))
	_, err := item.WriteAt([]byte(random.String(120)), 0)
	require.NoError(t, err)
	remoteContents := random.String(80)
	r.WriteObject(context.Background(), "existing", remoteContents, time.Now())
	before := time.Now()
	require.NoError(t, item.Close(nil))

	copies := conflictCopies(t, r, "existing")
	require.Len(t, copies, 1, "expected exactly one conflict copy")
	// The date may change during the writeback
	assert.Contains(t, []string{
		"existing.conflict-" + before.Format("20060102"),
		"existing.conflict-" + time.Now().Format("20060102"),
	}, copies[0].Remote())
	checkObject(t, r, copies[0].Remote(), remoteContents)
}

// TestItemConflictCopyBackupDir checks that the conflict copy is moved
// into --backup-dir when it is set.
func TestItemConflictCopyBackupDir(t *testing.T) {
	r, c := newConflictTestCache(t)
	c.opt.ConflictSuffix = "-conflict"
	ci := fs.GetConfig(context.Background())
	oldBackupDir := ci.BackupDir
	ci.BackupDir = r.FremoteName + "/backup"
	t.Cleanup(func() { ci.BackupDir = oldBackupDir })

	_, obj, item := newFile(t, r, c, "existing")
	require.NoError(t, item.Open(obj))
	localContents := random.String(120)
	_, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)
	remoteContents := random.String(80)
	r.WriteObject(context.Background(), "existing", remoteContents, time.Now())
	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", localContents)
	checkObject(t, r, "backup/existing-conflict", remoteContents)
}

// TestItemConflictCopyNumbered checks that a conflict copy doesn't
// overwrite an earlier one with the same name.
func TestItemConflictCopyNumbered(t *testing.T) {
	r, c := newConflictTestCache(t)
	c.opt.ConflictSuffix = "-conflict"
	ctx := context.Background()

	_, obj, item := newFile(t, r, c, "existing")
	var remoteContents []string
	for i := range 2 {
		require.NoError(t, item.Open(obj))
		_, err := item.WriteAt([]byte(random.String(120)), 0)
		require.NoError(t, err)
		contents := random.String(80 + i)
		r.WriteObject(ctx, "existing", contents, time.Now().Add(time.Duration(i)*time.Minute))
		remoteContents = append(remoteContents, contents)
		require.NoError(t, item.Close(nil))
		obj, err = r.Fremote.NewObject(ctx, "existing")
		require.NoError(t, err)
	}

	checkObject(t, r, "existing-conflict", remoteContents[0])
	checkObject(t, r, "existing-conflict-1", remoteContents[1])
}

// TestItemConflictCopyDirRename checks that a remote change to a dirty
// file is still found after its directory is renamed.
func TestItemConflictCopyDirRename(t *testing.T) {
	r, c := newConflictTestCache(t)
	ctx := context.Background()

	_, obj, item := newFile(t, r, c, "dir/existing")
	require.NoError(t, item.Open(obj))
	localContents := random.String(120)
	_, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)
	remoteContents := random.String(80)
	r.WriteObject(ctx, "dir/existing", remoteContents, time.Now())

	require.NoError(t, operations.DirMove(ctx, r.Fremote, "dir", "dir2"))
	require.NoError(t, c.DirRename("dir", "dir2"))
	require.NoError(t, item.Close(nil))

	checkObject(t, r, "dir2/existing", localContents)
	entries, err := r.Fremote.List(ctx, "dir2")
	require.NoError(t, err)
	assert.Len(t, entries, 2, "expected a conflict copy in %v", entries)
}

// TestItemConflictCopyRenameOverRestart checks that a new file renamed
// over an existing one is compared with the file it replaced when it is
// written back after a restart.
func TestItemConflictCopyRenameOverRestart(t *testing.T) {
	r, c := newConflictTestCache(t)
	ctx := context.Background()

	r.WriteObject(ctx, "existing", random.String(80), time.Now())
	replaced, err := r.Fremote.NewObject(ctx, "existing")
	require.NoError(t, err)

	item, _ := c.get("existing.tmp")
	require.NoError(t, item.Open(nil))
	localContents := random.String(120)
	_, err = item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)

	// Close the file to pacify Windows, but don't call item.Close()
	item.mu.Lock()
	require.NoError(t, item.fd.Close())
	item.fd = nil
	item.mu.Unlock()

	require.NoError(t, c.Rename("existing.tmp", "existing", nil, replaced))

	// Remove the item from the cache and reload it as after a restart
	c.mu.Lock()
	delete(c.item, "existing")
	c.mu.Unlock()
	item2, _ := c._get("existing")
	require.NoError(t, item2.reload(ctx))

	checkObject(t, r, "existing", localContents)
	assert.Empty(t, conflictCopies(t, r, "existing"))
}

// TestItemConflictCopyRetry checks that when the remote object has been
// moved aside but the upload then failed, the retried writeback uploads
// the local data without making a second conflict copy.
func TestItemConflictCopyRetry(t *testing.T) {
	r, c := newConflictTestCache(t)
	ctx := context.Background()

	_, obj, item := newFile(t, r, c, "existing")
	require.NoError(t, item.Open(obj))
	localContents := random.String(120)
	_, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)
	remoteContents := random.String(80)
	r.WriteObject(ctx, "existing", remoteContents, time.Now())

	// First writeback attempt: the remote is moved aside, then the
	// upload fails, leaving the item dirty with its old fingerprint.
	item.mu.Lock()
	fingerprint := item.info.Fingerprint
	item.mu.Unlock()
	o, err := c.backupConflict(ctx, "existing", fingerprint)
	require.NoError(t, err)
	assert.Nil(t, o)
	_, err = r.Fremote.NewObject(ctx, "existing")
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
	assert.True(t, item.IsDirty())

	// Retried writeback
	require.NoError(t, item.Close(nil))
	checkObject(t, r, "existing", localContents)

	copies := conflictCopies(t, r, "existing")
	require.Len(t, copies, 1, "expected exactly one conflict copy")
	checkObject(t, r, copies[0].Remote(), remoteContents)
}

// TestItemConflictCopyReopen checks that the remote is still seen as
// changed after the dirty item is opened again with the changed remote
// object and its modification time is set.
func TestItemConflictCopyReopen(t *testing.T) {
	r, c := newConflictTestCache(t)
	ctx := context.Background()

	_, obj, item := newFile(t, r, c, "existing")
	require.NoError(t, item.Open(obj))
	localContents := random.String(120)
	_, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)
	remoteContents := random.String(80)
	r.WriteObject(ctx, "existing", remoteContents, time.Now())

	newObj, err := r.Fremote.NewObject(ctx, "existing")
	require.NoError(t, err)
	require.NoError(t, item.Open(newObj))
	item.setModTime(time.Now())
	require.NoError(t, item.Close(nil))
	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", localContents)
	copies := conflictCopies(t, r, "existing")
	require.Len(t, copies, 1, "expected exactly one conflict copy")
	checkObject(t, r, copies[0].Remote(), remoteContents)
}

// TestItemConflictCopyReload checks that a new file which was never
// uploaded doesn't overwrite a remote file of the same name made in the
// meantime when it is written back after a restart.
func TestItemConflictCopyReload(t *testing.T) {
	r, c := newConflictTestCache(t)
	ctx := context.Background()

	item, _ := c.get("new")
	require.NoError(t, item.Open(nil))
	localContents := random.String(120)
	_, err := item.WriteAt([]byte(localContents), 0)
	require.NoError(t, err)

	// Close the file to pacify Windows, but don't call item.Close()
	item.mu.Lock()
	require.NoError(t, item.fd.Close())
	item.fd = nil
	item.mu.Unlock()

	// Remove the item from the cache
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()

	// Another client makes a file with the same name
	remoteContents := random.String(80)
	r.WriteObject(ctx, "new", remoteContents, time.Now())

	// Reload the item which writes it back
	item2, _ := c._get("new")
	require.NoError(t, item2.reload(ctx))

	checkObject(t, r, "new", localContents)
	copies := conflictCopies(t, r, "new")
	require.Len(t, copies, 1, "expected exactly one conflict copy")
	checkObject(t, r, copies[0].Remote(), remoteContents)
}
