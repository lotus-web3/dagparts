package ctbstore

import (
	"context"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/lib/result"
	"github.com/hashicorp/golang-lru/arc/v2"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"sync"
)

var log = logging.Logger("ctbstore")

type blockGet struct {
	b    result.Result[block.Block]
	wait chan struct{}
}

type CachedBlockstore struct {
	write blockstore.Blockstore
	cache *arc.ARCCache[cid.Cid, *blockGet]

	writeLk       sync.Mutex
	pendingWrites map[cid.Cid]block.Block
}

func makeGet() *blockGet {
	return &blockGet{
		wait: make(chan struct{}),
	}
}

var CacheBstoreSize = (4 << 30) / 16000 // 4GB with average block size of 16KB

func WithCache(base blockstore.Blockstore) *CachedBlockstore {
	c, _ := arc.NewARC[cid.Cid, *blockGet](CacheBstoreSize)

	bs := &CachedBlockstore{
		write: base,

		cache:         c,
		pendingWrites: make(map[cid.Cid]block.Block),
	}
	return bs
}

func (bs *CachedBlockstore) Flush(ctx context.Context) error {
	return bs.write.Flush(ctx)
}

func (bs *CachedBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return bs.write.AllKeysChan(ctx)
}

func (bs *CachedBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	bs.cache.Remove(c)
	return bs.write.DeleteBlock(ctx, c)
}

func (bs *CachedBlockstore) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	for _, c := range cids {
		bs.cache.Remove(c)
	}
	return bs.write.DeleteMany(ctx, cids)
}

func (bs *CachedBlockstore) View(ctx context.Context, c cid.Cid, callback func([]byte) error) error {
	bs.writeLk.Lock()
	if blk, ok := bs.cache.Get(c); ok {
		<-blk.wait
		if blk.b.Error != nil {
			return blk.b.Error
		}
		return callback(blk.b.Value.RawData())
	}

	if blk, ok := bs.pendingWrites[c]; ok {
		bs.writeLk.Unlock()
		return callback(blk.RawData())
	}
	g := makeGet()
	bs.cache.Add(c, g)
	bs.writeLk.Unlock()

	return bs.write.View(ctx, c, func(bytes []byte) error {
		blk, err := block.NewBlockWithCid(bytes, c)
		g.b = result.Wrap(block.Block(blk), err)
		close(g.wait)

		if err != nil {
			return err
		}

		return callback(bytes)
	})
}

func (bs *CachedBlockstore) Get(ctx context.Context, c cid.Cid) (b block.Block, err error) {
	defer func() {
		if b == nil && err == nil {
			panic("blockstore returned nil block and nil error")
		}
	}()

	bs.writeLk.Lock()
	if blk, ok := bs.cache.Get(c); ok {
		bs.writeLk.Unlock()
		<-blk.wait
		return blk.b.Unwrap()
	}

	if blk, ok := bs.pendingWrites[c]; ok {
		bs.writeLk.Unlock()
		return blk, nil
	}

	g := makeGet()
	bs.cache.Add(c, g)

	bs.writeLk.Unlock()

	b, err = bs.write.Get(ctx, c)
	g.b = result.Wrap(b, err)
	close(g.wait)

	return b, err
}

func (bs *CachedBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	b, err := bs.Get(ctx, c)
	if err != nil {
		return 0, err
	}

	return len(b.RawData()), nil
}

func (bs *CachedBlockstore) Put(ctx context.Context, blk block.Block) error {
	g := makeGet()
	g.b = result.Wrap(blk, nil)
	close(g.wait)

	bs.cache.Add(blk.Cid(), g)

	return bs.write.Put(ctx, blk)
}

func (bs *CachedBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if bs.cache.Contains(c) {
		return true, nil
	}

	bs.writeLk.Lock()
	if _, ok := bs.pendingWrites[c]; ok {
		bs.writeLk.Unlock()
		return true, nil
	}
	bs.writeLk.Unlock()

	return bs.write.Has(ctx, c)
}

func (bs *CachedBlockstore) HashOnRead(hor bool) {
	bs.write.HashOnRead(hor)
}

func (bs *CachedBlockstore) PutMany(ctx context.Context, blks []block.Block) error {
	toPut := make([]block.Block, 0, len(blks))

	for _, blk := range blks {
		if bs.cache.Contains(blk.Cid()) {
			continue
		}

		g := makeGet()
		g.b = result.Wrap(blk, nil)
		bs.cache.Add(blk.Cid(), g)
		toPut = append(toPut, blk)
	}

	if len(toPut) == 0 {
		return nil
	}

	//return bs.write.PutMany(ctx, toPut)

	// this part is EXTREMELY aggresive

	bs.writeLk.Lock()
	for i, blk := range blks {
		bs.pendingWrites[blk.Cid()] = blks[i]
	}
	bs.writeLk.Unlock()

	go func() {
		err := bs.write.PutMany(context.TODO(), toPut)

		bs.writeLk.Lock()
		for _, blk := range toPut {
			delete(bs.pendingWrites, blk.Cid())
		}
		bs.writeLk.Unlock()

		if err != nil {
			log.Error("failed to write to disk", err)
		}
	}()

	return nil
}
