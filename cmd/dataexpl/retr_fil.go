package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/filecoin-project/cidtravel/ctbstore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lassie/pkg/lassie"
	types2 "github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"golang.org/x/xerrors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	lapi "github.com/filecoin-project/lotus/api"
	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
)

// Selector specifies ipld selector string
//   - if the string starts with '{', it's interpreted as json selector string
//     see https://ipld.io/specs/selectors/ and https://ipld.io/specs/selectors/fixtures/selector-fixtures-1/
//   - otherwise the string is interpreted as ipld-selector-text-lite (simple ipld path)
//     see https://github.com/ipld/go-ipld-selector-text-lite
type Selector string

type selGetter func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, map[string]struct{}, func(), error)

func (h *dxhnd) getCarFilRetrieval(r *http.Request, ma address.Address, pcid, dcid cid.Cid) func(ss builder.SelectorSpec) (io.ReadCloser, error) {
	return func(ss builder.SelectorSpec) (io.ReadCloser, error) {
		//vars := mux.Vars(r)

		/*sel, err := pathToSel(vars["path"], false, ss)
		if err != nil {
			return nil, err
		}*/

		/*done, err := h.retrieveFil(r.Context(), nil, ma, pcid, dcid, &sel, nil)
		if err != nil {
			return nil, xerrors.Errorf("retrieve: %w", err)
		}
		defer done()*/

		// todo

		return nil, nil
	}
}

func (h *dxhnd) getFilRetrieval(ctx context.Context, ma address.Address, dcid cid.Cid, path string) func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, map[string]struct{}, func(), error) {
	return func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, map[string]struct{}, func(), error) {
		maddr, err := GetAddrInfo(ctx, h.api, ma)
		if err != nil {
			return cid.Cid{}, nil, nil, nil, err
		}

		// Initialize the LassieBlockstore
		lbs, err := NewLassieBlockstore(ctx, h.lw, &types2.RetrievalCandidate{
			MinerPeer: *maddr,
		})
		if err != nil {
			return cid.Undef, nil, nil, nil, xerrors.Errorf("failed to create LassieBlockstore: %w", err)
		}

		// Create a blockstore tiered with an in-memory store for caching
		bs := ctbstore.WithCache(bstore.Adapt(lbs))
		ds := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

		// Parse the selector spec from the path
		rs, err := SelectorSpecFromPath(Expression(path), false, ss)
		if err != nil {
			return cid.Cid{}, nil, nil, nil, xerrors.Errorf("failed to parse path-selector: %w", err)
		}

		// Find the root CID and links
		root, links, err := findRoot(ctx, dcid, rs, ds)
		if err != nil {
			return cid.Cid{}, nil, nil, nil, xerrors.Errorf("find root: %w", err)
		}

		// Return the root CID, DAGService, links, and a cleanup function
		return root, ds, links, lbs.done, nil
	}
}

const (
	RetrResSuccess = iota
	RetrResQueryOfferErr
	RetrResOfferError

	RetrResPriceTooHigh
	RetrResRetrievalSetupErr

	RetrResRetrievalRejected
	RetrResRetrievalErr
	RetrResRetrievalTimeout
)

func pathToSel(psel string, matchTraversal bool, sub builder.SelectorSpec) (Selector, error) {
	rs, err := SelectorSpecFromPath(Expression(psel), matchTraversal, sub)
	if err != nil {
		return "", xerrors.Errorf("failed to parse path-selector: %w", err)
	}

	var b bytes.Buffer
	if err := dagjson.Encode(rs.Node(), &b); err != nil {
		return "", err
	}

	fmt.Println(b.String())

	return Selector(b.String()), nil
}

// PathValidCharset is the regular expression fully matching a valid textselector
const PathValidCharset = `[- _0-9a-zA-Z\/\.~]`

// Expression is a string-type input to SelectorSpecFromPath
type Expression string

var invalidChar = regexp.MustCompile(`[^` + PathValidCharset[1:len(PathValidCharset)-1] + `]`)

func SelectorSpecFromPath(path Expression, matchPath bool, optionalSubselectorAtTarget builder.SelectorSpec) (builder.SelectorSpec, error) {
	/*

	   Path elem parsing
	   * If first char is not `~`, then path elem is unixfs
	   * If first char is `~`
	   	* If second char is not `~` then path[1:] is ipld dm
	   	* If second char is `~`
	   		* If third char is `~` then path is unixfs path[2:]
	   		* If third char is `i` then path is "~"+path[3:]


	   /some/path -> ufs(some)/ufs(path)

	   /~cb/~pa/file -> /cb/pa/ufs(file)

	   /~~~ -> /ufs(~)

	   /~~i -> /~

	*/

	if path == "/" {
		return nil, fmt.Errorf("a standalone '/' is not a valid path")
	} else if m := invalidChar.FindStringIndex(string(path)); m != nil {
		return nil, fmt.Errorf("path string contains invalid character at offset %d", m[0])
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	ss := optionalSubselectorAtTarget
	// if nothing is given - use an exact matcher
	if ss == nil {
		ss = ssb.Matcher()
	}

	segments := strings.Split(string(path), "/")

	// walk backwards wrapping the original selector recursively
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]

		if seg == "" {
			// allow one leading and one trailing '/' at most
			if i == 0 || i == len(segments)-1 {
				continue
			}
			return nil, fmt.Errorf("invalid empty segment at position %d", i)
		}

		seg, isUnix, err := decodeSegment(seg)
		if err != nil {
			return nil, err
		}

		if seg == "" {
			return nil, fmt.Errorf("invalid empty segment at position %d", i)
		}

		if seg == "." || seg == ".." {
			return nil, fmt.Errorf("unsupported path segment '%s' at position %d", seg, i)
		}

		ss = ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert(seg, ss)
		})

		if isUnix {
			ss = ssb.ExploreInterpretAs("unixfs", ss)
		}

		if matchPath {
			ss = ssb.ExploreUnion(ssb.Matcher(), ss)
		}
	}

	return ss, nil
}

func decodeSegment(seg string) (string, bool, error) {
	isUnix := true

	if len(seg) == 0 {
		return "", false, nil
	}

	if seg[0] == '~' {
		if len(seg) < 2 {
			return "", false, xerrors.Errorf("path segment prefixed with ~ must be longer than 3 characters")
		}

		if seg[1] == '~' {
			if len(seg) < 3 {
				return "", false, xerrors.Errorf("path segment prefixed with ~~ must be longer than 3 characters")
			}
			switch seg[2] {
			case '~':
				seg = seg[2:]
			case 'i':
				if len(seg) < 4 {
					return "", false, xerrors.Errorf("path segment prefixed with ~~i must be longer than 4 characters")
				}

				isUnix = false
				seg = "~" + seg[3:]
			default:
				return "", false, xerrors.Errorf("unknown segment mode '%c'", seg[2])
			}
		} else {
			isUnix = false
			seg = seg[1:]
		}
	}

	return seg, isUnix, nil
}

func ipldToPathSeg(is string) string {
	if is == "" { // invalid, but don't panic
		return ""
	}
	if is[0] != '~' {
		return "~" + is
	}
	if len(is) < 2 { // invalid but panicking is bad
		return "~~i"
	}

	return "~~i" + is[1:]
}

func unixToPathSeg(is string) string {
	if is == "" { // invalid, but don't panic
		return ""
	}
	if is[0] != '~' {
		return is
	}
	if len(is) < 2 { // invalid but panicking is bad
		return "~~~"
	}

	return "~~~" + is[1:]
}

func GetAddrInfo(ctx context.Context, api lapi.Gateway, maddr address.Address) (*peer.AddrInfo, error) {
	minfo, err := api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	if minfo.PeerId == nil {
		return nil, fmt.Errorf("storage provider %s has no peer ID set on-chain", maddr)
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return nil, fmt.Errorf("storage provider %s had invalid multiaddrs in their info: %w", maddr, err)
		}
		maddrs = append(maddrs, ma)
	}
	if len(maddrs) == 0 {
		return nil, fmt.Errorf("storage provider %s has no multiaddrs set on-chain", maddr)
	}

	return &peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}, nil
}

type LassieWrapper struct {
	h      host.Host
	lassie *lassie.Lassie

	mu         sync.Mutex
	retrievals map[uuid.UUID]map[mhStr]*types2.RetrievalCandidate
	cids       map[mhStr]map[uuid.UUID]struct{}
}

func (l *LassieWrapper) FindCandidates(ctx context.Context, c cid.Cid, f func(types2.RetrievalCandidate)) error {
	l.mu.Lock()
	var candidates []types2.RetrievalCandidate
	if retrievalIDs, ok := l.cids[mhStr(c.Hash())]; ok {
		for retrievalID := range retrievalIDs {
			if candidate, ok := l.retrievals[retrievalID][mhStr(c.Hash())]; ok {
				candidates = append(candidates, *candidate)
			}
		}
	}
	l.mu.Unlock()

	log.Infow("found candidates", "cid", c, "candidates", candidates)

	for _, cd := range candidates {
		cd.RootCid = c
		f(types2.NewRetrievalCandidate(cd.MinerPeer.ID, cd.MinerPeer.Addrs, c, metadata.HTTPV1(), &metadata.GraphsyncFilecoinV1{FastRetrieval: true}))
	}

	return nil
}

func (l *LassieWrapper) addCandidateForCid(retrievalID uuid.UUID, c cid.Cid, candidate *types2.RetrievalCandidate) {
	log.Infow("add candidate", "retr", retrievalID, "cid", c, "candi", candidate.MinerPeer.ID)

	l.mu.Lock()
	defer l.mu.Unlock()

	// Add candidate to retrievals map
	if _, ok := l.retrievals[retrievalID]; !ok {
		l.retrievals[retrievalID] = make(map[mhStr]*types2.RetrievalCandidate)
	}
	l.retrievals[retrievalID][mhStr(c.Hash())] = candidate

	// Add retrieval ID to cids map
	if _, ok := l.cids[mhStr(c.Hash())]; !ok {
		l.cids[mhStr(c.Hash())] = make(map[uuid.UUID]struct{})
	}
	l.cids[mhStr(c.Hash())][retrievalID] = struct{}{}
}

func (l *LassieWrapper) retrievalDone(retrievalID uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Remove the retrieval ID from all cids
	if retrievalCids, ok := l.retrievals[retrievalID]; ok {
		for c := range retrievalCids {
			delete(l.cids[c], retrievalID)
			if len(l.cids[c]) == 0 {
				delete(l.cids, c)
			}
		}
	}

	// Remove the retrieval
	delete(l.retrievals, retrievalID)
}

type mhStr string

func NewLassieWrapper(h host.Host) (*LassieWrapper, error) {
	ctx := context.Background()

	lw := &LassieWrapper{
		h: h,

		cids:       map[mhStr]map[uuid.UUID]struct{}{},
		retrievals: map[uuid.UUID]map[mhStr]*types2.RetrievalCandidate{},
	}

	lassieClient, err := lassie.NewLassie(ctx, lassie.WithHost(h),
		lassie.WithCandidateSource(lw),
		lassie.WithProtocols([]multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp, multicodec.TransportGraphsyncFilecoinv1}))
	if err != nil {
		return nil, fmt.Errorf("failed to create Lassie client: %w", err)
	}

	lw.lassie = lassieClient
	return lw, err
}

type LassieBlockstore struct {
	ctx  context.Context
	lw   *LassieWrapper
	retr uuid.UUID
	rc   *types2.RetrievalCandidate
}

func NewLassieBlockstore(ctx context.Context, lw *LassieWrapper, rc *types2.RetrievalCandidate) (*LassieBlockstore, error) {
	return &LassieBlockstore{
		ctx:  ctx,
		lw:   lw,
		rc:   rc,
		retr: uuid.Must(uuid.NewRandom()),
	}, nil
}

func (lbs *LassieBlockstore) done() {
	lbs.lw.retrievalDone(lbs.retr)
}

func (lbs *LassieBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return fmt.Errorf("DeleteBlock not supported")
}

func (lbs *LassieBlockstore) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	return fmt.Errorf("DeleteMany not supported")
}

func (lbs *LassieBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	// Since we're fetching blocks on-demand, we can't guarantee existence without fetching.
	// Returning true allows the Get method to attempt retrieval.
	return true, nil
}

func (lbs *LassieBlockstore) Get(ctx context.Context, c cid.Cid) (block.Block, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	lbs.lw.addCandidateForCid(lbs.retr, c, lbs.rc)

	// Create an in-memory blockstore to store the retrieved block
	blockBuffer := &ctbstore.WhyDoesThisNeedToExistBS{
		TBS: bstore.NewMemorySync(),
	}

	// Set up the link system with the in-memory blockstore
	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetReadStorage(blockBuffer)
	linkSystem.SetWriteStorage(blockBuffer)
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	// Create a retrieval request for the single block
	request := types2.RetrievalRequest{
		RetrievalID: must(types2.NewRetrievalID),
		LinkSystem:  linkSystem,
		Request: trustlessutils.Request{
			Root:  c,
			Scope: trustlessutils.DagScopeBlock,
		},
	}

	// Execute the retrieval
	_, err := lbs.lw.lassie.Fetch(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve block with CID %s: %w", c, err)
	}

	// Retrieve the block from the in-memory blockstore
	ds := merkledag.NewDAGService(blockservice.New(blockBuffer.TBS, offline.Exchange(blockBuffer.TBS)))
	nd, err := ds.Get(ctx, c)
	if err != nil {
		return nil, xerrors.Errorf("dag get: %w", err)
	}

	return nd, nil
}

func (lbs *LassieBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	block, err := lbs.Get(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(block.RawData()), nil
}

func (lbs *LassieBlockstore) Put(ctx context.Context, blk block.Block) error {
	return fmt.Errorf("Put not supported")
}

func (lbs *LassieBlockstore) PutMany(ctx context.Context, blks []block.Block) error {
	return fmt.Errorf("PutMany not supported")
}

func (lbs *LassieBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("AllKeysChan not supported")
}

func (lbs *LassieBlockstore) HashOnRead(enabled bool) {
	// No-op; hashing on read is not supported in this implementation
}
