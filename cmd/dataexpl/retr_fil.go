package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/filecoin-project/cidtravel/ctbstore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/net/host"
	"github.com/filecoin-project/lassie/pkg/retriever"
	types2 "github.com/filecoin-project/lassie/pkg/types"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
	"io"
	"net/http"
	"regexp"
	"strings"

	lapi "github.com/filecoin-project/lotus/api"
	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
)

type selGetter func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, map[string]struct{}, func(), error)

func (h *dxhnd) getCarFilRetrieval(r *http.Request, ma address.Address, pcid, dcid cid.Cid) func(ss builder.SelectorSpec) (io.ReadCloser, error) {
	return func(ss builder.SelectorSpec) (io.ReadCloser, error) {
		vars := mux.Vars(r)

		sel, err := pathToSel(vars["path"], false, ss)
		if err != nil {
			return nil, err
		}

		done, err := h.retrieveFil(r.Context(), nil, ma, pcid, dcid, &sel, nil)
		if err != nil {
			return nil, xerrors.Errorf("retrieve: %w", err)
		}
		defer done()

		// todo

		return nil, nil
	}
}

func (h *dxhnd) getFilRetrieval(bsb *ctbstore.TempBsb, r *http.Request, ma address.Address, pcid, dcid cid.Cid) selGetter {
	return func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, map[string]struct{}, func(), error) {
		vars := mux.Vars(r)

		sel, err := pathToSel(vars["path"], false, ss)
		if err != nil {
			return cid.Undef, nil, nil, nil, xerrors.Errorf("filretr: %w", err)
		}

		tbs, err := bsb.MakeStore()
		if err != nil {
			return cid.Cid{}, nil, nil, nil, xerrors.Errorf("make temp store: %w", err)
		}

		var cbs bstore.Blockstore = tbs
		bbs := ctbstore.NewBlocking(bstore.Adapt(cbs))
		cbs = bbs

		retBs := ctbstore.NewCtxWrap(cbs, ctbstore.WithNoBlock)

		done, err := h.retrieveFil(r.Context(), retBs, ma, pcid, dcid, &sel, func() {
			bbs.Finalize()
			if err := tbs.Release(); err != nil {
				log.Errorw("release temp store", "error")
			}

			log.Warnw("store released")
		})
		if err != nil {
			if err := tbs.Release(); err != nil {
				log.Errorw("release temp store", "error")
			}
			return cid.Undef, nil, nil, nil, xerrors.Errorf("retrieve: %w", err)
		}

		bs := bstore.NewTieredBstore(cbs, bstore.NewMemory())
		ds := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

		rs, err := SelectorSpecFromPath(Expression(vars["path"]), false, ss)
		if err != nil {
			return cid.Cid{}, nil, nil, nil, xerrors.Errorf("failed to parse path-selector: %w", err)
		}

		root, links, err := findRoot(r.Context(), dcid, rs, ds)
		if err != nil {
			return cid.Cid{}, nil, nil, nil, xerrors.Errorf("find root: %w", err)
		}

		return root, ds, links, done, err
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

func (h *dxhnd) retrieveFil(ctx context.Context, apiStore blockstore.Blockstore, minerAddr address.Address, pieceCid, file cid.Cid, sel *lapi.Selector, retrDone func()) (func(), error) {
	maddr, err := GetAddrInfo(ctx, h.api, minerAddr)
	if err != nil {
		return nil, err
	}

	host, err := host.InitHost(ctx, []libp2p.Option{})
	if err != nil {
		return nil, xerrors.Errorf("init temp host: %w", err)
	}

	lassie, err := lassie.NewLassie(ctx, lassie.WithHost(host), lassie.WithProviderAllowList(map[peer.ID]bool{}),
		lassie.WithFinder(retriever.NewDirectCandidateFinder(host, []peer.AddrInfo{*maddr})))
	if err != nil {
		return nil, xerrors.Errorf("start lassie: %w", err)
	}

	uselessWrapperStore := &ctbstore.WhyDoesThisNeedToExistBS{
		TBS: apiStore,
	}

	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetWriteStorage(uselessWrapperStore)
	linkSystem.SetReadStorage(uselessWrapperStore)
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	rid, err := types2.NewRetrievalID()
	if err != nil {
		return nil, err
	}

	rsn, err := selectorparse.ParseJSONSelector(string(*sel))
	if err != nil {
		return nil, xerrors.Errorf("failed to parse json-selector '%s': %w", *sel, err)
	}

	request := types2.RetrievalRequest{
		RetrievalID: rid,
		Cid:         file,
		Selector:    rsn,
		LinkSystem:  linkSystem,
	}

	request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
	request.PreloadLinkSystem.SetReadStorage(uselessWrapperStore)
	request.PreloadLinkSystem.SetWriteStorage(uselessWrapperStore)
	request.PreloadLinkSystem.TrustedStorage = true

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		stats, err := lassie.Fetch(ctx, request)
		if err != nil {
			log.Errorw("lassie fetch", "error", err)
		}
		_ = stats
	}()

	return func() {
		cancel()
	}, nil
}

func pathToSel(psel string, matchTraversal bool, sub builder.SelectorSpec) (lapi.Selector, error) {
	rs, err := SelectorSpecFromPath(Expression(psel), matchTraversal, sub)
	if err != nil {
		return "", xerrors.Errorf("failed to parse path-selector: %w", err)
	}

	var b bytes.Buffer
	if err := dagjson.Encode(rs.Node(), &b); err != nil {
		return "", err
	}

	fmt.Println(b.String())

	return lapi.Selector(b.String()), nil
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
