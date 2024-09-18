package main

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"github.com/filecoin-project/cidtravel/ctbstore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"github.com/ipld/go-car"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/urfave/cli/v2"
	"html/template"
	"io"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"os"
	gopath "path"
	"sort"
	"sync"

	"github.com/gabriel-vasile/mimetype"
	"github.com/gorilla/mux"
	format "github.com/ipfs/go-ipld-format"
	io2 "github.com/ipfs/go-unixfs/io"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	finderhttpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"

	lapi "github.com/filecoin-project/lotus/api"
	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
	cliutil "github.com/filecoin-project/lotus/cli/util"
)

//go:embed dexpl
var dres embed.FS

var maxDirTypeChecks, typeCheckDepth int64 = 16, 15

type marketMiner struct {
	Addr   address.Address
	Owner  address.Address
	QAP    string
	Locked types.FIL
}

type dxhnd struct {
	api lapi.Gateway
	bs  bstore.Blockstore

	h host.Host

	chainStores []bstore.Blockstore

	marketDealCache *lru.Cache

	clientMeta string

	idx *finderhttpclient.Client

	mminers   []marketMiner
	minerPids map[peer.ID]address.Address

	tempBsBld *ctbstore.TempBsb

	trackerFil *TrackerFil
}

func (h *dxhnd) handleIndex(w http.ResponseWriter, r *http.Request) {
	tpl, err := template.ParseFS(dres, "dexpl/index.gohtml")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	data := map[string]interface{}{}
	if err := tpl.Execute(w, data); err != nil {
		fmt.Println(err)
		return
	}
}

func (h *dxhnd) handleCar(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ma, err := address.NewFromString(vars["mid"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pcid, err := cid.Parse(vars["piece"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dcid, err := cid.Parse(vars["cid"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// retr root
	g := h.getCarFilRetrieval(r, ma, pcid, dcid)

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	))

	reader, err := g(sel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.ipld.car")

	name := r.FormValue("filename")
	if name == "" {
		name = dcid.String()
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.car"`, name))

	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
	_ = reader.Close()
}

func (h *dxhnd) handleCarIPFS(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	dcid, err := cid.Parse(vars["cid"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sg := h.getIpfs(r.Context(), dcid, vars["path"])

	// retr root

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	))

	root, ds, _, done, err := sg(sel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer done()

	w.Header().Set("Content-Type", "application/vnd.ipld.car")

	name := r.FormValue("filename")
	if name == "" {
		name = dcid.String()
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.car"`, name))

	w.WriteHeader(http.StatusOK)

	if err := car.WriteCarWithWalker(r.Context(), ds, []cid.Cid{root}, w, CarWalkFunc); err != nil {
		log.Errorw("write car", "error", err)
		return
	}
}

func CarWalkFunc(nd format.Node) (out []*format.Link, err error) {
	for _, link := range nd.Links() {
		pref := link.Cid.Prefix()
		if pref.Codec == cid.FilCommitmentSealed || pref.Codec == cid.FilCommitmentUnsealed {
			continue
		}
		out = append(out, link)
	}

	return out, nil
}

var dataexplCmd = &cli.Command{
	Name:  "run",
	Usage: "Explore data stored on filecoin",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "blk-cache",
			Value: os.TempDir(),
		},
		&cli.StringFlag{
			Name:     "client-meta",
			Required: true,
			Aliases:  []string{"m"},
		},
	},
	Action: func(cctx *cli.Context) error {
		logging.SetAllLoggers(logging.LevelInfo)

		api, closer, err := cliutil.GetGatewayAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := cliutil.ReqContext(cctx)

		gwHs, closerHs, err := client.NewGatewayRPCV1(ctx, "https://filecoin-hyperspace.chainstacklabs.com/rpc/v1", nil)
		if err != nil {
			return err
		}
		defer closerHs()

		gwCl, closerCl, err := client.NewGatewayRPCV1(ctx, "https://api.calibration.node.glif.io/rpc/v1", nil)
		if err != nil {
			return err
		}
		defer closerCl()

		networks := map[string]lapi.Gateway{
			"mainnet":     api,
			"hyperspace":  gwHs,
			"calibration": gwCl,
		}

		idx, err := finderhttpclient.New("https://cid.contact")
		if err != nil {
			return err
		}

		var lk sync.Mutex
		var wg sync.WaitGroup
		var mminers []marketMiner
		pidMiners := map[peer.ID]address.Address{}

		fmt.Println("loading miner states")

		tracker, err := OpenFilTracker("filtracker.db")
		if err != nil {
			return err
		}

		head, err := api.ChainHead(ctx)
		if err != nil {
			return xerrors.Errorf("getting chain head: %w", err)
		}

		mktAct, err := api.StateGetActor(ctx, market.Address, head.Key())
		if err != nil {
			return xerrors.Errorf("getting market actor: %w", err)
		}

		stor := adt.WrapStore(ctx, cbor.NewCborStore(bstore.NewAPIBlockstore(api)))

		mact, err := market.Load(stor, mktAct)
		if err != nil {
			return xerrors.Errorf("loading market actor: %w", err)
		}

		bt, err := mact.LockedTable()
		if err != nil {
			return xerrors.Errorf("getting locked table: %w", err)
		}

		err = bt.ForEach(func(a address.Address, mb abi.TokenAmount) error {
			if mb.IsZero() {
				return nil
			}

			wg.Add(1)
			go func(a address.Address, mb abi.TokenAmount) {
				defer wg.Done()

				mi, err := api.StateMinerInfo(ctx, a, types.EmptyTSK)
				if err != nil {
					return
				}
				mp, err := api.StateMinerPower(ctx, a, types.EmptyTSK)
				if err != nil {
					return
				}

				lk.Lock()
				defer lk.Unlock()

				if err := tracker.UpsertProvider(a); err != nil {
					log.Errorw("upserting provider", "error", err)
				}

				mminers = append(mminers, marketMiner{
					Addr:   a,
					Owner:  mi.Owner,
					QAP:    types.SizeStr(mp.MinerPower.QualityAdjPower),
					Locked: types.FIL(mb),
				})

				if mi.PeerId != nil {
					pidMiners[*mi.PeerId] = a
				}
			}(a, mb)

			return nil
		})

		wg.Wait()
		sort.Slice(mminers, func(i, j int) bool {
			return big.Cmp(abi.TokenAmount(mminers[i].Locked), abi.TokenAmount(mminers[j].Locked)) > 0
		})

		// setup server
		dc, _ := lru.New(10_000_000)

		h, err := libp2p.New()
		if err != nil {
			return err
		}

		dh := &dxhnd{
			api: api,
			idx: idx,

			h: h,

			marketDealCache: dc,

			clientMeta: cctx.String("client-meta"),

			mminers:   mminers,
			minerPids: pidMiners,

			bs:        ctbstore.WithCache(bstore.NewAPIBlockstore(api)),
			tempBsBld: ctbstore.NewTempBsBuilder(cctx.String("blk-cache")),

			trackerFil: tracker,
		}

		m := mux.NewRouter()

		var staticFS = fs.FS(dres)
		static, err := fs.Sub(staticFS, "dexpl")
		if err != nil {
			log.Fatal(err)
		}

		m.PathPrefix("/static/").Handler(http.FileServer(http.FS(static))).Methods("GET", "HEAD")

		m.HandleFunc("/", dh.handleIndex).Methods("GET")

		m.HandleFunc("/chain/filecoin", dh.handleChains(networks)).Methods("GET")

		for net, gateway := range networks {
			dh.chainStores = append(dh.chainStores, bstore.NewAPIBlockstore(gateway))

			m.HandleFunc("/chain/filecoin/"+net, dh.handleChain(gateway, net)).Methods("GET")
			m.HandleFunc("/chain/filecoin/"+net+"/actor", dh.handleChainActor(gateway)).Methods("GET")
		}

		m.HandleFunc("/providers", dh.handleProviders).Methods("GET")
		m.HandleFunc("/ping/miner/{id}", dh.handlePingMiner).Methods("GET")
		m.HandleFunc("/ping/peer/ipfs/{id}", dh.handlePingIPFS).Methods("GET")
		m.HandleFunc("/ping/peer/lotus/{id}", dh.handlePingLotus).Methods("GET")
		m.HandleFunc("/clients", dh.handleClients).Methods("GET")
		m.HandleFunc("/client/{id}", dh.handleClient).Methods("GET")
		m.HandleFunc("/provider/{id}", dh.handleProviderSectors).Methods("GET")
		m.HandleFunc("/provider/{id}/stats", dh.handleProviderStats).Methods("GET")

		m.HandleFunc("/deal/{id}", dh.handleDeal).Methods("GET")
		m.HandleFunc("/view/ipfs/{cid}/{path:.*}", dh.handleViewIPFS).Methods("GET", "HEAD")
		m.HandleFunc("/view/{mid}/{piece}/{cid}/{path:.*}", dh.handleViewFil).Methods("GET", "HEAD")
		m.HandleFunc("/car/ipfs/{cid}/{path:.*}", dh.handleCarIPFS).Methods("GET", "HEAD")
		m.HandleFunc("/car/{mid}/{piece}/{cid}/{path:.*}", dh.handleCar).Methods("GET")

		m.HandleFunc("/matchdeal/{mid}/{piece}", dh.handleMatchPiece).Methods("GET")

		m.HandleFunc("/find/{cid}", dh.handleFind).Methods("GET")

		server := &http.Server{
			Addr:    ":5658",
			Handler: m,
			BaseContext: func(_ net.Listener) context.Context {
				return cctx.Context
			},
		}
		go func() {
			_ = server.ListenAndServe()
		}()

		// start pinger
		go dh.pinger()

		fmt.Println("ready")

		<-ctx.Done()

		return nil
	},
}

type nodeInfo struct {
	Name string
	Size string
	Cid  cid.Cid
	Desc string
}

func parseLinks(ctx context.Context, ls []*format.Link, dserv format.DAGService, maxChecks int64) ([]nodeInfo, error) {
	links := make([]nodeInfo, len(ls))

	for i, l := range ls {
		links[i] = nodeInfo{
			Name: l.Name,
			Size: types.SizeStr(types.NewInt(l.Size)),
			Cid:  l.Cid,
		}

		if int64(i) < maxChecks {
			ni, _, err := linkDesc(ctx, l.Cid, l.Name, dserv)
			if err != nil {
				return nil, err
			}

			links[i].Desc = ni.Desc
		}
	}

	return links, nil
}

func linkDesc(ctx context.Context, c cid.Cid, name string, dserv format.DAGService) (*nodeInfo, bool, error) {
	var rrd interface {
		io.ReadSeeker
	}

	out := &nodeInfo{
		Cid: c,
	}

	switch c.Type() {
	case cid.DagProtobuf:
		out.Desc = "DAG-PB"

		fnode, err := dserv.Get(ctx, c)
		if err != nil {
			return out, false, nil
		}
		protoBufNode, ok := fnode.(*merkledag.ProtoNode)
		if !ok {
			out.Desc = "NOT!?-DAG-PB"
			return out, false, nil
		}
		fsNode, err := unixfs.FSNodeFromBytes(protoBufNode.Data())
		if err != nil {
			return out, true, err
		}

		switch fsNode.Type() {
		case unixfs.TDirectory:
			out.Desc = fmt.Sprintf("DIR (%d entries)", len(fnode.Links()))
			return out, true, nil
		case unixfs.THAMTShard:
			out.Desc = fmt.Sprintf("HAMT (%d links)", len(fnode.Links()))
			return out, true, nil
		case unixfs.TSymlink:
			out.Desc = fmt.Sprintf("SYMLINK")
			return out, true, nil
		case unixfs.TFile:
			out.Size = types.SizeStr(types.NewInt(fsNode.FileSize()))
		default:
			return nil, false, xerrors.Errorf("unknown ufs type " + fmt.Sprint(fsNode.Type()))
		}

		rrd, err = io2.NewDagReader(ctx, fnode, dserv)
		if err != nil {
			out.Desc = fmt.Sprintf("FILE (pb,e1:%s)", err)
			return out, true, nil
		}

		ctype := mime.TypeByExtension(gopath.Ext(name))
		if ctype == "" {
			mimeType, err := mimetype.DetectReader(rrd)
			if err != nil {
				out.Desc = fmt.Sprintf("FILE (pb,e2:%s)", err)
				return out, true, nil
			}

			ctype = mimeType.String()
		}

		out.Desc = fmt.Sprintf("FILE (pb,%s)", ctype)

		return out, true, nil
	case cid.Raw:
		fnode, err := dserv.Get(ctx, c)
		if err != nil {
			out.Desc = "RAW"
			return out, false, nil
		}

		out.Size = types.SizeStr(types.NewInt(uint64(len(fnode.RawData()))))

		rrd = bytes.NewReader(fnode.RawData())

		ctype := mime.TypeByExtension(gopath.Ext(name))
		if ctype == "" {
			mimeType, err := mimetype.DetectReader(rrd)
			if err != nil {
				return nil, false, err
			}

			ctype = mimeType.String()
		}

		out.Desc = fmt.Sprintf("FILE (raw,%s)", ctype)
		return out, true, nil
	case cid.DagCBOR:
		out.Desc = "DAG-CBOR"
		return out, true, nil
	default:
		out.Desc = fmt.Sprintf("UNK:0x%x", c.Type())
		return out, true, nil
	}
}

func must[T any](c func() (T, error)) T {
	res, err := c()
	if err != nil {
		panic(err)
	}
	return res
}

func noerr[T any](res T, err error) T {
	if err != nil {
		panic(err)
	}
	return res
}
