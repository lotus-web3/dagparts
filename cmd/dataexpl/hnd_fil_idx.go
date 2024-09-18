package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/builtin"
	market14 "github.com/filecoin-project/go-state-types/builtin/v14/market"
	adt14 "github.com/filecoin-project/go-state-types/builtin/v14/util/adt"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	market2 "github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/exp/constraints"
	"html/template"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"

	"github.com/filecoin-project/lotus/chain/types"
)

func (h *dxhnd) handleProviders(w http.ResponseWriter, r *http.Request) {
	pstat, err := h.trackerFil.AllProviderPingStats(time.Hour * 2)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rstat, err := h.trackerFil.AllProviderRetrievalStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filterPing := r.FormValue("ping") != "a"
	filterRetrieval := r.FormValue("retrievals") == "s"

	minerlist := make([]marketMiner, 0, len(h.mminers))
	for _, m := range h.mminers {
		if filterPing && (pstat[m.Addr] == nil || pstat[m.Addr].Success*10 <= pstat[m.Addr].Fail) {
			continue
		}

		if filterRetrieval && (rstat[m.Addr] == nil || rstat[m.Addr].Success == 0) {
			continue
		}

		minerlist = append(minerlist, m)
	}

	sort.SliceStable(minerlist, func(i, j int) bool {
		si := derefOr(rstat[minerlist[i].Addr], RetrievalStats{})
		sj := derefOr(rstat[minerlist[j].Addr], RetrievalStats{})
		if si.Success != sj.Success {
			return si.Success > sj.Success
		}

		pi := derefOr(pstat[minerlist[i].Addr], ProviderPingStats{})
		pj := derefOr(pstat[minerlist[j].Addr], ProviderPingStats{})

		// rate is zero if no pings
		pirate := float64(pi.Success+1) / float64(pi.Success+pi.Fail+1)
		pjrate := float64(pj.Success+1) / float64(pj.Success+pj.Fail+1)

		if pirate != pjrate {
			return pirate > pjrate
		}

		idi, _ := address.IDFromAddress(minerlist[i].Addr)
		idj, _ := address.IDFromAddress(minerlist[j].Addr)

		return idi < idj
	})

	tpl, err := template.New("providers.gohtml").Funcs(map[string]any{
		"sizeClass": sizeClass,
	}).ParseFS(dres, "dexpl/providers.gohtml")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	data := map[string]interface{}{
		"miners":    minerlist,
		"ping":      pstat,
		"retrieval": rstat,

		"pingAll": !filterPing,
		"retrAll": !filterRetrieval,
	}
	if err := tpl.Execute(w, data); err != nil {
		fmt.Println(err)
		return
	}
}

func derefOr[T any](v *T, def T) T {
	if v == nil {
		return def
	}
	return *v
}

func (h *dxhnd) handleProviderStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ma, err := address.NewFromString(vars["id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pstat, err := h.trackerFil.ProviderDetails(ma)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tpl, err := template.New("provstats.gohtml").Funcs(map[string]any{
		"sizeClass": sizeClass,
	}).ParseFS(dres, "dexpl/provstats.gohtml")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	data := map[string]interface{}{
		"Addr": ma,
		"Stat": pstat,
	}
	if err := tpl.Execute(w, data); err != nil {
		fmt.Println(err)
		return
	}
}

func (h *dxhnd) handleClients(w http.ResponseWriter, r *http.Request) {
	type clEntry struct {
		Addr  address.Address
		Count int64
		Data  string
	}

	var clEnts []clEntry
	cf, err := os.OpenFile(filepath.Join(h.clientMeta, "clients.json"), os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.NewDecoder(cf).Decode(&clEnts); err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cf.Close()

	tpl, err := template.ParseFS(dres, "dexpl/clients.gohtml")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	data := map[string]interface{}{
		"clients": clEnts,
	}
	if err := tpl.Execute(w, data); err != nil {
		fmt.Println(err)
		return
	}
}

func (h *dxhnd) handleClient(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ma, err := address.NewFromString(vars["id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type provMeta struct {
		Prov address.Address
		Data string
		Deal abi.DealID
	}

	clDeals := make([]provMeta, 0)

	cf, err := os.OpenFile(filepath.Join(h.clientMeta, fmt.Sprintf("cl-%s.json", ma)), os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.NewDecoder(cf).Decode(&clDeals); err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cf.Close()

	providers := map[address.Address]int{}
	for _, d := range clDeals {
		providers[d.Prov]++
	}

	tpl, err := template.ParseFS(dres, "dexpl/client.gohtml")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	data := map[string]interface{}{
		"deals":     clDeals,
		"addr":      ma,
		"providers": intoList(providers),
	}
	if err := tpl.Execute(w, data); err != nil {
		fmt.Println(err)
		return
	}
}

func intoList[K address.Address, V constraints.Ordered](in map[K]V) []struct {
	K K
	V V
} {
	var out []struct {
		K K
		V V
	}

	for k, v := range in {
		out = append(out, struct {
			K K
			V V
		}{K: k, V: v})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].V > out[j].V
	})

	return out
}

type DealInfo struct {
	DealCID cid.Cid
	Client  address.Address
	Filplus bool
	Size    string
}

func (h *dxhnd) StateMinerSectors(ctx context.Context, maddr address.Address) ([]*miner.SectorOnChainInfo, error) {
	head, err := h.api.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	mAct, err := h.api.StateGetActor(ctx, maddr, head.Key())
	if err != nil {
		return nil, err
	}

	mkAct, err := h.api.StateGetActor(ctx, market2.Address, head.Key())
	if err != nil {
		return nil, err
	}

	stor := adt.WrapStore(ctx, cbor.NewCborStore(h.bs))

	mact, err := miner.Load(stor, mAct)
	if err != nil {
		return nil, err
	}

	var m14 market14.State
	if err := stor.Get(ctx, mkAct.Head, &m14); err != nil {
		return nil, err
	}

	psa, err := adt14.AsMap(stor, m14.ProviderSectors, market14.ProviderSectorsHamtBitwidth)
	if err != nil {
		return nil, err
	}

	mid, err := address.IDFromAddress(maddr)
	if err != nil {
		return nil, err
	}

	var sarrRoot cbg.CborCid
	ex, err := psa.Get(abi.UIntKey(mid), &sarrRoot)
	if err != nil {
		return nil, err
	}
	var sarr *adt14.Map
	if ex {
		sarr, err = adt14.AsMap(stor, cid.Cid(sarrRoot), market14.ProviderSectorsHamtBitwidth)
		if err != nil {
			return nil, err
		}
	}
	if sarr == nil {
		log.Errorw("empty sall", "ma", maddr)
	}

	var ms []*miner.SectorOnChainInfo
	var wg sync.WaitGroup
	var lk sync.Mutex

	var perr error

	err = mact.ForEachDeadline(func(idx uint64, dl miner.Deadline) error {
		return dl.ForEachPartition(func(idx uint64, part miner.Partition) error {
			wg.Add(1)

			go func() {
				defer wg.Done()

				ls, err := part.LiveSectors()
				if err != nil {
					lk.Lock()
					perr = multierror.Append(perr, err)
					lk.Unlock()
					return
				}

				err = ls.ForEach(func(i uint64) error {
					wg.Add(1)

					go func() {
						defer wg.Done()

						sec, err := mact.GetSector(abi.SectorNumber(i))
						if err != nil {
							lk.Lock()
							perr = multierror.Append(perr, err)
							lk.Unlock()
							return
						}

						if len(sec.DealIDs) == 0 && !(sec.DealWeight.IsZero() && sec.VerifiedDealWeight.IsZero()) && sarr != nil {
							var sids market14.SectorDealIDs
							if _, err := sarr.Get(abi.UIntKey(i), &sids); err == nil {
								sec.DealIDs = sids
							}
						}

						lk.Lock()
						ms = append(ms, sec)
						lk.Unlock()
					}()

					return nil
				})
				if err != nil {
					lk.Lock()
					perr = multierror.Append(perr, err)
					lk.Unlock()
					return
				}
			}()

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	wg.Wait()
	if perr != nil {
		return nil, perr
	}

	return ms, nil
}

func (h *dxhnd) handleProviderSectors(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	ma, err := address.NewFromString(vars["id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse 'page' query parameter
	pageStr := r.URL.Query().Get("page")
	page := 1
	if pageStr != "" {
		p, err := strconv.Atoi(pageStr)
		if err == nil && p > 0 {
			page = p
		}
	}

	head, err := h.api.ChainHead(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	mp, err := h.api.StateMinerPower(ctx, ma, head.Key())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ms, err := h.StateMinerSectors(ctx, ma)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Sort sectors by SectorNumber descending
	sort.Slice(ms, func(i, j int) bool {
		return ms[i].SectorNumber > ms[j].SectorNumber
	})

	// Implement pagination
	pageSize := 1000
	totalSectors := len(ms)
	totalPages := (totalSectors + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}
	startIdx := (page - 1) * pageSize
	endIdx := page * pageSize
	if endIdx > totalSectors {
		endIdx = totalSectors
	}
	msPage := ms[startIdx:endIdx]

	// Collect DealIDs from msPage
	var deals []abi.DealID
	for _, info := range msPage {
		for _, d := range info.DealIDs {
			deals = append(deals, d)
		}
	}

	// Process deals
	var wg sync.WaitGroup
	var lk sync.Mutex

	commps := map[abi.DealID]DealInfo{}
	wg.Add(len(deals))

	for _, deal := range deals {
		go func(deal abi.DealID) {
			defer wg.Done()

			cv, found := h.marketDealCache.Get(deal)
			if !found {
				md, err := h.api.StateMarketStorageDeal(ctx, deal, types.EmptyTSK)
				if err != nil {
					return
				}
				cv = md
				h.marketDealCache.Add(deal, cv)
			}

			md := cv.(*api.MarketDeal)

			if md.Proposal.Provider == address.Undef {
				return
			}

			lk.Lock()
			commps[deal] = DealInfo{
				DealCID: md.Proposal.PieceCID,
				Client:  md.Proposal.Client,
				Filplus: md.Proposal.VerifiedDeal,
				Size:    types.SizeStr(types.NewInt(uint64(md.Proposal.PieceSize))),
			}
			lk.Unlock()
		}(deal)
	}
	wg.Wait()

	// Filter out inactive deals from msPage
	for i := range msPage {
		m := msPage[i]
		filtered := make([]abi.DealID, 0, len(m.DealIDs))
		for _, d := range m.DealIDs {
			if _, found := commps[d]; found {
				filtered = append(filtered, d)
			}
		}
		m.DealIDs = filtered
	}

	now, err := h.api.ChainHead(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	mi, err := h.api.StateMinerInfo(r.Context(), ma, now.Key())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	multiaddrs := make([]multiaddr.Multiaddr, 0, len(mi.Multiaddrs))
	for i, a := range mi.Multiaddrs {
		maddr, err := multiaddr.NewMultiaddrBytes(a)
		if err != nil {
			log.Warnf("parsing multiaddr %d (%x): %s", i, a, err)
			continue
		}
		multiaddrs = append(multiaddrs, maddr)
	}

	if mi.PeerId != nil {
		multiaddrs, err = peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
			ID:    *mi.PeerId,
			Addrs: multiaddrs,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	data := map[string]interface{}{
		"maddr":        ma,
		"sectors":      msPage,
		"deals":        commps,
		"info":         mi,
		"addrs":        multiaddrs,
		"qap":          types.SizeStr(mp.MinerPower.QualityAdjPower),
		"raw":          types.SizeStr(mp.MinerPower.RawBytePower),
		"page":         page,
		"totalPages":   totalPages,
		"startIdx":     startIdx,
		"endIdx":       endIdx,
		"totalSectors": totalSectors,
	}

	tpl, err := template.New("sectors.gohtml").Funcs(template.FuncMap{
		"EpochTime": func(e abi.ChainEpoch) string {
			return cliutil.EpochTime(now.Height(), e)
		},
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
			return a - b
		},
	}).ParseFS(dres, "dexpl/sectors.gohtml")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	if err := tpl.Execute(w, data); err != nil {
		fmt.Println(err)
		return
	}
}

func (h *dxhnd) handleDeal(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	did, err := strconv.ParseInt(vars["id"], 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	d, err := h.api.StateMarketStorageDeal(ctx, abi.DealID(did), types.EmptyTSK)
	if err != nil {
		http.Error(w, xerrors.Errorf("StateMarketStorageDeal: %w", err).Error(), http.StatusInternalServerError)
		return
	}

	lstr, err := d.Proposal.Label.ToString()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	dcid, err := cid.Parse(lstr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lstr = dcid.String()
	d.Proposal.Label, _ = market.NewLabelFromString(lstr) // if it's b64, will break urls

	var cdesc string

	{
		// get left side of the dag up to typeCheckDepth
		g := h.getFilRetrieval(r.Context(), d.Proposal.Provider, dcid, "")

		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		root, dserv, _, done, err := g(ssb.ExploreRecursive(selector.RecursionLimitDepth(typeCheckDepth),
			ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
				eb.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
					eb.Insert("Hash", ssb.ExploreRecursiveEdge())
				})))
			})),
		))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer done()

		// this gets type / size / linkcount for root

		desc, _, err := linkDesc(ctx, root, "", dserv)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		cdesc = desc.Desc

		if desc.Size != "" {
			cdesc = fmt.Sprintf("%s %s", cdesc, desc.Size)
		}
	}

	now, err := h.api.ChainHead(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp, err := h.idx.Find(ctx, dcid.Hash())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bitswapProvs := map[peer.ID]int{}
	filProvs := map[peer.ID]int{}

	for _, result := range resp.MultihashResults {
		for _, providerResult := range result.ProviderResults {
			if len(providerResult.Metadata) == 0 {
				log.Errorw("empty provider")
				continue
			}

			var meta metadata.Metadata

			func() {
				defer func() {
					if e := recover(); e != nil {
						log.Errorw("recovered", "e", e, "m", providerResult.Metadata)
					}
				}()
				if err := meta.UnmarshalBinary(providerResult.Metadata); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}()

			for _, proto := range meta.Protocols() {
				switch meta.Get(proto).(type) {
				case *metadata.GraphsyncFilecoinV1:
					filProvs[providerResult.Provider.ID]++
				case *metadata.Bitswap:
					bitswapProvs[providerResult.Provider.ID]++
				}
			}
		}
	}

	tpl, err := template.New("deal.gohtml").Funcs(map[string]interface{}{
		"EpochTime": func(e abi.ChainEpoch) string {
			return cliutil.EpochTime(now.Height(), e)
		},
		"SizeStr": func(s abi.PaddedPieceSize) string {
			return types.SizeStr(types.NewInt(uint64(s)))
		},
	}).ParseFS(dres, "dexpl/deal.gohtml")
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	data := map[string]interface{}{
		"deal":  d,
		"label": lstr,
		"id":    did,

		"provsBitswap": len(bitswapProvs),
		"provsFil":     len(filProvs),

		"contentDesc": cdesc,
	}
	if err := tpl.Execute(w, data); err != nil {
		fmt.Println(err)
		return
	}
}

func (h *dxhnd) handleChains(nets map[string]api.Gateway) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		networks := make([]string, 0, len(nets))
		for n := range nets {
			networks = append(networks, n)
		}
		sort.Strings(networks)

		tpl, err := template.New("chains.gohtml").ParseFS(dres, "dexpl/chains.gohtml")
		if err != nil {
			fmt.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		data := map[string]interface{}{
			"networks": networks,
		}
		if err := tpl.Execute(w, data); err != nil {
			fmt.Println(err)
			return
		}
	}
}

func (h *dxhnd) handleChain(gw api.Gateway, net string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ts, err := gw.ChainHead(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if r.FormValue("epoch") != "" {
			ep, err := strconv.ParseInt(r.FormValue("epoch"), 0, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			ts, err = gw.ChainGetTipSetByHeight(r.Context(), abi.ChainEpoch(ep), ts.Key())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		actors := map[string]cid.Cid{}
		getActor := func(addr address.Address) {
			act, err := gw.StateGetActor(r.Context(), addr, ts.Key())
			if err != nil {
				return
			}

			var data bytes.Buffer
			if err := act.MarshalCBOR(&data); err != nil {
				return
			}

			c, err := (&cid.V1Builder{Codec: cid.DagCBOR, MhType: multihash.IDENTITY}).Sum(data.Bytes())
			if err != nil {
				return
			}

			actors[addr.String()] = c
		}

		getActor(builtin.SystemActorAddr)
		getActor(builtin.InitActorAddr)
		getActor(builtin.RewardActorAddr)
		getActor(builtin.CronActorAddr)
		getActor(builtin.StoragePowerActorAddr)
		getActor(builtin.StorageMarketActorAddr)
		getActor(builtin.VerifiedRegistryActorAddr)
		getActor(builtin.DatacapActorAddr)
		getActor(builtin.BurntFundsActorAddr)

		tpl, err := template.New("chain.gohtml").ParseFS(dres, "dexpl/chain.gohtml")
		if err != nil {
			fmt.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		data := map[string]interface{}{
			"ts":      ts,
			"isHead":  r.FormValue("epoch") == "",
			"actors":  actors,
			"network": net,
		}
		if err := tpl.Execute(w, data); err != nil {
			fmt.Println(err)
			return
		}
	}
}

func (h *dxhnd) handleChainActor(gw api.Gateway) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ts, err := gw.ChainHead(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if r.FormValue("epoch") != "" {
			ep, err := strconv.ParseInt(r.FormValue("epoch"), 0, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			ts, err = gw.ChainGetTipSetByHeight(r.Context(), abi.ChainEpoch(ep), ts.Key())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		addr, err := address.NewFromString(r.FormValue("addr"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		act, err := gw.StateGetActor(r.Context(), addr, ts.Key())
		if err != nil {
			return
		}

		var data bytes.Buffer
		if err := act.MarshalCBOR(&data); err != nil {
			return
		}

		c, err := (&cid.V1Builder{Codec: cid.DagCBOR, MhType: multihash.IDENTITY}).Sum(data.Bytes())
		if err != nil {
			return
		}

		http.Redirect(w, r, "/view/ipfs/"+c.String()+"/?view=ipld", http.StatusFound)
	}
}
