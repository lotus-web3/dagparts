package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
	"net/http"
	"time"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/gorilla/mux"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (h *dxhnd) handlePingMiner(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	a, err := address.NewFromString(vars["id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	mi, err := h.api.StateMinerInfo(ctx, a, types.EmptyTSK)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if mi.PeerId == nil {
		h.trackerFil.RecordPing(a, 0, errors.New("miner has no peer id"))
		http.Error(w, "nil peerid", http.StatusInternalServerError)
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

	pi := peer.AddrInfo{
		ID:    *mi.PeerId,
		Addrs: multiaddrs,
	}

	{
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		if err := h.NetConnect(ctx, pi); err != nil {
			h.trackerFil.RecordPing(a, 0, xerrors.Errorf("net connect: %w", err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	start := time.Now()

	d, err := h.NetPing(ctx, pi.ID)
	if err != nil {
		h.trackerFil.RecordPing(a, time.Now().Sub(start), xerrors.Errorf("net ping: %w", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.trackerFil.RecordPing(a, time.Now().Sub(start), nil)

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("%s %s", pi.ID, d.Round(time.Millisecond))))
}

func (h *dxhnd) NetConnect(ctx context.Context, p peer.AddrInfo) error {
	if swrm, ok := h.h.Network().(*swarm.Swarm); ok {
		swrm.Backoff().Clear(p.ID)
	}

	return h.h.Connect(ctx, p)
}

func (h *dxhnd) NetPing(ctx context.Context, p peer.ID) (time.Duration, error) {
	result, ok := <-ping.Ping(ctx, h.h, p)
	if !ok {
		return 0, xerrors.Errorf("didn't get ping result: %w", ctx.Err())
	}
	return result.RTT, result.Error
}

func (h *dxhnd) handlePingLotus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)

	aijson, err := base64.URLEncoding.DecodeString(vars["id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pi := peer.AddrInfo{}
	if err := json.Unmarshal(aijson, &pi); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	{
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		if err := h.NetConnect(ctx, pi); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	d, err := h.NetPing(ctx, pi.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("%s", d.Round(time.Millisecond))))
}

func (h *dxhnd) handlePingIPFS(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)

	aijson, err := base64.URLEncoding.DecodeString(vars["id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pi := peer.AddrInfo{}
	if err := json.Unmarshal(aijson, &pi); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	iapi, err := httpapi.NewLocalApi()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	start := time.Now()

	// todo: not quite pinging

	if err := iapi.Swarm().Connect(ctx, pi); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	d := time.Now().Sub(start)

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("%s", d.Round(time.Millisecond))))
}

func (h *dxhnd) pinger() {
	ctx := context.Background()

	for {
		for _, mminer := range h.mminers {
			a := mminer.Addr

			mi, err := h.api.StateMinerInfo(ctx, a, types.EmptyTSK)
			if err != nil {
				time.Sleep(1 * time.Second) // api conn error
				continue
			}

			if mi.PeerId == nil {
				h.trackerFil.RecordPing(a, 0, errors.New("miner has no peer id"))
				continue
			}

			multiaddrs := make([]multiaddr.Multiaddr, 0, len(mi.Multiaddrs))
			for _, a := range mi.Multiaddrs {
				maddr, err := multiaddr.NewMultiaddrBytes(a)
				if err != nil {
					continue
				}
				multiaddrs = append(multiaddrs, maddr)
			}

			pi := peer.AddrInfo{
				ID:    *mi.PeerId,
				Addrs: multiaddrs,
			}

			{
				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				err := h.NetConnect(ctx, pi)
				cancel()
				if err != nil {
					h.trackerFil.RecordPing(a, 0, xerrors.Errorf("net connect: %w", err))
					continue
				}
			}

			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			start := time.Now()

			d, err := h.NetPing(ctx, pi.ID)
			cancel()
			if err != nil {
				h.trackerFil.RecordPing(a, time.Now().Sub(start), xerrors.Errorf("net ping: %w", err))
				continue
			}

			h.trackerFil.RecordPing(a, d, nil)
		}
	}
}
