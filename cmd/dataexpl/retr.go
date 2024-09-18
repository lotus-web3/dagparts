package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mdagipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"io"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"golang.org/x/xerrors"
)

func findRoot(ctx context.Context, root cid.Cid, selspec builder.SelectorSpec, ds format.DAGService) (cid.Cid, map[string]struct{}, error) {
	rsn := selspec.Node()

	links := map[string]struct{}{}

	var newRoot cid.Cid
	var errHalt = errors.New("halt walk")

	if err := TraverseDag(
		ctx,
		ds,
		root,
		rsn,
		func(p traversal.Progress, n ipld.Node, r traversal.VisitReason) error {

			fmt.Printf("fr p %#v  r %c\n", p.Path, r)

			links[p.LastBlock.Path.String()] = struct{}{}

			if r == traversal.VisitReason_SelectionMatch {
				if p.LastBlock.Path.String() != p.Path.String() {
					return xerrors.Errorf("unsupported selection path '%s' does not correspond to a block boundary (a.k.a. CID link)", p.Path.String())
				}

				if p.LastBlock.Link == nil {
					// this is likely the root node that we've matched here
					newRoot = root
					return errHalt
				}

				cidLnk, castOK := p.LastBlock.Link.(cidlink.Link)
				if !castOK {
					return xerrors.Errorf("cidlink cast unexpectedly failed on '%s'", p.LastBlock.Link)
				}

				newRoot = cidLnk.Cid

				return errHalt
			}
			return nil
		},
	); err != nil && err != errHalt {
		return cid.Undef, nil, xerrors.Errorf("error while locating partial retrieval sub-root: %w", err)
	}

	if newRoot == cid.Undef {
		return cid.Undef, nil, xerrors.Errorf("path selection does not match a node within %s", root)
	}
	return newRoot, links, nil
}

func TraverseDag(
	ctx context.Context,
	ds mdagipld.DAGService,
	startFrom cid.Cid,
	optionalSelector ipld.Node,
	visitCallback traversal.AdvVisitFn,
) error {

	if optionalSelector == nil {
		optionalSelector = selectorparse.CommonSelector_MatchAllRecursively
	}

	parsedSelector, err := selector.ParseSelector(optionalSelector)
	if err != nil {
		return err
	}

	linkContext := ipld.LinkContext{Ctx: ctx}
	linkSystem := CreateLinkSystem(ds)
	// this is what allows us to understand dagpb
	nodePrototypeChooser := dagpb.AddSupportToChooser(
		func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
			return basicnode.Prototype.Any, nil
		},
	)

	// this is how we pull the start node out of the DS
	startLink := cidlink.Link{Cid: startFrom}
	startNodePrototype, err := nodePrototypeChooser(startLink, linkContext)
	if err != nil {
		return err
	}
	startNode, err := linkSystem.Load(
		linkContext,
		startLink,
		startNodePrototype,
	)
	if err != nil {
		return err
	}

	// this is the actual execution, invoking the supplied callback
	return traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     linkSystem,
			LinkTargetNodePrototypeChooser: nodePrototypeChooser,
		},
	}.WalkAdv(startNode, parsedSelector, visitCallback)
}

func CreateLinkSystem(ds mdagipld.DAGService) linking.LinkSystem {
	// this is how we implement GETs
	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cl, isCid := lnk.(cidlink.Link)
		if !isCid {
			return nil, fmt.Errorf("unexpected link type %#v", lnk)
		}

		node, err := ds.Get(lctx.Ctx, cl.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewBuffer(node.RawData()), nil
	}
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)
	return linkSystem
}
