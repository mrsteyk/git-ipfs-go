package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/dgraph-io/badger/v3"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	ipldgit "github.com/ipfs/go-ipld-git"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
)

const BIG_SIZE int = (1 << 21)                                                     // 2MB, most comfortable for IPFS iirc
const EMPTY_DAG_PB = "bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354" // type of dag-pb, sha256, empty

// Fast MHash for git uses
func CidHash(h string) (cid.Cid, error) {
	// 0x11 - SHA-1
	// 0x14 - 160 bits
	mh, err := multihash.FromHexString("1114" + h)
	if err != nil {
		return cid.Undef, err
	}

	// 0x78 - git-raw codec, ideally use the table but why pull it all up?
	return cid.NewCidV1(0x78, mh), nil
}

// CidHashCompressed?

// Checks for if we did that has already...
func HasHash(t *badger.Txn, hash []byte) (bool, error) {
	_, err := t.Get(hash)
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func AddHash(t *badger.Txn, hash []byte) error {
	return t.Set([]byte(hash), []byte{})
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Ivalid usage!")
	}

	// Todo: file?
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	repo, err := git.PlainOpen(os.Args[1])
	if err != nil {
		log.Fatalf("git open: %v", err)
	}

	store, err := repo.References()
	if err != nil {
		log.Fatalf("git refs: %v", err)
	}
	store.ForEach(func(r *plumbing.Reference) error {
		log.Printf("Ref: %#v\n", r.Name().String())
		return nil
	})

	localRef, err := repo.Reference(plumbing.ReferenceName(os.Args[2]), true)
	if err != nil {
		log.Fatalf("git ref: %v", err)
	}
	log.Printf("ref's hash %#v\n", localRef.Hash().String())

	// IPFS api
	api := ipfs.NewLocalShell()

	data_dagpb_empty, err := api.BlockGet("bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354")
	os.WriteFile("1", data_dagpb_empty, 0777)

	// Push objects...
	hash := localRef.Hash().String()

	// todo := make(chan string)
	// // defer close(todo)
	// todo <- hash
	// for {
	// 	hash, ok := <-todo
	// 	if !ok {
	// 		log.Println("Channel closed!")
	// 	}
	// }

	txn := db.NewTransaction(true)
	pushObject(repo, hash, api, txn)

	hcid, _ := CidHash(hash)
	log.Printf("Ref should be @ %#v!\n", hcid.String())
	var repo_old string
	if len(os.Args) > 3 {
		repo_old = os.Args[3]
	} else {
		repo_old = EMPTY_DAG_PB
	}
	repo_new, err := api.PatchLink(repo_old, os.Args[2], hcid.String(), true)
	if err != nil {
		panic(err)
	}
	log.Printf("New repo should be @ %#v!\n", repo_new)
}

func pushObject(repo *git.Repository, hash string, api *ipfs.Shell, txn *badger.Txn) {
	obj, err := repo.Storer.EncodedObject(plumbing.AnyObject, plumbing.NewHash(hash))
	if err != nil {
		log.Fatalf("git repo encode object: %v\n", err)
	}

	hcid, err := CidHash(hash)
	if err != nil {
		log.Fatalf("CidHash: %v\n", err)
	}
	// log.Printf("Expected CIDv1: %#v\n", hcid.String())

	raw, err := getRawBytes(obj, repo)
	if err != nil {
		log.Fatalf("git raw bytes: %v", err)
	}
	if len(raw) >= BIG_SIZE {
		panic("NO SUPPORT FOR BIG OBJECTS")
	}
	links, err := getLinksFromBytes(raw)
	if err != nil {
		panic(err)
	}
	log.Printf("Object %#v linked to %v", hash, links)

	res, err := api.BlockPut(raw, "git-raw", "sha1", -1)
	if err != nil {
		panic(err)
	}
	if res != hcid.String() {
		log.Fatalf("res (%v) != hcid (%v)", res, hcid)
	}
	log.Printf("Pushed %#v to %#v!\n", hash, res)
	AddHash(txn, []byte(obj.Hash().String()))

	for _, v := range links {
		hash, err := multihash.Decode(v.Hash())
		if err != nil {
			panic(err)
		}
		nh := hex.EncodeToString(hash.Digest)
		dupe, err := HasHash(txn, []byte(nh))
		if err != nil {
			panic(err)
		}
		if !dupe {
			pushObject(repo, nh, api, txn)
		}
	}
}

func getLinksFromBytes(raw []byte) ([]*cid.Cid, error) {
	nd, err := ipldgit.ParseObjectFromBuffer(raw)
	if err != nil {
		return nil, err
	}
	return getLinksFromNode(nd), nil
}

func getLinksFromNode(nd ipld.Node) []*cid.Cid {
	switch nd.Prototype() {
	case ipldgit.Type.Blob:
		return nil
	case ipldgit.Type.Commit:
		// out := []*cid.Cid{}
		commit, _ := nd.(ipldgit.Commit)
		tree := commit.FieldTree()
		c := tree.Link().(cidlink.Link).Cid
		out := []*cid.Cid{&c}
		parents := commit.FieldParents()
		if parents != nil {
			ps := parents.ListIterator()
			for !ps.Done() {
				_, thing, _ := ps.Next()
				lnk, _ := thing.AsLink()
				cid := lnk.(cidlink.Link).Cid
				out = append(out, &cid)
			}
		}
		return out
	case ipldgit.Type.Tag:
		tag, _ := nd.(ipldgit.Tag)
		object := tag.FieldObject()
		lnk, _ := object.AsLink()
		c := lnk.(cidlink.Link).Cid
		return []*cid.Cid{&c}
	case ipldgit.Type.Tree:
		out := []*cid.Cid{}
		tree, _ := nd.(ipldgit.Tree)
		ps := tree.MapIterator()
		for !ps.Done() {
			_, thing, _ := ps.Next()
			treeLink := thing.(ipldgit.TreeEntry).FieldHash()
			lnk, _ := treeLink.AsLink()
			cid := lnk.(cidlink.Link).Cid
			out = append(out, &cid)
		}
		return out
	}
	panic("unreachable")
}

// Compression ver too???
func getRawBytes(obj plumbing.EncodedObject, repo *git.Repository) ([]byte, error) {
	objReader, err := obj.Reader()
	if err != nil {
		return []byte{}, fmt.Errorf("git object reader: %v", err)
	}
	raw, err := ioutil.ReadAll(objReader)
	if err != nil {
		return []byte{}, fmt.Errorf("git object read: %v", err)
	}

	log.Printf("Object %#v is a %v %v", obj.Hash().String(), obj.Type().String(), obj.Size())
	// What the fuck
	switch obj.Type() {
	case plumbing.BlobObject:
		// log.Printf("Object %#v is a blob %v", hash, obj.Size())
		raw = append([]byte(fmt.Sprintf("blob %d\x00", obj.Size())), raw...)
	case plumbing.CommitObject:
		// log.Printf("Object %#v is a commit %v", hash, obj.Size())
		raw = append([]byte(fmt.Sprintf("commit %d\x00", obj.Size())), raw...)
	case plumbing.TagObject:
		// log.Printf("Object %#v is a tag %v", hash, obj.Size())
		raw = append([]byte(fmt.Sprintf("tag %d\x00", obj.Size())), raw...)
	case plumbing.TreeObject:
		// log.Printf("Object %#v is a tree %v", hash, obj.Size())
		raw = append([]byte(fmt.Sprintf("tree %d\x00", obj.Size())), raw...)
	default:
		return []byte{}, fmt.Errorf("object %#v is of unknown type: %v", obj.Hash().String(), obj.Type().String())
	}

	return raw, nil
}
