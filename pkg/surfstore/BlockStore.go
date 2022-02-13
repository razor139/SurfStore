package surfstore

import (
	context "context"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//panic("todo")
	if block, ok := bs.BlockMap[blockHash.GetHash()]; ok {
		return &Block{BlockData: block.BlockData, BlockSize: block.BlockSize}, nil
	} else {
		return &Block{}, fmt.Errorf("Hash not present in Block Map:%v", blockHash.GetHash())
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	hashString := GetBlockHashString(block.BlockData[0:block.BlockSize])
	fmt.Println("Hash string:", hashString, " block size:", block.BlockSize, " block len:", len(block.BlockData))

	if len(hashString) == 0 {
		return &Success{Flag: false}, fmt.Errorf("Hash string not generated")
	}
	bs.BlockMap[hashString] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo")
	var hashSubSet BlockHashes

	for _, hash := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[hash]; ok {
			hashSubSet.Hashes = append(hashSubSet.Hashes, hash)
		}
	}
	return &hashSubSet, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
