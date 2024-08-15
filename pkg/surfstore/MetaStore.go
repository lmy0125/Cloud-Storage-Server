package surfstore

import (
	context "context"
	"fmt"
	"log"
	"strings"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	log.Println("GET in Meta Store")
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	log.Println("Update in Meta Store")
	// if file was not in map
	if _, ok := m.FileMetaMap[fileMetaData.Filename]; !ok {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: 1}, nil
	}
	// if version matches
	if fileMetaData.Version == 1+m.FileMetaMap[fileMetaData.Filename].Version {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	} else {
		return &Version{Version: -1}, fmt.Errorf("File version not match")
	}
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	log.Println("GET Block Store Map")
	// map from block server address to block hashes
	blockMap := make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		addr := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		addr = "localhost:"+strings.Split(addr, ":")[1]
		if _, ok := blockMap[addr]; !ok {
			blockMap[addr] = &BlockHashes{Hashes: []string{blockHash}}
		} else {
			blockMap[addr].Hashes = append(blockMap[addr].Hashes, blockHash)
		}
	}

	return &BlockStoreMap{BlockStoreMap: blockMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
