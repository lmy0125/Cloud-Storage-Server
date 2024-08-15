package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	orderHashes := []string{}
	for key := range c.ServerMap {
		orderHashes = append(orderHashes, key)
	}
	sort.Strings(orderHashes)
	
	server := ""
	for i := 0; i < len(orderHashes); i++ {
		if orderHashes[i] > blockId {
			// log.Println("in")
			server = c.ServerMap[orderHashes[i]]
			break
		}
	}
	// log.Println("server",server, blockId, orderHashes)
	if server == "" {
		server = c.ServerMap[orderHashes[0]]
	}

	return server
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	hashRing := &ConsistentHashRing{};
	serverMap := make(map[string]string)
	for _, addr := range serverAddrs {
		hash := hashRing.Hash("blockstore"+addr)
		serverMap[hash] = addr
	}

	hashRing.ServerMap = serverMap
	return hashRing
}
