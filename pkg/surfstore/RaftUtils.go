package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),	//each entry contains command for state machine, and term when entry was received by leader (first index is 0)
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		// added
		id:             id,
		peers:          config.RaftAddrs,
		pendingCommits: make([]*chan bool, 0),
		commitIndex:    -1,					// index of highest log entry known to be committed (initialized to -1, increases monotonically)
		lastApplied:    -1,					// index of highest log entry applied to state machine (initialized to -1, increases monotonically)
		
		majorityCrashed: make([]*chan bool, 0),
		// for leaders
		nextIndex: 		make([]int64, 0),	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
		matchIndex: 	make([]int64, 0),	// for each server, index of highest log entry known to be replicated on server (initialized to -1, increases monotonically)
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		return e
	}

	return grpcServer.Serve(l)
}
