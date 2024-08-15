package surfstore

import (
	context "context"
	"fmt"
	"log"
	"math"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// when leader request update
// 1. add to log	2. wait for followers response	3. if majority, commit log for leader; else, wait for all?	4. commit log for followers
// does EntryInput contain all the log entries?
// When to use nextIndex[]? why it is initialize as last log index +1?
// Should the first heart beat be empty entry?
// how to check follower crashed in all the Get functions?

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	// Added for discussion
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	majorityCrashed []*chan bool

	nextIndex  []int64
	matchIndex []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) isMajorityCrashed(ctx context.Context) {
	crashes := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.checkCrash(ctx, int64(idx), addr, crashes)
	}

	totalCrashes := 0
	totalResponses := 1
	// wait in loop for responses
	for {
		result := <-crashes
		totalResponses++
		if result {
			totalCrashes++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalCrashes > len(s.peers)/2 {
		// put on correct channel
		*s.majorityCrashed[0] <- true
	} else {
		*s.majorityCrashed[0] <- false
	}
}

func (s *RaftSurfstore) checkCrash(ctx context.Context, id int64, addr string, crashes chan bool) {
	fmt.Println("DEBUG checkCrash", s.id, id, addr)
	appendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("Fail to dial", addr)
	}
	client := NewRaftSurfstoreClient(conn)
	appendEntryOutput, err := client.AppendEntries(ctx, &appendEntriesInput)

	if !appendEntryOutput.Success {
		crashes <- true
	} else {
		crashes <- false
	}
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if !s.isLeader {
		return &FileInfoMap{}, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return &FileInfoMap{}, ERR_SERVER_CRASHED
	}
	majorityCrashedChann := make(chan bool)
	s.majorityCrashed = make([]*chan bool, 0)
	s.majorityCrashed = append(s.majorityCrashed, &majorityCrashedChann)

	go s.isMajorityCrashed(ctx)

	majorityCrashed := <- majorityCrashedChann
	if majorityCrashed {
		return &FileInfoMap{}, ERR_SERVER_CRASHED
		// return s.GetFileInfoMap(ctx, &emptypb.Empty{})
	}

	fmt.Println("GET File Info Map in Raft", s.id)
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if !s.isLeader {
		return &BlockStoreMap{}, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return &BlockStoreMap{}, ERR_SERVER_CRASHED
	}
	majorityCrashedChann := make(chan bool)
	s.majorityCrashed = make([]*chan bool, 0)
	s.majorityCrashed = append(s.majorityCrashed, &majorityCrashedChann)

	go s.isMajorityCrashed(ctx)

	majorityCrashed := <- majorityCrashedChann
	if majorityCrashed {
		return &BlockStoreMap{}, ERR_SERVER_CRASHED
		// return s.GetBlockStoreMap(ctx, hashes)
	}

	blockStoreMap, err := s.metaStore.GetBlockStoreMap(ctx, hashes)
	if err != nil {
		return &BlockStoreMap{}, fmt.Errorf("GetBlockStoreMap failed in Raft.")
	}
	fmt.Println("GET Block Store Map in Raft", s.id)
	return blockStoreMap, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if !s.isLeader {
		return &BlockStoreAddrs{}, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return &BlockStoreAddrs{}, ERR_SERVER_CRASHED
	}
	majorityCrashedChann := make(chan bool)
	s.majorityCrashed = make([]*chan bool, 0)
	s.majorityCrashed = append(s.majorityCrashed, &majorityCrashedChann)

	go s.isMajorityCrashed(ctx)

	majorityCrashed := <- majorityCrashedChann
	if majorityCrashed {
		return &BlockStoreAddrs{}, ERR_SERVER_CRASHED
		// return s.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	}

	fmt.Println("GET Block Store Addrs in Raft", s.id)
	return &BlockStoreAddrs{BlockStoreAddrs: s.metaStore.BlockStoreAddrs}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if !s.isLeader {
		return &Version{Version: -2}, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return &Version{Version: -2}, ERR_SERVER_CRASHED
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})

	commitChan := make(chan bool)
	s.pendingCommits = make([]*chan bool, 0)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	fmt.Println("Update File", s.peers[s.id], s.isLeader, "Term:", s.term, "commitedIndex:", s.commitIndex, "lastApplied:", s.lastApplied)
	for idx := range s.log {
		fmt.Println(s.id, s.log[idx].Term, s.log[idx].FileMetaData.Filename, s.log[idx].FileMetaData.Version)
	}
	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan
	fmt.Println("DEBUG commit", commit)
	// once committed, apply to the state machine
	if commit {
		version, err := s.metaStore.UpdateFile(ctx, filemeta)
		if err != nil {
			return &Version{Version: -2}, fmt.Errorf("Update MetaStore fail.")
		}
		s.lastApplied++
		metaMap, _ := s.GetFileInfoMap(ctx, &emptypb.Empty{})
		PrintMetaMap(metaMap.FileInfoMap)
		// respond after entry applied to state machine
		s.SendHeartbeat(ctx, &emptypb.Empty{})
		return version, err
	} else {
		return &Version{Version: -2}, fmt.Errorf("Majority didn't commit.")
	}
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower(ctx, int64(idx), addr, responses)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalAppends > len(s.peers)/2 {
		// put on correct channel
		*s.pendingCommits[0] <- true
		// update commit Index correctly
		s.commitIndex++
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, id int64, addr string, responses chan bool) {
	var prevLogTerm int64
	var prevLogIndex int64
	// if len(s.log) > 0 {
	// 	prevLogTerm = s.log[len(s.log)-1].Term
	// 	prevLogIndex = int64(len(s.log) - 1)
	// } else {
	// 	prevLogTerm = -1
	// 	prevLogIndex = -1
	// }
	if s.nextIndex[id] > 0 {
		prevLogIndex = s.nextIndex[id]-1
		prevLogTerm = s.log[prevLogIndex].Term
	} else {
		prevLogTerm = -1
		prevLogIndex = -1
	}

	appendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("Fail to dial", addr)
	}
	client := NewRaftSurfstoreClient(conn)

	appendEntryOutput, err := client.AppendEntries(ctx, &appendEntriesInput)

	if appendEntryOutput.Success {
		s.nextIndex[id]++
		responses <- true
	} else {
		responses <- false
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	if s.isCrashed {
		fmt.Println(s.id, "id crashed")
		return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: -1}, nil
	}
	if input.Term < s.term {
		return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: -1}, nil
	}
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}

	if len(s.log) > 0 && input.PrevLogIndex >= 0{
		if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: -1}, nil
		}
	}

	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	for s.commitIndex > s.lastApplied {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)

	// TODO actually check entries
	s.log = input.Entries

	for s.lastApplied < input.LeaderCommit {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(s.lastApplied)))
	}

	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	fmt.Println("Append Entries", s.peers[s.id], s.isLeader, "Term:", s.term, "commitedIndex:", s.commitIndex, "lastApplied:", s.lastApplied)
	for idx := range s.log {
		fmt.Println(s.id, s.log[idx].Term, s.log[idx].FileMetaData.Filename, s.log[idx].FileMetaData.Version)
	}
	return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: true, MatchedIndex: s.lastApplied}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	// update state
	initNextIndex := make([]int64, 0)
	initMatchIndex := make([]int64, 0)

	for range s.peers {
		initNextIndex = append(initNextIndex, int64(len(s.log)))
		initMatchIndex = append(initMatchIndex, -1)
	}
	s.nextIndex = initNextIndex
	s.matchIndex = initMatchIndex

	fmt.Println("Set Leader", s.peers[s.id], s.isLeader, "Term:", s.term, "commitedIndex:", s.commitIndex, "lastApplied:", s.lastApplied)

	return &Success{Flag: true}, nil
}

// Sends a round of AppendEntries to all other nodes. The leader will attempt to replicate logs to all other nodes when this is called.
// It can be called even when there are no entries to replicate. If a node is not in the leader state it should do nothing
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	// var prevLogTerm int64
	// var prevLogIndex int64
	// if len(s.log) > 0 {
	// 	prevLogTerm = s.log[len(s.log)-1].Term
	// 	prevLogIndex = int64(len(s.log) - 1)
	// } else {
	// 	prevLogTerm = -1
	// 	prevLogIndex = -1
	// }

	// contact all followers
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		
		var prevLogTerm int64
		var prevLogIndex int64
		if s.nextIndex[idx] > 0 {
			prevLogIndex = s.nextIndex[idx]-1
			prevLogTerm = s.log[prevLogIndex].Term
		} else {
			prevLogTerm = -1
			prevLogIndex = -1
		}
	
		entryInput := AppendEntryInput{
			Term: s.term,
			// TODO put the right values
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return &Success{Flag: false}, fmt.Errorf("Failed to dial " + addr)
		}
		client := NewRaftSurfstoreClient(conn)

		_, _ = client.AppendEntries(ctx, &entryInput)
	}

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)