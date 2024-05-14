package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (

	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower            int           = 0
	Leader              int           = 1
	Candidate           int           = -1
	ElectionRandomRange int           = 400
	HeartbeatTimeout    time.Duration = 40 * time.Millisecond
	rpcTimeOut          time.Duration = 70 * time.Millisecond
	ElectionTimeout     time.Duration = 450 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//all servers
	commitIndex int
	lastApplied int
	//leader
	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg

	currentTerm             int
	votedFor                int //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	logs                    []Log
	state                   int
	electionTimerch         chan struct{}
	HeartbeatTimeoutTimerch chan struct{}
	votes                   int
	lastIncludedIndex       int
	lastIncludedTerm        int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Log struct {
	Command interface{}
	Term    int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	//log entries to store,empty for HeartbeatTimeout
	Entries []Log
	//leader's commit idx
	// LeaderCommit int
	LeaderCommit int
	LastApplied  int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
	Me            int
}

type AppendEntries struct {
	Req   *AppendEntriesArgs
	Reply *AppendEntriesReply
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) getLogEntryByIndex(index int) (int, int) {
	if index-rf.lastIncludedIndex == 0 {
		return rf.lastIncludedTerm, rf.lastIncludedIndex
	} else if index <= len(rf.logs)+rf.lastIncludedIndex && index > rf.lastIncludedIndex {
		return rf.logs[index-rf.lastIncludedIndex-1].Term, index - rf.lastIncludedIndex - 1
	} else {
		return -1, -1
	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("::::LockGetState")

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	DPrintf("me[%d] Term[%d] isLeader[%t]", rf.me, term, isleader) //fmt.Println("::::UnLockGetState")

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Error: raft[%d] readPersist.", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logs = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		var logLength = len(rf.logs)
		DPrintf("[%v] raft[%d] readPersist, term:[%d], votedFor:[%d], logLength[%d]", time.Now(), rf.me, rf.currentTerm, rf.votedFor, logLength)
		rf.mu.Unlock()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastApplied {
		return
	}

	temp := rf.logs[index-rf.lastIncludedIndex:]

	rf.lastIncludedTerm = rf.logs[index-rf.lastIncludedIndex-1].Term
	rf.lastIncludedIndex = index
	rf.logs = make([]Log, len(temp))
	copy(rf.logs, temp)
	DPrintf("snapshot index[%d] raft[%d] len[%d] lastIncludeIdx[%d]", index, rf.me, len(rf.logs), rf.lastIncludedIndex)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)

}

func (rf *Raft) GetRfState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("VoteLock3:::::%d", rf.me)

	var flag = false
	if args.Term > rf.currentTerm {
		rf.transState(Follower, args.Term)
	}
	var lastLogTerm int
	switch len(rf.logs) {
	case 0:
		lastLogTerm = rf.lastIncludedTerm
	default:
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.Term >= rf.currentTerm {
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.logs)+rf.lastIncludedIndex) {
			flag = true
			rf.votedFor = args.CandidateId
			rf.resetTimer()
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = flag
	rf.persist()

	// rf.votedFor = args.CandidateId

	DPrintf("VoteRaft Term[%d] VotedFor[%d] server[%d] LastLogTerm[%d] LogsLength[%d]", rf.currentTerm, rf.votedFor, rf.me, lastLogTerm, len(rf.logs)+rf.lastIncludedIndex)

	DPrintf("VoteUnLock3:::me:%d::ArgsTerm::%d:%#v", rf.me, args.Term, reply)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("RPC vote server[%d] me[%d]", server, rf.me)
	return ok
}

var chMu sync.Mutex

func (rf *Raft) SendRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// rplys := []*RequestVoteReply{}
	chRplys := make(chan interface{}, len(rf.peers)-1)
	chTimeout := make(chan struct{}, len(rf.peers))

	//超时处理
	go rf.quitRpcTimeout(chRplys, chTimeout, &chMu)

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		var reply = &RequestVoteReply{}
		i := idx
		go func() {
			rf.sendRequestVote(i, args, reply)
			select {
			case <-chTimeout:
				DPrintf("rpc超时,chan已关闭,me::%d", i)
			default:
				//防止chan在close时被写入造成data race
				chMu.Lock()
				DPrintf("rpc响应,me::%d", i)
				chRplys <- *reply
				chMu.Unlock()
			}
		}()
	}

	//阻塞等待rpc超时
	DPrintf("TimeoutBefore:%d", rf.me)
	<-chTimeout
	DPrintf("Timeoutaft:%d,len::%d", rf.me, len(chTimeout))

	for reply := range chRplys {
		rf.mu.Lock()
		rply := reply.(RequestVoteReply)
		DPrintf("GetVoteRply:::me[%d] term[%d] rply[%#v]", rf.me, rf.currentTerm, rply)
		if rply.Term == rf.currentTerm && rply.VoteGranted {
			rf.votes++
		} else if rply.Term > rf.currentTerm {
			// rf.resetTimer()
			rf.transState(Follower, rply.Term)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		DPrintf("VoteUnLock1:::::%d::votes::%d", rf.me, rf.votes)
	}

	rf.mu.Lock()
	DPrintf("VoteLock2:::::%d::votes::%d::state::%d", rf.me, rf.votes, rf.state)
	if rf.votes > len(rf.peers)/2 && rf.state == Candidate {
		rf.transState(Leader, rf.currentTerm)
		rf.mu.Unlock()
		DPrintf("VoteUnLock2:::::%d", rf.me)
		rf.initializeNextIndex()
		DPrintf("SenHeartBeatTimeout.Leader:%d", rf.me)
		// go rf.sendHeartBeatTimeout()
		rf.sendAppendEntriesAll(0)
	} else {
		rf.transState(Follower, rf.currentTerm)
		rf.mu.Unlock()
		DPrintf("VoteUnLock2:::::%d", rf.me)
	}
}

func (rf *Raft) initializeNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.logs) + rf.lastIncludedIndex + 1
		rf.matchIndex[idx] = -1
	}

}

func (rf *Raft) quitRpcTimeout(chReplys chan interface{}, chTimeout chan struct{}, mu *sync.Mutex) {
	time.Sleep(rpcTimeOut)
	for i := 0; i < cap(chTimeout); i++ {
		chTimeout <- struct{}{}
		DPrintf("close::::%d,len::%d", i, len(chTimeout))
	}
	DPrintf("cancel:::::::::me::%d", rf.me)
	mu.Lock()
	close(chReplys)
	mu.Unlock()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()

	if !isLeader {
		return 0, 0, isLeader
	}

	rf.mu.Lock()
	appendEntrie := Log{
		Command: command,
		Term:    term,
	}
	rf.logs = append(rf.logs, appendEntrie)
	lenLogs := len(rf.logs)
	lastIncludeindex := rf.lastIncludedIndex

	DPrintf("LeaderServer[%d] idx[%d] entries[%#v] lastIncludeIdx[%d]", rf.me, len(rf.logs)+lastIncludeindex, appendEntrie, lastIncludeindex)
	rf.mu.Unlock()

	// rf.sendAppendEntriesAll(lenLogs)

	return lenLogs + lastIncludeindex, term, isLeader
}

func (rf *Raft) batchAppendEntries() {
	// prelength := 0

	for !rf.killed() {
		rf.mu.Lock()
		lenLogs := len(rf.logs)
		rf.mu.Unlock()
		if rf.GetRfState() != Leader {
			time.Sleep(rpcTimeOut)
			continue
		}
		DPrintf("LogLen[%d]", lenLogs)
		// if lenLogs > 0 {
		// prelength = lenLogs
		// rf.mu.Lock()
		// rf.persist()
		// lastIndex := rf.lastIncludedIndex
		// rf.mu.Unlock()
		// rf.sendAppendEntriesAll(lenLogs + lastIndex)
		// }
		rf.mu.Lock()
		rf.persist()
		lastIndex := rf.lastIncludedIndex
		rf.mu.Unlock()
		rf.sendAppendEntriesAll(lenLogs + lastIndex)

		time.Sleep(HeartbeatTimeout)
	}

}

func (rf *Raft) sendHeartBeatTimeout() {
	reply := &AppendEntriesReply{}
	for idx, _ := range rf.peers {
		term, isLeader := rf.GetState()
		if !isLeader {
			return
		}
		args := rf.getEntriesArgs(idx)
		args.Entries = nil

		if idx != rf.me {
			DPrintf("HeartBeat")
			rf.sendAppendEntries(idx, &args, reply)
			if reply.Term == term {
				rf.mu.Lock()
				if !reply.Success {
					rf.handleConflitReply(reply)
				} else {
					match := len(args.Entries) + args.PrevLogIndex
					next := match + 1
					rf.nextIndex[idx] = max(rf.nextIndex[idx], next)
					rf.matchIndex[idx] = max(rf.matchIndex[idx], match)
				}
				rf.mu.Unlock()
			} else if reply.Term > term {
				rf.mu.Lock()
				rf.transState(Follower, reply.Term)
				rf.mu.Unlock()
				return
			}
		}
	}
}

func (rf *Raft) handleConflitReply(reply *AppendEntriesReply) {
	last := 0
	if rf.currentTerm != reply.Term || reply.ConflictTerm == 0 {
		return
	}

	if rf.nextIndex[reply.Me] <= 1 {
		return
	}
	if reply.ConflictTerm != -1 {
		last = rf.searchMatchIndexByTerm(reply.ConflictTerm, len(rf.logs)+rf.lastIncludedIndex, false)
		if last != rf.lastIncludedIndex {
			rf.nextIndex[reply.Me] = last
		}
	}
	if last == rf.lastIncludedIndex || last == 0 {
		rf.nextIndex[reply.Me] = reply.ConflictIndex
	}
	DPrintf("fast reTrack reply[%#v] nextIndex[%d]", reply, rf.nextIndex[reply.Me])
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		state := rf.GetRfState()
		if state == Leader {
			time.Sleep(HeartbeatTimeout)
			select {
			case <-rf.HeartbeatTimeoutTimerch:
				DPrintf("HeartBeatTimeout Timeout")
			default:
				// go rf.sendHeartBeatTimeout()
				// rf.sendAppendEntriesAll(0)
			}
		} else {
			//随机定时器，时间范围为400-600ms，心跳间隔为200ms
			rand.Seed(time.Now().UnixNano())
			randomInt := rand.Intn(ElectionRandomRange)
			DPrintf("election timer :: %d,server:%d", randomInt, rf.me)
			time.Sleep(time.Duration(randomInt)*time.Millisecond + ElectionTimeout)

			select {
			case <-rf.electionTimerch:
				DPrintf("election timeout")
			default:
				rf.resetTimer()
				rf.startElection()
			}

		}

	}
}

func (rf *Raft) resetTimer() {
	DPrintf("Raft[%d] resetElectionTimer", rf.me)
	if len(rf.electionTimerch) == 0 {
		rf.electionTimerch <- struct{}{}
	}
}

func (rf *Raft) stopHeartBeatTimeout() {
	if len(rf.HeartbeatTimeoutTimerch) == 0 {
		rf.HeartbeatTimeoutTimerch <- struct{}{}
	}
}

func (rf *Raft) transState(state int, term int) {
	DPrintf("TransTo[%d] me[%d] term[%d]", state, rf.me, term)
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.persist()
	}
	switch state {
	case Follower:
		rf.state = Follower
		rf.currentTerm = term
	case Candidate:
		rf.state = Candidate
		rf.currentTerm = term
		rf.votedFor = rf.me
		rf.votes = 1
		rf.persist()
	case Leader:
		rf.state = Leader
	}
}

var appendMu sync.Mutex

func (rf *Raft) sendAppendEntriesAll(index int) {
	if rf.state != Leader {
		return
	}

	chRplys := make(chan interface{}, len(rf.peers)-1)
	chTimeout := make(chan struct{}, len(rf.peers))
	go rf.quitRpcTimeout(chRplys, chTimeout, &appendMu)

	for idx, _ := range rf.peers {
		if idx != rf.me {
			if rf.GetRfState() != Leader {
				return
			}
			i := idx
			rf.mu.Lock()
			nextIndex := rf.nextIndex[idx]
			lastIndex := rf.lastIncludedIndex
			DPrintf("firstSendpeer[%d] next[%d] lastIndex[%d]", idx, nextIndex, lastIndex)
			rf.mu.Unlock()

			if nextIndex <= lastIndex {
				rf.mu.Lock()

				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()

				go func() {
					reply := &InstallSnapshotReply{}
					rf.sendInstallSnapshot(i, args, reply)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.transState(Follower, reply.Term)
					} else {
						rf.nextIndex[i] = max(rf.nextIndex[i], rf.lastIncludedIndex+1)
						rf.matchIndex[i] = max(rf.matchIndex[i], rf.lastIncludedIndex)
					}
					rf.mu.Unlock()

				}()

			} else {

				// if args.Entries == nil {
				// 	continue
				// }
				go func() {
					DPrintf("append entries")
					reply := &AppendEntriesReply{}
					args := rf.getEntriesArgs(i)
					rf.sendAppendEntries(i, &args, reply)
					select {
					case <-chTimeout:
						DPrintf("rpc超时,chan已关闭,me::%d", i)
					default:
						//防止chan在close时被写入造成data race
						appendMu.Lock()
						DPrintf("rpc响应,me::%d", i)
						chRplys <- *reply
						appendMu.Unlock()
					}
				}()
			}

		}
	}

	//阻塞等待rpc超时
	DPrintf("TimeoutBefore:%d", rf.me)
	<-chTimeout
	DPrintf("Timeoutaft:%d,len::%d", rf.me, len(chTimeout))

	var cnt = 0

	for reply := range chRplys {
		rf.mu.Lock()
		rply := reply.(AppendEntriesReply)

		DPrintf("server[%d] term[%d] leaderTerm[%d]", rply.Me, rply.Term, rf.currentTerm)
		if rply.Term > rf.currentTerm {
			rf.transState(Follower, rply.Term)
			rf.mu.Unlock()
			return
		}
		if rply.Term == rf.currentTerm && rply.Success {
			DPrintf(" agree reply[%#v]", rply)
			match := index
			next := index + 1
			rf.nextIndex[rply.Me] = max(rf.nextIndex[rply.Me], next)
			rf.matchIndex[rply.Me] = max(rf.matchIndex[rply.Me], match)
			DPrintf("Agree server[%d]", rply.Me)
			cnt++
		} else if rply.Term > rf.currentTerm {
			rf.transState(Follower, rply.Term)
			rf.mu.Unlock()
			return
		} else {
			rf.handleConflitReply(&rply)
			go rf.retry(rply.Me)
		}
		rf.mu.Unlock()
	}

	//commit log
	DPrintf("commit numbers [%d]", cnt+1)

	if cnt+1 > len(rf.peers)/2 && index > 0 {
		DPrintf("commit Leader[%d] idx[%d]", rf.me, index)

		rf.mu.Lock()
		//抛弃过时的commitIdx更新
		if index > rf.commitIndex && rf.logs[index-rf.lastIncludedIndex-1].Term == rf.currentTerm {
			rf.commitIndex = index
		}

		rf.mu.Unlock()

		// rf.stopHeartBeatTimeout()
		// go rf.sendHeartBeatTimeout()
		rf.sendAppendEntriesAll(0)
	}

}

func (rf *Raft) retry(peer int) {
	if peer == rf.me {
		return
	}
	for !rf.killed() {
		if rf.GetRfState() != Leader {
			return
		}
		rf.mu.Lock()
		DPrintf("Retry peer[%d] next[%d] lastIndex[%d]", peer, rf.nextIndex[peer], rf.lastIncludedIndex)

		if rf.nextIndex[peer] <= rf.lastIncludedIndex {

			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			rf.mu.Unlock()

			reply := &InstallSnapshotReply{}
			rf.sendInstallSnapshot(peer, args, reply)

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.transState(Follower, reply.Term)
			} else {
				rf.nextIndex[peer] = max(rf.nextIndex[peer], rf.lastIncludedIndex+1)
				rf.matchIndex[peer] = max(rf.matchIndex[peer], rf.lastIncludedIndex)
			}
			rf.mu.Unlock()

			break
		}
		rf.mu.Unlock()
		rply := &AppendEntriesReply{}
		args := rf.getEntriesArgs(peer)
		DPrintf("retry")
		rf.sendAppendEntries(peer, &args, rply)
		rf.mu.Lock()
		DPrintf("Retry Raft[%d] server[%d] args[%#v]", rf.me, peer, args)
		if rply.Term == rf.currentTerm && rply.Success {
			match := len(args.Entries) + args.PrevLogIndex
			next := match + 1
			rf.nextIndex[rply.Me] = max(rf.nextIndex[rply.Me], next)
			rf.matchIndex[rply.Me] = max(rf.matchIndex[rply.Me], match)
			rf.mu.Unlock()
			break
		} else if rply.Term > rf.currentTerm {
			rf.transState(Follower, rply.Term)
			rf.mu.Unlock()
			return
		} else {
			rf.handleConflitReply(rply)
			rf.mu.Unlock()
		}
		time.Sleep(rpcTimeOut)
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) getEntriesArgs(peer int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextIndex := rf.nextIndex[peer]
	var prevLogIndex = nextIndex - 1

	var entry []Log
	if nextIndex-1 == len(rf.logs)+rf.lastIncludedIndex {
		entry = nil
	} else if nextIndex-1 > rf.lastIncludedIndex && nextIndex-1 < len(rf.logs)+rf.lastIncludedIndex {
		entry = rf.logs[nextIndex-1-rf.lastIncludedIndex : len(rf.logs)]
	} else if nextIndex-1 == rf.lastIncludedIndex {
		entry = rf.logs
	}

	DPrintf("lastIncludeIndex[%d] nextIndex[%d] EntryLength[%d] server[%d]", rf.lastIncludedIndex, nextIndex, len(rf.logs)+1, peer)
	// entries := make(map[int]Log)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		Entries:      entry,
		LeaderCommit: rf.commitIndex,
	}

	DPrintf("prevLogIdx[%d] lastIncludeIdx[%d] len[%d]", prevLogIndex, rf.lastIncludedIndex, len(rf.logs))
	if prevLogIndex > rf.lastIncludedIndex && prevLogIndex-1 < len(rf.logs)+rf.lastIncludedIndex {
		args.PrevLogTerm = rf.logs[prevLogIndex-1-rf.lastIncludedIndex].Term
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	}

	return args
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.transState(Follower, args.Term)
	}

	DPrintf("LogAppendLock::%d", rf.me)

	entriesLen := len(args.Entries)
	prevLogIndex := args.PrevLogIndex

	check := rf.checkEntries(args, reply)
	if !check {
		reply.Success = false
		DPrintf("false reply[%#v] currentTerm[%d]", reply, rf.currentTerm)
	} else {
		reply.Success = true
		if args.Entries != nil {
			temp := append(rf.logs, args.Entries...)
			rf.logs = make([]Log, len(temp))
			copy(rf.logs, temp)
		}
		DPrintf("Raft[%d] Logs[]", rf.me)
	}
	reply.Term = rf.currentTerm
	reply.Me = rf.me

	if args.LeaderCommit > rf.commitIndex && check {
		DPrintf("leadercommit[%d] index of last new entry[%d]", args.LeaderCommit, entriesLen+prevLogIndex)
		//set commitIndex=min(LeaderCommit,index of last new entry)
		rf.commitIndex = func(x int, y int) int {
			if x > y {
				return y
			} else {
				return x
			}
		}(args.LeaderCommit, entriesLen+prevLogIndex)
	}
	rf.persist()
}

// todo:修改判斷条件
func (rf *Raft) checkEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("lastIndex[%d] lastLogTerm[%d] prevIndex[%d] prevTerm[%d] server[%d]", rf.lastIncludedIndex, rf.lastIncludedTerm, args.PrevLogIndex, args.PrevLogTerm, rf.me)

	// ret := true
	if args.Term < rf.currentTerm {
		return false
	}

	rf.resetTimer()

	if args.PrevLogIndex > len(rf.logs)+rf.lastIncludedIndex {
		reply.ConflictIndex = len(rf.logs) + rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return false
	}

	if args.PrevLogIndex > rf.lastIncludedIndex && rf.logs[args.PrevLogIndex-rf.lastIncludedIndex-1].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
		reply.ConflictIndex = rf.searchMatchIndexByTerm(reply.ConflictTerm, args.PrevLogIndex, true)
		return false
	}
	duplication := -1
	flag := false

	for idx, v := range args.Entries {
		newIdx := args.PrevLogIndex + idx
		DPrintf("newIdx[%d] len[%d]", newIdx, len(rf.logs))
		if newIdx >= len(rf.logs)+rf.lastIncludedIndex {
			break
		}
		if rf.logs[newIdx-rf.lastIncludedIndex].Term != v.Term {
			rf.logs = rf.logs[0 : newIdx-rf.lastIncludedIndex]
			args.Entries = args.Entries[idx:]
			flag = true
			break
		} else {
			duplication = idx
		}
	}

	if duplication != -1 && !flag {
		DPrintf("Duplicate[%d]", duplication)
		args.Entries = args.Entries[duplication+1:]
	}

	return true
}

func (rf *Raft) searchMatchIndexByTerm(term int, index int, isFirst bool) int {
	val := 0
	index = index - rf.lastIncludedIndex
	var i int
	for i = index; i > 1; i-- {
		// if isFirst {
		if rf.logs[i-1].Term == term && rf.logs[i-2].Term < term {
			if isFirst {
				val = i
			} else {
				val = i - 1
			}

			break
		}
	}
	if i == 1 && rf.logs[i-1].Term == term {
		if isFirst {
			val = i
		} else {
			val = i - 1
		}
	}
	val = val + rf.lastIncludedIndex
	DPrintf("Raft[%d] state[%d] val[%d]", rf.me, rf.state, val)
	return val
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("RPC entries server[%d] me[%d]", server, rf.me)
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("send InstallSnapshot server[%d] me[%d]", server, rf.me)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	flag := func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("InstallSnapshot me[%d] currentTerm[%d] argsTerm[%d]", rf.me, rf.currentTerm, args.Term)

		if args.Term > rf.currentTerm {
			rf.transState(Follower, args.Term)
		}

		reply.Term = rf.currentTerm

		if args.Term < rf.currentTerm {
			return false
		}

		rf.resetTimer()
		if args.LastIncludedIndex <= rf.lastIncludedIndex {
			return false
		}

		defer rf.persist()

		defer func() {
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
			rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
		}()

		term, idx := rf.getLogEntryByIndex(args.LastIncludedIndex)
		if idx != -1 && term == args.LastIncludedTerm {
			if idx+1 < len(rf.logs) {
				temp := rf.logs[idx+1:]
				copy(rf.logs, temp)
			}
			return false
		}

		rf.logs = make([]Log, 0)

		return true
	}()

	if flag {
		DPrintf("apply snapshot me[%d]", rf.me)
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("::lock::StartElection%d", rf.me)

	// rf.currentTerm++
	// rf.votedFor = rf.me
	// rf.state = Candidate
	// rf.votes = 1
	rf.transState(Candidate, rf.currentTerm+1)

	var lastLogTerm int
	switch len(rf.logs) {
	case 0:
		lastLogTerm = rf.lastIncludedTerm
	default:
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) + rf.lastIncludedIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := &RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
	rf.mu.Unlock()
	DPrintf("::Unlock::StartElection%d", rf.me)

	DPrintf("VoteArgs[%#v] server[%d]", args, rf.me)
	rf.SendRequestVote(args, reply)

}

func (rf *Raft) loopApplyMsg() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.checkN()

		var appliedMsgs = make([]ApplyMsg, 0)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//此处不可加term判断，应服从Leader指令
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied-rf.lastIncludedIndex-1].Command,
					CommandIndex: rf.lastApplied,
					// CommandTerm:  rf.logs[rf.lastApplied-1].Term,
				})
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] command[%#v]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.logs[rf.lastApplied-rf.lastIncludedIndex-1].Command)
			}
		}()
		// 锁外提交给应用层
		for _, msg := range appliedMsgs {
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) checkN() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	idx := 0

	for i := rf.commitIndex - rf.lastIncludedIndex - 1; i < len(rf.logs) && i >= 0; i++ {
		cnt := 0
		for _, v := range rf.matchIndex {
			if v >= i+1+rf.lastIncludedIndex {
				cnt++
			}
		}
		if cnt+1 > len(rf.peers)/2 && rf.logs[i].Term == rf.currentTerm {
			idx = i + 1
		} else {
			break
		}
	}
	idx = idx + rf.lastIncludedIndex
	if idx != 0 && idx > rf.commitIndex {
		DPrintf("CheckN commitIdx[%d] newIdx[%d]", rf.commitIndex, idx)
		rf.commitIndex = idx
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		dead:                    0,
		logs:                    make([]Log, 0),
		votedFor:                -1,
		state:                   Follower,
		electionTimerch:         make(chan struct{}, 1),
		HeartbeatTimeoutTimerch: make(chan struct{}, 1),
		currentTerm:             0,
		votes:                   0,
		commitIndex:             0,
		lastApplied:             0,
		nextIndex:               make([]int, len(peers)),
		matchIndex:              make([]int, len(peers)),
		applyCh:                 applyCh,
		lastIncludedIndex:       0,
		lastIncludedTerm:        0,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.batchAppendEntries()

	//applyMsg
	go rf.loopApplyMsg()

	return rf
}
