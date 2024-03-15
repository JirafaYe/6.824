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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	Follower  int = 0
	Leader    int = 1
	Candidate int = -1
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

	currentTerm  int
	votedFor     int //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	logs         []Log
	state        int
	entriesLogCh chan struct{}
	heartbeat    time.Duration
	rpcTimeOut   time.Duration
	votes        int

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
	//log entries to store,empty for heartbeat
	Entries map[int]Log
	//leader's commit idx
	// LeaderCommit int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type AppendEntries struct {
	Req   *AppendEntriesArgs
	Reply *AppendEntriesReply
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

}

func (rf *Raft) GetRfState() int {
	rf.mu.Lock()
	// //fmt.Println("::::LockElecState")
	defer rf.mu.Unlock()
	// //fmt.Println("::::UnLockElecState")

	return rf.state
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	DPrintf("VoteLock3:::::%d", rf.me)
	DPrintf("VoteRaft Term[%d] VotedFor[%d]", rf.currentTerm, rf.votedFor)

	var flag = false
	if args.Term > rf.currentTerm {
		// rf.currentTerm = args.Term
		// rf.state = Follower
		// rf.votedFor = -1
		rf.transState(Follower, args.Term)
	}
	// fmt.Printf("%#v:::rf\n", rf)
	// //fmt.Println("me:::::Votedfor:::term:::argsTerm::::Can", rf.me, rf.votedFor, rf.currentTerm, args.Term, args.CandidateId)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.Term == rf.currentTerm {
		flag = true
		rf.votedFor = args.CandidateId
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = flag
	rf.mu.Unlock()
	rf.resetTimer()

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
	return ok
}

var chMu sync.Mutex

// func (rf *Raft) preventRpcTimeout(f func(), reply *any, chRplys chan interface{}, chTimeout chan struct{}) {
// 	f()
// 	select {
// 	case <-chTimeout:
// 		DPrintf("rpc超时,chan已关闭,me::%d", i)
// 	default:
// 		//防止chan在close时被写入造成data race
// 		chMu.Lock()
// 		// DPrintf("rpc响应,me::%d", i)
// 		chRplys <- *reply
// 		chMu.Unlock()
// 	}
// }

func (rf *Raft) SendRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// rplys := []*RequestVoteReply{}
	chRplys := make(chan interface{}, len(rf.peers)-1)
	chTimeout := make(chan struct{}, len(rf.peers))
	// ctx, cancel := context.WithCancel(context.Background())

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
		DPrintf("GetVoteRply:::me::%d::term::%d::rply:%#v", rf.me, rf.currentTerm, rply)
		if rply.Term == rf.currentTerm && rply.VoteGranted {
			rf.votes++
		} else if rply.Term > rf.currentTerm {
			// rf.state = Follower
			// rf.currentTerm--
			// rf.votedFor = -1
			rf.transState(Follower, rply.Term)
		}
		rf.mu.Unlock()
		DPrintf("VoteUnLock1:::::%d::votes::%d", rf.me, rf.votes)
		// //fmt.Println("::::UnLockVote1")
	}

	rf.mu.Lock()
	DPrintf("VoteLock2:::::%d::votes::%d::state::%d", rf.me, rf.votes, rf.state)
	// //fmt.Println("sendALL::::Candidate::Folower", rf.me)
	if rf.votes > len(rf.peers)/2 && rf.state == Candidate {
		rf.transState(Leader, rf.currentTerm)
		rf.mu.Unlock()
		DPrintf("VoteUnLock2:::::%d", rf.me)

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			Entries:  nil,
		}
		DPrintf("SenHeartBeat.Leader:%d", rf.me)
		for idx, _ := range rf.peers {
			if idx != rf.me {
				i := idx
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}
	} else {
		// rf.state = Follower
		// rf.currentTerm--
		// rf.votedFor = -1
		rf.transState(Follower, rf.currentTerm-1)
		rf.mu.Unlock()
		DPrintf("VoteUnLock2:::::%d", rf.me)
	}
}

func (rf *Raft) quitRpcTimeout(chReplys chan interface{}, chTimeout chan struct{}, mu *sync.Mutex) {
	time.Sleep(rf.rpcTimeOut)
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
	// index := len(rf.logs)

	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, -1, isLeader
	}

	rf.mu.Lock()
	appendEntrie := Log{
		Command: command,
		Term:    term,
	}
	rf.logs = append(rf.logs, appendEntrie)
	rf.mu.Unlock()

	rf.sendAppendEntriesAll(len(rf.logs))

	return len(rf.logs), term, isLeader
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
		// time.Sleep().
		// //fmt.Println("ticker")
		state := rf.GetRfState()
		if state == Leader {
			//fmt.Println("isLeader", rf.me, ":::::::::")

			time.Sleep(rf.heartbeat)
			rf.mu.Lock()
			// //fmt.Println("::::LockTicker")
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			// //fmt.Println("::::UnLockTicker")
			reply := &AppendEntriesReply{}
			for idx, _ := range rf.peers {
				if idx != rf.me {
					rf.sendAppendEntries(idx, args, reply)
				}
			}
		} else {
			// //fmt.Println("Follower")

			//随机定时器，时间范围为400-600ms，心跳间隔为200ms
			rand.Seed(time.Now().UnixNano())
			randomInt := rand.Intn(300) + 250
			DPrintf("election timer :: %d,server:%d", randomInt, rf.me)
			time.Sleep(time.Duration(randomInt) * time.Millisecond)

			select {
			case <-rf.entriesLogCh:
				//fmt.Println("resetTimer::::", rf.me)
				// rf.mu.Lock()
				// rf.votedFor = -1
				// rf.mu.Unlock()
			default:
				//fmt.Println("startElection::", rf.me)
				rf.startElection()
			}

		}

	}
}

func (rf *Raft) resetTimer() {
	if len(rf.entriesLogCh) == 0 {
		rf.entriesLogCh <- struct{}{}
	}
}

func (rf *Raft) transState(state int, term int) {
	DPrintf("TransTo[%d] me[%d]", state, rf.me)
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	switch state {
	case Follower:
		rf.state = Follower
		// rf.votedFor = -1
		rf.currentTerm = term
	case Candidate:
		rf.state = Candidate
		rf.currentTerm = term
		rf.votedFor = rf.me
		rf.votes = 1
	case Leader:
		rf.state = Leader
	}
}

var appendMu sync.Mutex

func (rf *Raft) sendAppendEntriesAll(index int) {
	if rf.state != Leader {
		return
	}

	entries := make(map[int]Log)
	DPrintf("index:::::[%d]", index)
	if index == 0 {
		entries = nil
	} else {
		entries[index-1] = rf.logs[index-1]
	}

	// entries[index] = rf.logs[index]

	var prevLogIndex = index - 1

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	if prevLogIndex > 0 {
		args.PrevLogTerm = rf.logs[prevLogIndex-1].Term
	}

	chRplys := make(chan interface{}, len(rf.peers)-1)
	chTimeout := make(chan struct{}, len(rf.peers))
	go rf.quitRpcTimeout(chRplys, chTimeout, &appendMu)

	//todo:接收返回参数
	for idx, _ := range rf.peers {
		if idx != rf.me {
			i := idx
			reply := &AppendEntriesReply{}
			go func() {
				rf.sendAppendEntries(i, args, reply)
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

	//阻塞等待rpc超时
	DPrintf("TimeoutBefore:%d", rf.me)
	<-chTimeout
	DPrintf("Timeoutaft:%d,len::%d", rf.me, len(chTimeout))

	var cnt = 0

	for reply := range chRplys {
		rf.mu.Lock()
		rply := reply.(AppendEntriesReply)
		if rply.Term == rf.currentTerm && rply.Success {
			cnt++
		}
		rf.mu.Unlock()
	}

	//commit log
	DPrintf("commit numbers [%d]", cnt)
	rf.mu.Lock()
	if cnt > len(rf.peers)/2 {
		rf.commitIndex = index
	}
	rf.mu.Unlock()
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Entries == nil {
		DPrintf("HearbeatLock::%d::args::%#v", rf.me, args)
		if args.Term >= rf.currentTerm {
			rf.transState(Follower, args.Term)
		}
		reply.Term = rf.currentTerm
		// rf.commitIndex = args.LeaderCommit
		rf.mu.Unlock()
		DPrintf("HearbeatunLock::%d::logs[%#v]", rf.me, rf.logs)
		rf.resetTimer()
	} else {
		DPrintf("LogAppendLock::%d", rf.me)
		if args.Term < rf.currentTerm || !rf.checkEntries(args) {
			reply.Success = false
		} else {
			reply.Success = true
			rf.logs = append(rf.logs, args.Entries[args.PrevLogIndex])
			DPrintf("Append Logs[%#v]", rf.logs)
		}
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}
	if args.LeaderCommit > rf.commitIndex {
		//set commitIndex=min(LeaderCommit,index of last new entry)
		rf.commitIndex = func(x int, y int) int {
			if x > y {
				return y
			} else {
				return x
			}
		}(args.LeaderCommit, len(rf.logs))
	}
}

func (rf *Raft) checkEntries(args *AppendEntriesArgs) bool {
	DPrintf("Check Entries::::me::%d::args%#v", rf.me, args)

	ret := true

	// if args.PrevLogIndex >= len(rf.logs) {
	// 	ret = false
	// } else
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.logs) && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		ret = false
	} else if len(rf.logs) > args.PrevLogIndex+1 {
		index := args.PrevLogIndex + 1
		log := rf.logs[index]
		if log.Term != args.Term {
			rf.logs = rf.logs[0:index]
		}
	}
	return ret
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("::lock::StartElection%d", rf.me)

	// rf.currentTerm++
	// rf.votedFor = rf.me
	// rf.state = Candidate
	// rf.votes = 1
	rf.transState(Candidate, rf.currentTerm+1)

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	reply := &RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
	rf.mu.Unlock()
	DPrintf("::Unlock::StartElection%d", rf.me)

	DPrintf("VoteArgs::%#v", args)
	rf.SendRequestVote(args, reply)

}

func (rf *Raft) loopApplyMsg(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		var appliedMsgs = make([]ApplyMsg, 0)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
					// CommandTerm:  rf.logs[rf.lastApplied-1].Term,
				})
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}()
		// 锁外提交给应用层
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
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
		dead:         0,
		logs:         make([]Log, 0),
		votedFor:     -1,
		state:        Follower,
		entriesLogCh: make(chan struct{}, 1),
		heartbeat:    100 * time.Millisecond,
		rpcTimeOut:   180 * time.Millisecond,
		currentTerm:  0,
		votes:        0,
		commitIndex:  0,
		lastApplied:  0,
		nextIndex:    make([]int, len(peers)),
		matchIndex:   make([]int, len(peers)),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//applyMsg
	go rf.loopApplyMsg(applyCh)

	return rf
}
