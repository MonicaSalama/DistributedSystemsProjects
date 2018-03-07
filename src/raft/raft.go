package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

import "bytes"
import "encoding/gob"



//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm		int
	VotedFor 			int
	isLeader 			bool
	isCandidate   bool
	lastRecv      time.Time
	timeOut				int64
	isFunctional	bool
	votes		int

	//////
	Log			[]EntrieArgs
	lastApplied		int
	commitIndex		int
  muCommitIndex        sync.Mutex
	applyCh chan ApplyMsg
	/// if leader
	nextIndex		[]int
	matchIndex	[]int

}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.CurrentTerm
	isleader := rf.isLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type EntrieArgs struct {
	Term		int
	Command	interface{}
}

type AppendEntriesArgs struct {
	Term					int
	LeaderId			int
	PrevLogIndex	int
	PrevLogTerm   int
	Entries 			[]EntrieArgs
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	Index   int
}

/* returns last log index*/
func (rf *Raft) lastLogIndex() int{
	return len(rf.Log)-1
}

/* to grant vote must check if the candidate has at least upToDate log*/
func(rf *Raft) checkUpToDateLog(candidateLastLogIndex int, candidateLastLogTerm int) bool{
	upToDate := false
	if (rf.Log[rf.lastLogIndex()].Term < candidateLastLogTerm) {
		upToDate = true
	} else if (rf.Log[rf.lastLogIndex()].Term == candidateLastLogTerm && rf.lastLogIndex() <= candidateLastLogIndex) {
		upToDate = true
	}
	return upToDate
}

/* checks if it can grant the vote to candidare */
func (rf *Raft) checkVoteValidity(args *RequestVoteArgs) bool {
	/* checks granting vote to only 1 candidate during a term */
	valid := (rf.CurrentTerm < args.Term || (rf.CurrentTerm == args.Term && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId)))
	return valid && rf.checkUpToDateLog(args.LastLogIndex, args.LastLogTerm)
}

//
//  RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// grant vote if valid candidate
	if rf.checkVoteValidity(args) {
		rf.isLeader = false
		rf.VotedFor = args.CandidateId
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.CurrentTerm = args.Term
		rf.lastRecv = time.Now()
	}else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
	// if it has a stale term --> update
	if (args.Term > rf.CurrentTerm) {
		rf.LeaveLeaderShip(args.Term)
	}
	// term might have changed --> save changes
	rf.persist()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// if leader or candidate return to follower
func (rf *Raft) LeaveLeaderShip(newTerm int) {
	rf.isCandidate = false
	rf.isLeader = false
	rf.CurrentTerm = newTerm
}
// receiving heart beat
func (rf *Raft) ReceiveHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
		rf.LeaveLeaderShip(args.Term)
		reply.Success = true
		rf.lastRecv = time.Now()
		// leader includes its commitIndex in order for the follower to commit the corresponding entries
		rf.setCommitIndex(args.LeaderCommit)
}
// update commit index --> commit through channel
func (rf *Raft) setCommitIndex(newVal int) {
	if (rf.commitIndex >= newVal) {
		return
	}
	rf.commitIndex = newVal
	// commit entries with index i < commitIndex
	go rf.Commit()
}

/* follower sets its commitIndex with minimum of leader commitIndex and the index of the last entrie of its log */
func (rf *Raft) CommitIndex(args *AppendEntriesArgs) {
	if (rf.commitIndex < args.LeaderCommit) {
		if (rf.lastLogIndex() < args.LeaderCommit) {
			rf.setCommitIndex(rf.lastLogIndex())
		} else {
			rf.setCommitIndex(args.LeaderCommit)
		}
	}
}

/* if term larger than current term then update */
func (rf *Raft) updateTerm(newTerm int) {
	if (rf.CurrentTerm < newTerm) {
		rf.CurrentTerm = newTerm
	}
}

/* adding entries to follower's log */
func (rf *Raft) addToLog(entries []EntrieArgs, j int) {
	for i:= 0 ; i < len(entries) ;i++ {
		if j > rf.lastLogIndex() {
			rf.Log = append(rf.Log,entries[i:]...)
			break
		} else if (rf.Log[j].Term != entries[i].Term) {
			rf.Log = append(rf.Log[:j],entries[i:]...)
			break
		}
		j = j + 1
	}

}
/* follower receiving entries from leader or stale leader */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if (args.Term < rf.CurrentTerm) {
		// stale leader --> then reply false
		reply.Success = false
		return
	}
	if (args.Entries == nil) {
		// receiving heartbeat from leader
		rf.ReceiveHeartBeat(args, reply)
	} else {
		// receiving entries
		rf.lastRecv = time.Now()
		rf.updateTerm(args.Term)
		// check consistency
		if rf.lastLogIndex() < args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// not consistent
			reply.Success = false
			reply.Index = rf.commitIndex
		} else {
			// consistent add to log and update commitIndex
			reply.Success = true
			rf.addToLog(args.Entries, args.PrevLogIndex+1)
			rf.CommitIndex(args)
		}
	}
	rf.persist()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.CurrentTerm
	index := rf.lastLogIndex() + 1
	isLeader := rf.isLeader
  if (isLeader) {
		// add entrie to log
		entrie := EntrieArgs{Term: term, Command: command}
		rf.Log = append(rf.Log,entrie)
		// save current state
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isFunctional = false
	rf.isLeader = false
}
/* to generate random timeout */
func random() int64 {
		var min int64
		var max int64
		min = 500
		max = 700
	  return rand.Int63n(max - min) + min
}
/* leader mechanism to update its commit index */
func (rf *Raft) updateCommitIndex(term int) {
	for true {
		rf.mu.Lock()
		if (!rf.isLeader || rf.CurrentTerm != term || !rf.isFunctional) {
			// no more leader return
			rf.mu.Unlock()
			return
		}
		lastIndex := rf.lastLogIndex()
		if (rf.commitIndex == lastIndex || rf.Log[lastIndex].Term != rf.CurrentTerm) {
			// already upToDate commitIndex
			rf.mu.Unlock()
			continue
		}
		// calculate new commitIndex
		candidate := rf.commitIndex+1
		for candidate < lastIndex {
			if (rf.Log[candidate].Term == rf.CurrentTerm) {
				break
			}
			candidate = candidate + 1
		}
		count := 0
		for i:= 0 ; i < len(rf.matchIndex) ; i++ {
			if (rf.matchIndex[i] >= candidate) {
				count++
			}
		}
		if (count >= len(rf.matchIndex)/2) {
			rf.setCommitIndex(candidate)
		}
		rf.mu.Unlock()
	}
}
/* leader tries sending log entries to follower*/
func (rf *Raft) sendToFollower(server int, args *AppendEntriesArgs, newMatch int ) {
		term := args.Term
		preVal := args.PrevLogIndex+1
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, args, reply)
    if (ok) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if (reply.Term > rf.CurrentTerm) {
				// stale and must retrun a follower
				rf.LeaveLeaderShip(reply.Term)
				rf.persist()
				rf.lastRecv = time.Now()
				return
			}
			if (term != rf.CurrentTerm) {
				// new term started this thread is no more valid
				return
			}
			if (reply.Success) {
				// success then update nextIndex to be sent
				if (rf.nextIndex[server] < (newMatch + 1)) {
					rf.nextIndex[server] = newMatch + 1
					rf.matchIndex[server] = newMatch
				}
			} else if (rf.nextIndex[server] == preVal) {
				// follower had inconsistent log with the leader --> try again
				// rf.nextIndex[server] = rf.nextIndex[server] - 1
				rf.nextIndex[server] = reply.Index + 1
			}
		}
}
func (rf *Raft) sendEntries(term int) {
	for true {
		rf.mu.Lock()
		// it's non functional or not a leader anymore return
		if (!rf.isLeader || rf.CurrentTerm != term || !rf.isFunctional) {
			rf.mu.Unlock()
			return
		}
		index := rf.lastLogIndex()
		for i:= 0 ; i < len(rf.peers) ; i++ {
			if (i == rf.me) {
				continue
			}
			if (index >= rf.nextIndex[i]) {
				//send entries to followers
				args := &AppendEntriesArgs{}
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
				args.Entries = rf.Log[rf.nextIndex[i]:]
				args.LeaderCommit = rf.commitIndex
				go rf.sendToFollower(i, args, index)
			}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

/* responsible on constantly sending heartBeats to followers every 150 ms*/
func (rf *Raft) sendHeartBeats(term int) {
	for true {
		rf.mu.Lock()
		if (!rf.isFunctional || rf.CurrentTerm != term || !rf.isLeader) {
			rf.mu.Unlock()
			return
		}
		for i:= 0 ; i < len(rf.peers) ; i++ {
			if (i == rf.me) {
				continue
			}
			// send to follower as the minimum of the commitIndex
			// or the last entry known to be consistent btween leader and follower i
			var commitIndex int
			if (rf.matchIndex[i] <= rf.commitIndex) {
				commitIndex = rf.matchIndex[i]
			} else {
				commitIndex = rf.commitIndex
			}
			// thread to send the heartBeat to follower i
			go func (i int, commitIndex int, Term int, leaderId int) {
					args := &AppendEntriesArgs{}
					args.Term = Term
					args.LeaderId = leaderId
					args.LeaderCommit = commitIndex
					reply := &AppendEntriesReply{}
			 		ok := rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if (ok && !reply.Success) {
						// the only case the heartBeat fails if a newer term started so leader should return to be a follower
						rf.LeaveLeaderShip(reply.Term)
						rf.persist()
						rf.lastRecv = time.Now()
					}
			}(i, commitIndex, term, rf.me)
		}
		rf.mu.Unlock()
    time.Sleep(100 * time.Millisecond)
	}
}

/* helper function to fill array with certain value */
func fill(arr []int, value int) {
	for i:= 0 ;  i < len(arr) ; i++ {
		arr[i] = value
	}
}

/* leader starts its job*/
 func (rf *Raft) leader() {
	rf.isCandidate = false
	rf.isLeader = true
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	fill(rf.nextIndex, rf.lastLogIndex()+1)
	fill(rf.matchIndex, 0)
	// start goroutine responsible of sending heartBeats
	go rf.sendHeartBeats(rf.CurrentTerm)
	// goroutine responsible of sending log entries to followers
	go rf.sendEntries(rf.CurrentTerm)
	// goroutine responsible of updating leader's commitIndex
	// whenever an entrie is appended to at least 1/2 of followers logs
	go rf.updateCommitIndex(rf.CurrentTerm)
}

// candidate sends vote request to rest of the peers
func (rf *Raft) requestCandindancy() {
	electionTerm := rf.CurrentTerm
	args := &RequestVoteArgs{}
	args.Term = electionTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastLogIndex()
  args.LastLogTerm = rf.Log[rf.lastLogIndex()].Term
		for i := 0; i < len(rf.peers); i++ {
			if (i == rf.me) {
				continue
			}
			if (!rf.isCandidate) {
				break
			}
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if (rf.isCandidate && ok && electionTerm == rf.CurrentTerm && reply.VoteGranted) {
					// valid vote
					rf.votes = rf.votes + 1
					if (rf.votes >= len(rf.peers)/2) {
						if (!rf.isLeader) {
							rf.leader()
						}
					}
				} else if (reply.Term > rf.CurrentTerm) {
					rf.CurrentTerm = reply.Term
					rf.isCandidate = false
					rf.isLeader = false
					rf.votes = 0
					rf.persist()
				}

			}(i)
		}
}

/* goroutine for checking of timeout to make sure the follower can hear the leader*/
func (rf *Raft) checkForTimeOut() {
	True := true
	for True {
		rf.mu.Lock()
		if !rf.isFunctional {
			rf.mu.Unlock()
			return
		}
		// if not leader and didn't hear from a leader
		// for an interval larger then the timout --> start election
		if !rf.isLeader && time.Since(rf.lastRecv).Nanoseconds()/1e6 >= rf.timeOut {
			 rf.CurrentTerm = rf.CurrentTerm + 1
			 rf.isCandidate = true
			 rf.VotedFor = rf.me
			 rf.timeOut = random()
			 rf.votes = 0
			 rf.lastRecv = time.Now()
			 rf.persist()
			 rf.requestCandindancy()
		 }
		rf.mu.Unlock()
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.isLeader = false
	rf.isCandidate = false
	rf.lastRecv = time.Now()
	rf.timeOut = random()
	rf.isFunctional = true
	rf.votes = 0
  rf.lastApplied = 0
	rf.commitIndex = 0
  //add dummy entrie
	entrieZero := EntrieArgs{}
	entrieZero.Term = 0
	rf.Log = append(rf.Log,entrieZero)
	rf.applyCh = applyCh
	go rf.checkForTimeOut()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

/* goroutine for commiting entries */
func (rf *Raft) Commit() {
	rf.muCommitIndex.Lock()
		for (rf.commitIndex > rf.lastApplied) {
			rf.lastApplied = rf.lastApplied + 1
			msg := ApplyMsg{Index: rf.lastApplied, Command: rf.Log[rf.lastApplied].Command}
			rf.applyCh <- msg
		}
	rf.muCommitIndex.Unlock()
}
