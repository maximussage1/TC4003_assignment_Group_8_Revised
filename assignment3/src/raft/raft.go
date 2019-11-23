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

import "sync"
import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
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
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	applyCh chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	//Updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int
	logs        []map[string]interface{}

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	//Reinitialized after election
	nextIndex  []int
	matchIndex []int

	//EXTRAS.
	serverState      int //0-follower, 1-candidate, 2-leader
	electionTimeout  time.Duration
	heartBeatTimeout time.Duration
	heartBeatTicker  *time.Ticker
	lastHeart        time.Time
	running          bool
	counter          int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term = rf.currentTerm
	var isLeader = rf.serverState == 2
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last long entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool //True means candidate received vote
}

/* AppendEntries section
Invoked by leader to replicate log entries; also used as heartbeat
*/
type AppendEntriesArgs struct {
	Term         int                      //leader's term
	LeaderId     int                      // so follower can redirect clients
	PrevLogIndex int                      // index of log entry immediately proceeding new ones
	PrevLogTerm  int                      // term of prevLogIndex entry
	Entries      []map[string]interface{} //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int                      //leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	Len     int  // EXTRA field used to save the
}

//
// example RequestVote RPC handler.
// Invoked by candidates to gather votes
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.serverState = 0
			rf.votedFor = -1
			rf.currentTerm = args.Term
		}
		lastLogTerm := 0
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1]["term"].(int)
		}
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && len(rf.logs) <= args.LastLogIndex)) {
			reply.VoteGranted = true
			rf.serverState = 0
			rf.votedFor = args.CandidateId
			rf.lastHeart = time.Now()
		} else {
			reply.VoteGranted = false
		}
	}
	rf.persist()
	reply.Term = rf.currentTerm
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.serverState = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
		}
	}
	return ok && reply.VoteGranted
}

//Invoked by leader to replicate log entries, also used as heartbeat
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false // the "leader" is way behind, this can't succeed.
	} else {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.serverState = 0
		rf.votedFor = args.LeaderId
		if args.PrevLogIndex > 0 {
			if len(rf.logs) >= args.PrevLogIndex && rf.logs[args.PrevLogIndex-1]["term"] == args.PrevLogTerm {
				reply.Success = true
			} else {
				reply.Success = false
				reply.Len = len(rf.logs) //there were some logs missing, this server sends the logs to the leader.
				rf.lastHeart = time.Now()
			}
		}
	}
	reply.Term = rf.currentTerm
	if reply.Success {
		rf.logs = rf.logs[0:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
		for iter := rf.commitIndex; iter < args.LeaderCommit; iter++ {
			command := rf.logs[iter]["command"]
			rf.applyCh <- ApplyMsg{Index: iter + 1, Command: command, UseSnapshot: false}
		}
		rf.commitIndex = args.LeaderCommit
		rf.lastHeart = time.Now()
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.serverState = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
		}
		if ok && rf.serverState == 2 && !reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex
			if reply.Len < args.PrevLogIndex {
				rf.nextIndex[server] = reply.Len + 1
			}
			rf.nextIndex[server] = 1
		}
	}
	return ok && reply.Success
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
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
	index := len(rf.logs) + 1
	if rf.serverState == 2 {
		log := map[string]interface{}{"command": command, "term": rf.currentTerm}
		rf.logs = append(rf.logs, log)
		rf.persist()
	} else {
	}
	return index, rf.currentTerm, rf.serverState == 2
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	/* stop the world */
	rf.running = false
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
	//Starting server
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here.
	rf.init(applyCh, persister)
	go rf.mainServerLoop()
	return rf
}

//This is the infinite loop where all the magic happens, if there is a timeout, any given server can start
// an election. This should run in a goroutine since it sleeps.
func (rf *Raft) mainServerLoop() {
	for {
		if !rf.running {
			break
		}
		time.Sleep(rf.electionTimeout - time.Since(rf.lastHeart))

		if rf.serverState != 2 && time.Since(rf.lastHeart) >= rf.electionTimeout {
			rf.mu.Lock()
			// re-generate time
			rf.electionTimeout = time.Duration(rand.Intn(100)+100) * time.Millisecond
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.serverState = 1
			rf.persist()
			rf.mu.Unlock()
			rf.startElection()
		}
		/* sleep at most electionTimeout duration */
		if time.Since(rf.lastHeart) >= rf.electionTimeout {
			rf.lastHeart = time.Now()
		}
	}
}

func (rf *Raft) init(applyCh chan ApplyMsg, persister *Persister) {
	rf.applyCh = applyCh
	rf.running = true
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.electionTimeout = time.Duration(rand.Intn(100)+100) * time.Millisecond
	rf.heartBeatTimeout = time.Duration(rand.Intn(50)+50) * time.Millisecond
	rf.counter = 0
	rf.lastHeart = time.Now()
	rf.heartBeatTicker = time.NewTicker(rf.heartBeatTimeout)
	rf.serverState = 0 //start in follower state
	rf.commitIndex = 0
	rf.lastApplied = 0
}

//This is the main process where the election will happen.
// The server that calls this function will start contacting its peers in a goroutine and nominate itself
//as the new leader.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	var agreed int64 = 1
	defer rf.mu.Unlock()
	index := len(rf.logs)
	term := 0
	if index != 0 {
		term = rf.logs[index-1]["term"].(int)
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int, currTerm int, index int, term int) {
				args := RequestVoteArgs{Term: currTerm, CandidateId: rf.me, LastLogIndex: index, LastLogTerm: term}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, args, &reply)
				rf.mu.Lock()
				if ok && args.Term == rf.currentTerm && rf.serverState == 1 {
					atomic.AddInt64(&agreed, 1)
					if int(agreed)*2 > len(rf.peers) {
						rf.serverState = 2
						rf.nextIndex = rf.nextIndex[0:0]
						rf.matchIndex = rf.matchIndex[0:0]
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex = append(rf.nextIndex, len(rf.logs)+1)
							rf.matchIndex = append(rf.matchIndex, 0)
						}
						/* persist state */
						rf.persist()
						go rf.doSubmit() //this server has become the leader
					}
				}
				rf.mu.Unlock()
			}(i, rf.currentTerm, index, term)
		}
	}
}

func (rf *Raft) doSubmit() {
	/* ensure only one thread is running */
	if atomic.AddInt64(&rf.counter, 1) > 1 {
		atomic.AddInt64(&rf.counter, -1)
		return
	}
	for range rf.heartBeatTicker.C {
		if !rf.running {
			break
		}
		if rf.serverState != 2 {
			break
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(peer int) {
					rf.mu.Lock()
					index := rf.nextIndex[peer]
					term := 0
					entries := make([]map[string]interface{}, 0)
					if len(rf.logs) >= index {
						entries = append(entries, rf.logs[index-1:]...)
					}
					if index > 1 {
						term = rf.logs[index-2]["term"].(int)
					}
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: index - 1, PrevLogTerm: term, Entries: entries, LeaderCommit: rf.commitIndex}
					reply := AppendEntriesReply{}
					rf.nextIndex[peer] = args.PrevLogIndex + len(entries) + 1
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(peer, args, &reply)
					rf.mu.Lock()
					if ok && args.Term == rf.currentTerm && rf.serverState == 2 {
						rf.matchIndex[peer] = args.PrevLogIndex + len(entries)
						/* update commitIndex */
						for iter := rf.commitIndex; iter < len(rf.logs); iter++ {
							if rf.logs[iter]["term"].(int) < rf.currentTerm {
								continue
							}
							count := 1
							for j := 0; j < len(rf.peers); j++ {
								if j != rf.me {
									if rf.matchIndex[j] > iter {
										count++
									}
								}
							}

							if count*2 > len(rf.peers) {
								//When a majority of peers have accepted the data, it becomes commited
								for i := rf.commitIndex; i <= iter; i++ {
									rf.commitIndex = i + 1
									command := rf.logs[i]["command"]
									rf.applyCh <- ApplyMsg{Index: i + 1, Command: command, UseSnapshot: false}
								}
							} else { // commit in order
								break
							}
						}
					}
					rf.mu.Unlock()
				}(i)
			}
		}
	}
	atomic.AddInt64(&rf.counter, -1)
}
