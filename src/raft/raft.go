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
	"sync"
	"time"
	"math/rand"
	"fmt"
	"github.com/xuhaowang/mit6.824/src/labrpc"
)

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}


type State int

const (
	leader     State = 0
	follower   State = 1
	candidate  State = 2
)

const heartbeatsPeroid time.Duration = 100    //the heartbeats peroid is 100ms, 10 times per second

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	state     State               // this peer's state
	term      int
	lastTime  time.Time           // the last time at which the peer heard from the leader
	votedFor  int
	votesNum  int                 // The votes got by this candidate

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = (rf.state == leader)
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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


type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderID   int
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("I am %d, term: %d; request AppendEntries to %d, state: %d, term: %d\n", 
	           args.LeaderID, args.LeaderTerm, rf.me, rf.state, rf.term)
	rf.lastTime = time.Now()
	reply.CurrentTerm = rf.term
	if args.LeaderTerm < rf.term {
		reply.Success = false
	} else {
		reply.Success = true
		if rf.state != follower {
			rf.mu.Lock()
			rf.state = follower
			rf.mu.Unlock()
		} 		
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int
	CandidateId     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm    int
	VoteGranted    bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("I am %d, term: %d; RequestVote to %d, state: %d, term: %d,\n",  
	            args.CandidateId, args.Term, rf.me, rf.state, rf.term)
	reply.CurrentTerm = rf.term
	if rf.votedFor != -1 {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
	}
	if args.Term < rf.term {
		reply.VoteGranted = false
		return
	} else if args.Term == rf.term {
		return
	} else {
		rf.mu.Lock()
		rf.term = args.Term
		if rf.state == follower {
			rf.lastTime = time.Now()
		} else {
			rf.state = follower
		}
		rf.mu.Unlock()
		return
	}
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
	rf.state = follower                 //begin as a follower
	rf.term = 0
	rf.votedFor = -1

	go rf.runAsFollwer(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}


//When the rf instance is follower, run this function
func (rf *Raft) runAsFollwer(applyCh chan ApplyMsg) {
	fmt.Printf("I am %d, term: %d, run as follower\n", rf.me, rf.term)
	electionTimeout := time.Duration(200 + rand.Intn(150)) * time.Millisecond
	rf.lastTime = time.Now()
	//timer := time.NewTimer(time.Duration(electionTimeout) * time.Millisecond)
	for{
		time.Sleep(electionTimeout)
		if time.Since(rf.lastTime) >= electionTimeout {
			rf.term += 1
			rf.state = candidate
			rf.votedFor = -1
			break
		}
	}
	if rf.state == candidate {
		go rf.runAsCandidate(applyCh)
	}
}

func (rf *Raft) runAsCandidate(applyCh chan ApplyMsg) {
	fmt.Printf("I am %d, term: %d, run as candidate\n", rf.me, rf.term)
	loop1:
	for {
		rf.mu.Lock()
		rf.votedFor = rf.me
		rf.votesNum = 1
		rf.mu.Unlock()
		rf.sendRequestVoteToPeers()
		electionTimeout := time.Duration(200 + rand.Intn(150))
		timer := time.NewTimer(electionTimeout * time.Millisecond)
		loop2:
		for {
			select {
			case <- timer.C:
				if rf.state == candidate {
					rf.mu.Lock()
					rf.term += 1
					rf.mu.Unlock()
					break loop2
				} else {
					break loop1
				}
			default:
				if (rf.votesNum >= int(len(rf.peers) / 2) + 1) {
					rf.mu.Lock()
					rf.state = leader
					rf.mu.Unlock()
					break loop1
				}
				if rf.state != candidate {
					break loop1
				}
			}
		}
		
	}
	if rf.state == follower {
		go rf.runAsFollwer(applyCh)
	}
	if rf.state == leader {
		go rf.runAsLeader(applyCh)
	}
}

func (rf *Raft) sendRequestVoteToPeers(){
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		} else{
			go func (target int){
				args := &RequestVoteArgs{
					Term: rf.term,
					CandidateId: rf.me,
				}
				reply := &RequestVoteReply{}
				rf.peers[target].Call("Raft.RequestVote", args, reply)
				if reply.VoteGranted {
					rf.mu.Lock()
					rf.votesNum += 1
					rf.mu.Unlock()
				}
				if rf.term < reply.CurrentTerm {
					rf.mu.Lock()
					rf.term = reply.CurrentTerm
					rf.state = follower
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) runAsLeader(applyCh chan ApplyMsg){
	fmt.Printf("I am %d, term: %d, run as leader\n", rf.me, rf.term)
	ticker := time.NewTicker(heartbeatsPeroid * time.Millisecond)
	rf.sendAppendEntriesToPeers()
	loop:
	for{
		select {
		case <- ticker.C:
			rf.sendAppendEntriesToPeers()
		default:
		    if rf.state == follower{
			    break loop
		    }
		}
	}
	if rf.state == follower {
		go rf.runAsFollwer(applyCh)
	}
}


func (rf *Raft) sendAppendEntriesToPeers(){
	for i := 0; i < len(rf.peers); i++{
		if i == rf.me {
			continue
		} else {
			go func (target int)  {
				args := &AppendEntriesArgs{
					LeaderTerm: rf.term, 
					LeaderID: rf.me,
				}
				reply := &AppendEntriesReply {}
				rf.peers[target].Call("Raft.AppendEntries", args, reply)
				if rf.term < reply.CurrentTerm {
					rf.mu.Lock()
					rf.term = reply.CurrentTerm
					rf.state = follower
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}


