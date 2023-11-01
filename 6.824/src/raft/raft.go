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
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/labrpc"

// import "bytes"
// import "../labgob"

// Timing

const (
	HeartBeatTime = time.Millisecond * 100
	TimeMin       = 4 * HeartBeatTime
	TimeMax       = 8 * HeartBeatTime
)

// state of beat

const (
	Beat = iota
	Append
)

// state of back

const (
	LowerTerm = iota
	ShortLog
	MismatchTerm
)

// Raft's three states
const (
	Leader = iota
	Follower
	Candidate
)

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

type entry struct {
	Term    int
	Command interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // index of candidate’s last log entry (§5.4)
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type RequestVoteReply struct {
	Server      int
	Term        int  // index of candidate’s last log entry (§5.4)
	VoteGranted bool // true means candidate received vote
	// Your data here (2A).
}

//
// use to heartbeat
//

type AppendArgs struct {
	PeerIndex    int
	LeaderTerm   int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
	BeatState    int
}

type AppendReply struct {
	Term          int
	ConflictIndex int
	ConflictTerm  int
	Accept        bool
	Back          int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

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

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// 非易失性
	currentTerm int // 当前服务器可见任期
	//candidateId int     // 当前任期的candidateId
	votedFor   int     // 当前任期投票给的candidateId，如果没有为-1
	logEntries []entry // first index is 1
	// 易失性
	applyCh     chan ApplyMsg
	state       int32 // state of Raft, is it persistent???
	commitIndex int   // 最大的被提交的logEntries的下标
	lastApplied int   // 最大的被应用到状态机的logEntries的下标
	beatChan    chan struct{}
	voteChan    chan struct{}
	// 选举后重新初始化
	nextIndex  []int // 对于每个服务器，发送到该服务器下一个logEntry的索引
	matchIndex []int // 对于每个服务器，服务器上已知要复制的最高logEntry的索引
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

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

func (rf *Raft) RandTime(min, max time.Duration) time.Duration {
	delta := max - min
	ret := min + time.Duration(rand.Int63n(int64(delta)))
	// log.Printf("randtime is %v", ret)
	return ret
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = atomic.LoadInt32(&rf.state) == Leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isLeader
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.lastApplied) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logEntries) != nil {
		log.Fatalf("fail to encode!\n")
	}
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
	d := gob.NewDecoder(r)
	var (
		DecodeTerm    int
		DecodeIndex   int
		DecodeVote    int
		DecodeEntries []entry
	)
	if d.Decode(&DecodeTerm) != nil || d.Decode(&DecodeIndex) != nil || d.Decode(&DecodeVote) != nil || d.Decode(&DecodeEntries) != nil {
		log.Fatalf("fail to decode!")
	} else {
		rf.currentTerm = DecodeTerm
		rf.lastApplied = DecodeIndex
		rf.votedFor = DecodeVote
		rf.logEntries = DecodeEntries
	}
}

func (rf *Raft) ResponseVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Server = rf.me
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	} else if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			if atomic.LoadInt32(&rf.state) != Follower {
				rf.beatChan <- struct{}{}
			}
		}
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastIndex := len(rf.logEntries) - 1
			lastTerm := rf.logEntries[lastIndex].Term
			if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
				return
			} else { // voted for
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.voteChan <- struct{}{}
			}
		}
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) RequestVote(server int, ctx context.Context, replyCh chan<- RequestVoteReply) {
	select {
	case <-ctx.Done():
		return
	default:
		args := RequestVoteArgs{}
		reply := RequestVoteReply{}
		rf.mu.Lock()
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		args.LastLogIndex = len(rf.logEntries) - 1
		args.LastLogTerm = rf.logEntries[args.LastLogIndex].Term
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.ResponseVote", &args, &reply)
		if ok {
			if ctx.Err() == nil {
				replyCh <- reply
			}
			return
		} else {
			//log.Printf("requestvote: raft %d find raft %d is disconnect!\n", rf.me, server)
			//go rf.RequestVote(server, ctx, replyCh)
		}
	}
}

func (rf *Raft) collectVotes(server int, ctx context.Context, replyCh <-chan RequestVoteReply) {
	votes := 1
	for atomic.LoadInt32(&rf.state) == Candidate && rf.killed() == false {
		select {
		case <-ctx.Done():
			go rf.startElection()
			return
		case reply := <-replyCh:
			if reply.VoteGranted {
				votes++
				log.Printf("raft %d gets a vote from raft %d!\n", server, reply.Server)
				if votes >= len(rf.peers)/2+1 {
					log.Printf("raft %d is uponElection!\n", server)
					go rf.leaderSelect()
					return
				}
			} else {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					log.Printf("collect: raft %d has term %d is lower than raft %d has term %d", rf.me, rf.currentTerm, reply.Server, reply.Term)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
					go rf.followerSelect()
					return
				}
				rf.mu.Unlock()
			}
		case <-rf.beatChan:
			go rf.followerSelect()
			return
		case <-rf.voteChan:
			go rf.followerSelect()
			return
		}
	}
}

func (rf *Raft) startElection() {
	{
		rf.mu.Lock()
		atomic.StoreInt32(&rf.state, Candidate)
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		// log.Printf("raft %d start election!\n", rf.me)
		rf.mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), rf.RandTime(TimeMin, TimeMax))
	defer cancel()
	replyCh := make(chan RequestVoteReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.RequestVote(i, ctx, replyCh)
		}
	}
	rf.collectVotes(rf.me, ctx, replyCh)
}

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
	// Your code here (2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		isLeader = false
		return index, term, isLeader
	}
	if atomic.LoadInt32(&rf.state) != Leader {
		isLeader = false
	} else {
		// The leader
		// appends the command to its log as a new entry, then is-
		// sues AppendEntries RPCs in parallel to each of the other
		// servers to replicate the entry.
		term = rf.currentTerm
		Entry := entry{Term: rf.currentTerm, Command: command}
		log.Printf("now leader is %d,term is %d command is %v\n", rf.me, rf.currentTerm, command)
		rf.logEntries = append(rf.logEntries, Entry)
		index = len(rf.logEntries) - 1
		// go rf.beatAll(Append)
	}
	rf.persist()
	return index, term, isLeader
}

//func (rf *Raft) convertState(state int32) {
//	// Timer := time.NewTimer(rf.electionTimeOut)
//	rf.mu.Lock()
//	rf.state = state
//	rf.mu.Unlock()
//}

func (rf *Raft) followerSelect() {
	// rf.convertState(Follower)
	atomic.StoreInt32(&rf.state, Follower)
	ret := rf.RandTime(TimeMin, TimeMax)
	timer := time.NewTimer(ret)
	defer timer.Stop()
	for rf.killed() == false && atomic.LoadInt32(&rf.state) == Follower {
		select {
		case <-rf.beatChan:
			timer.Stop()
			timer.Reset(rf.RandTime(TimeMin, TimeMax))
		case <-rf.voteChan:
			timer.Stop()
			timer.Reset(rf.RandTime(TimeMin, TimeMax))
		case <-timer.C:
			go rf.startElection()
			return
		}
	}
}

// get beat or entries from leader

func (rf *Raft) ReceiveEntries(args *AppendArgs, reply *AppendReply) {
	// if the raft accepts this beat
	// that means the term is ok and put it on chan
	// else that means the raft refuses
	reply.Accept = true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// log.Printf("raft %d receive entries from raft %d\n", rf.me, args.PeerIndex)
	reply.Term = rf.currentTerm
	// Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.LeaderTerm {
		reply.Accept = false
		reply.Back = LowerTerm
		log.Printf("LowerTerm: leader raft %d has term %d is lower than follower raft %d has term %d!\n",
			args.PeerIndex, args.LeaderTerm, rf.me, rf.currentTerm)
		return
	}
	rf.beatChan <- struct{}{}
	if rf.currentTerm < args.LeaderTerm {
		log.Printf("receive: raft %d has term %d is lower than raft %d has term %d", rf.me, rf.currentTerm, args.PeerIndex, args.LeaderTerm)
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.logEntries) {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.logEntries)
		reply.Accept = false
		reply.Back = ShortLog
		log.Printf("raft %d is Shorter than raft %d!\n", rf.me, args.PeerIndex)
		return
	}
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logEntries[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		l := 1
		r := index
		for l <= r {
			mid := (l + r) / 2
			if rf.logEntries[mid].Term != reply.ConflictTerm {
				l = mid + 1
			} else {
				r = mid - 1
				index = mid
			}
		}
		//for rf.logEntries[index-1].Term == reply.ConflictTerm {
		//	index--
		//}
		reply.ConflictIndex = index
		reply.Accept = false
		reply.Back = MismatchTerm
		//log.Printf("beatState: %d MismatchTerm: leader raft %d has %d term is mismatch with follower raft %d has %d term at index %d!\n",
		//	args.BeatState, args.PeerIndex, args.PrevLogTerm, rf.me, rf.logEntries[args.PrevLogIndex].Term, args.PrevLogIndex)
		return
	}
	switch args.BeatState {
	case Beat:

	case Append:
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		//if len(args.Entries) > 0 && len(rf.logEntries) > args.PrevLogIndex+1 && rf.logEntries[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		//	log.Printf("raft %d overrided by raft %d!\n", rf.me, args.PeerIndex)
		//	rf.logEntries = rf.logEntries[0 : args.PrevLogIndex+1]
		//}
		// Append any new entries not already in the log
		//if len(rf.logEntries)-args.PrevLogIndex-1 < len(args.Entries) {
		//	//log.Printf("args.Entries start from index %d = %v\n", args.PrevLogIndex+1, args.Entries)
		//	//log.Printf("raft %d get Append from raft %d!\n", rf.me, args.PeerIndex)
		//	notin := args.Entries[len(rf.logEntries)-args.PrevLogIndex-1 : len(args.Entries)]
		//	rf.logEntries = append(rf.logEntries, notin...)
		//}
		i := args.PrevLogIndex + 1
		for i < min(len(rf.logEntries), args.PrevLogIndex+len(args.Entries)+1) {
			if args.Entries[i-args.PrevLogIndex-1] != rf.logEntries[i] {
				rf.logEntries = rf.logEntries[0:i]
				break
			}
			i++
		}
		//if i-args.PrevLogIndex-1 < len(args.Entries) {
		//	rf.logEntries = append(rf.logEntries, args.Entries[i-args.PrevLogIndex-1:]...)
		//}
		if i <= args.PrevLogIndex+len(args.Entries) {
			rf.logEntries = rf.logEntries[0:i]
			rf.logEntries = append(rf.logEntries, args.Entries[i-args.PrevLogIndex-1:]...)
		}
		//rf.logEntries = rf.logEntries[0 : args.PrevLogIndex+1]
		//rf.logEntries = append(rf.logEntries, args.Entries...)
	}
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		//log.Printf("args.Entries start from index %d = %v\n", args.PrevLogIndex+1, args.Entries)
		//log.Printf("raft %d.Entries = %v", rf.me, rf.logEntries)
		//log.Printf("state is %d,raft %d commitIndex updated by raft %d!\n", args.BeatState, rf.me, args.PeerIndex)
		nowIndex := min(args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
		rf.commitIndex = nowIndex
	}
	// log.Println("len(rf.logEntries)  = ", len(rf.logEntries), "rf.me = ", rf.me, "bool = ", reply.Accept)
}

func (rf *Raft) AppendEntries(server int, BeatState int) {
	args := AppendArgs{}
	reply := AppendReply{}
	args.BeatState = BeatState
	{
		rf.mu.Lock()
		args.PeerIndex = rf.me
		args.LeaderTerm = rf.currentTerm
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.logEntries[args.PrevLogIndex].Term
		if BeatState == Append {
			args.Entries = make([]entry, len(rf.logEntries)-args.PrevLogIndex-1)
			copy(args.Entries, rf.logEntries[args.PrevLogIndex+1:])
		}
		rf.mu.Unlock()
	}
	switch BeatState {
	case Beat:
		if atomic.LoadInt32(&rf.state) == Leader && rf.killed() == false {
			ok := rf.peers[server].Call("Raft.ReceiveEntries", &args, &reply)
			if ok {
				rf.mu.Lock()
				if !reply.Accept {
					if reply.Back == LowerTerm {
						if atomic.CompareAndSwapInt32(&rf.state, Leader, Follower) {
							log.Printf("beat: raft %d has term %d is lower than raft %d has term %d", rf.me, rf.currentTerm, server, reply.Term)
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							rf.mu.Unlock()
							go rf.followerSelect()
							return
						}
					} else if reply.Back == ShortLog {
						rf.nextIndex[server] = reply.ConflictIndex
						rf.matchIndex[server] = rf.nextIndex[server] - 1
					} else if reply.Back == MismatchTerm {
						findIndex := -1
						conflictTerm := reply.ConflictTerm
						// 找到日志条目中term等于conflictTerm的最后一个条目的下标，没有则返回-1
						// 利用term的单调性，可以二分
						l := 1
						r := len(rf.logEntries) - 1
						for l <= r {
							mid := (l + r) / 2
							if conflictTerm < rf.logEntries[mid].Term {
								l = mid + 1
							} else if conflictTerm == rf.logEntries[mid].Term {
								findIndex = mid
								l = mid + 1
							} else {
								r = mid - 1
							}
						}
						//for {
						//	if conflictTerm == rf.logEntries[index].Term {
						//		findIndex = index
						//		break
						//	}
						//	index--
						//	if index == 0 {
						//		break
						//	}
						//}
						if findIndex != -1 {
							rf.nextIndex[server] = findIndex + 1
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
						rf.matchIndex[server] = rf.nextIndex[server] - 1
					}
					if atomic.LoadInt32(&rf.state) == Leader {
						go rf.AppendEntries(server, Append)
					}
				}
				rf.mu.Unlock()
			} else {
				//log.Printf("beat: raft %d find raft %d is disconnect!\n", rf.me, server)
				//time.Sleep(HeartBeatTime / 2)
				//if atomic.LoadInt32(&rf.state) == Leader {
				//	go rf.AppendEntries(server, Beat)
				//}
			}
		}
	case Append:
		if atomic.LoadInt32(&rf.state) == Leader && rf.killed() == false {
			ok := rf.peers[server].Call("Raft.ReceiveEntries", &args, &reply)
			if ok {
				rf.mu.Lock()
				if !reply.Accept {
					if reply.Back == LowerTerm {
						if atomic.CompareAndSwapInt32(&rf.state, Leader, Follower) {
							log.Printf("append: raft %d has term %d is lower than raft %d has term %d", rf.me, rf.currentTerm, server, reply.Term)
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							rf.mu.Unlock()
							go rf.followerSelect()
							return
						}
					} else if reply.Back == ShortLog {
						rf.nextIndex[server] = reply.ConflictIndex
						rf.matchIndex[server] = rf.nextIndex[server] - 1
					} else if reply.Back == MismatchTerm {
						findIndex := -1
						conflictTerm := reply.ConflictTerm
						// 找到日志条目中term等于conflictTerm的最后一个条目的下标，没有则返回-1
						// 利用term的单调性，可以二分
						l := 1
						r := len(rf.logEntries) - 1
						for l <= r {
							mid := (l + r) / 2
							if conflictTerm < rf.logEntries[mid].Term {
								l = mid + 1
							} else if conflictTerm == rf.logEntries[mid].Term {
								findIndex = mid
								l = mid + 1
							} else {
								r = mid - 1
							}
						}
						//for {
						//	if conflictTerm == rf.logEntries[index].Term {
						//		findIndex = index
						//		break
						//	}
						//	index--
						//	if index == 0 {
						//		break
						//	}
						//}
						if findIndex != -1 {
							rf.nextIndex[server] = findIndex + 1
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
						rf.matchIndex[server] = rf.nextIndex[server] - 1
					}
					if atomic.LoadInt32(&rf.state) == Leader {
						go rf.AppendEntries(server, Append)
					}
				} else {
					//log.Printf("server = %d\n", server)
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
			} else {

			}
		}
	}
}

func (rf *Raft) commitInc() {
	for atomic.LoadInt32(&rf.state) == Leader && rf.killed() == false {

		{
			rf.mu.Lock()
			length := len(rf.peers)
			rf.matchIndex[rf.me] = len(rf.logEntries) - 1
			temp := make([]int, length)
			copy(temp, rf.matchIndex)
			log.Printf("leader raft is %d term is %d temp is %v\n", rf.me, rf.currentTerm, temp)
			sort.Ints(temp)
			if temp[length/2] < len(rf.logEntries) && temp[length/2] > rf.commitIndex && rf.logEntries[temp[length/2]].Term == rf.currentTerm {
				rf.commitIndex = temp[length/2]
			}
			rf.mu.Unlock()
		}

		time.Sleep(HeartBeatTime * 2)
	}
}

func (rf *Raft) beatAll() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			if len(rf.logEntries) > rf.nextIndex[i] {
				go rf.AppendEntries(i, Append)
			} else {
				rf.nextIndex[i] = len(rf.logEntries)
				go rf.AppendEntries(i, Beat)
			}
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) leaderSelect() {
	// rf.convertState(Leader)
	atomic.StoreInt32(&rf.state, Leader)

	{
		rf.mu.Lock()
		if rf.nextIndex == nil || rf.matchIndex == nil {
			log.Printf("raft %d first come to power!\n", rf.me)
			rf.matchIndex = make([]int, len(rf.peers))
			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logEntries)
				rf.matchIndex[i] = 0
			}
			rf.matchIndex[rf.me] = len(rf.peers) - 1
		}
		rf.mu.Unlock()
	}

	ticker := time.NewTicker(HeartBeatTime)
	defer ticker.Stop()
	go rf.beatAll()
	go rf.commitInc()
	for atomic.LoadInt32(&rf.state) == Leader && rf.killed() == false {
		select {
		case <-ticker.C:
			go rf.beatAll()
		case <-rf.beatChan:
			if atomic.CompareAndSwapInt32(&rf.state, Leader, Follower) {
				go rf.followerSelect()
			}
			return
		case <-rf.voteChan:
			if atomic.CompareAndSwapInt32(&rf.state, Leader, Follower) {
				go rf.followerSelect()
			}
			return
		}
	}
}

func (rf *Raft) applyMonitor() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied && rf.lastApplied+1 < len(rf.logEntries) {
			Entry := rf.logEntries[rf.lastApplied+1]
			log.Printf("term is %d raft %d apply %v index %d!\n", rf.currentTerm, rf.me, Entry, rf.lastApplied+1)
			apply := ApplyMsg{
				CommandValid: true,
				Command:      Entry.Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.lastApplied++
			rf.mu.Unlock()
			rf.applyCh <- apply
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(HeartBeatTime)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func (rf *Raft) init() {
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = append(rf.logEntries, entry{Term: 0, Command: "a sentinel,no command!"})
	atomic.StoreInt32(&rf.state, Follower)
	rf.beatChan = make(chan struct{}, 10)
	rf.voteChan = make(chan struct{}, 10)
	rf.commitIndex = 0
	rf.lastApplied = 0
	//log.Printf("%d is restarted!\n", rf.me)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	seed := rand.Int63()
	// log.Printf("seed is %v\n", seed)
	rand.Seed(seed)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.init()
	rf.readPersist(persister.ReadRaftState())
	go rf.followerSelect()
	go rf.applyMonitor()
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	return rf
}
