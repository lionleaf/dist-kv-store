package shardmaster

import (
	"math/rand"
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"

const Debug = false

var log_mu sync.Mutex

func (sm *ShardMaster) Logf(format string, a ...interface{}) {
	if !Debug {
		return
	}

	log_mu.Lock()
	defer log_mu.Unlock()

	me := sm.me

	fmt.Printf("\x1b[%dm", (me%6)+31)
	fmt.Printf("SM#%d : ", me)
	fmt.Printf(format+"\n", a...)
	fmt.Printf("\x1b[0m")
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	opReqChan    chan OpReq
	lastDummySeq int            //Seq of last time we launched a dummy op to fill a hole
	activeGIDs   map[int64]bool //If inactive remove from map instead of setting to false

	lastConfig int

	configs []Config // indexed by config num
}

type Op struct {
	OpID    int64
	Type    OpType
	GID     int64    //Used by all Ops but Query
	Servers []string //Used by Join
	Shard   int      //Used by move
	Num     int      //Used by Query
}

type OpType int

const (
	JoinOp OpType = iota + 1
	LeaveOp
	MoveOp
	QueryOp
	Dummy
)

type OpReq struct {
	op        Op
	replyChan chan Config
}

func (sm *ShardMaster) sequentialApplier() {

	seq := 1
	for !sm.isdead() {
		select {
		case opreq := <-sm.opReqChan:
			op := opreq.op
			sm.Logf("Got operation through channel")
			seq = sm.addToPaxos(seq, op)
			sm.Logf("Operation added to paxos log at %d", seq)

			if op.Type == QueryOp {
				if op.Num < 0 {
					//Returning latest config
					opreq.replyChan <- sm.configs[sm.lastConfig]
					sm.Logf("Query applied! Feeding value config nr %d through channel. %d", sm.lastConfig, seq)
				} else {
					opreq.replyChan <- sm.configs[op.Num]
					sm.Logf("Query applied! Feeding value config nr %d through channel. %d", op.Num, seq)
				}
			} else {
				opreq.replyChan <- Config{}
			}

		case <-time.After(50 * time.Millisecond):
			sm.Logf("Ping")
			seq = sm.ping(seq)
		}

		sm.Logf("Calling Done(%d)", seq-2)
		sm.px.Done(seq - 1)
	}

}

//Takes the last non-applied seq and returns the new one
func (sm *ShardMaster) ping(seq int) int {
	//TODO: Is this a good dummy OP?
	dummyOp := Op{}

	for !sm.isdead() {
		fate, val := sm.px.Status(seq)

		if fate == paxos.Decided {
			sm.applyOp(val.(Op))
			seq++
			continue
		}

		if sm.px.Max() > seq && seq > sm.lastDummySeq {
			sm.px.Start(seq, dummyOp)
			sm.waitForPaxos(seq)
			sm.lastDummySeq = seq
		} else {
			return seq
		}

	}
	sm.Logf("ERRRRORR: Ping fallthrough, we are dying! Return seq -1 ")
	return -1

}

func (sm *ShardMaster) addToPaxos(seq int, op Op) (retseq int) {
	for !sm.isdead() {
		//Suggest OP as next seq
		sm.px.Start(seq, op)

		val, err := sm.waitForPaxos(seq)

		if err != nil {
			sm.Logf("ERRRROROOROROO!!!")
			continue
		}

		sm.applyOp(val.(Op))

		seq++

		//Did work?
		if val.(Op).OpID == op.OpID {
			sm.Logf("Applied operation in log at seq %d", seq-1)
			return seq
		} else {
			sm.Logf("Somebody else took seq %d before us, applying it and trying again", seq-1)
		}
	}
	return -1

}

func (sm *ShardMaster) waitForPaxos(seq int) (val interface{}, err error) {
	var status paxos.Fate
	to := 10 * time.Millisecond
	for {
		status, val = sm.px.Status(seq)
		if status == paxos.Decided {
			err = nil
			return
		}

		if status == paxos.Forgotten || sm.isdead() {
			err = fmt.Errorf("We are dead or waiting for something forgotten. Server shutting down?")
			sm.Logf("We are dead or waiting for something forgotten. Server shutting down?")
			return
		}

		sm.Logf("Still waiting for paxos: %d", seq)
		time.Sleep(to)
		if to < 3*time.Second {
			to *= 2
		} else {
			err = fmt.Errorf("Wait for paxos timeout!1")
			return
		}
	}

}

func (sm *ShardMaster) applyOp(op Op) {
	sm.Logf("Applying op to database")
	switch op.Type {
	case JoinOp:
		sm.Logf("Join, you guys!")
		sm.ApplyJoin(op.GID, op.Servers)
	case LeaveOp:
		sm.ApplyLeave(op.GID)
		sm.Logf("Leave op applied!")
	case MoveOp:
		sm.ApplyMove(op.GID, op.Shard)
		sm.Logf("Move op applied!")
	case QueryOp:
		//Do nothing
	case Dummy:
		//Do nothing
	}
}

func (sm *ShardMaster) ApplyMove(GID int64, Shard int) {
	newConfig := sm.makeNewConfig()
	newConfig.Shards[Shard] = GID
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) ApplyJoin(GID int64, Servers []string) {
	sm.activeGIDs[GID] = true
	newConfig := sm.makeNewConfig()
	newConfig.Groups[GID] = Servers

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)

}

func (sm *ShardMaster) ApplyLeave(GID int64) {
	delete(sm.activeGIDs, GID)
	newConfig := sm.makeNewConfig()
	delete(newConfig.Groups, GID)
	for i, group := range newConfig.Shards {
		if group == GID {
			newConfig.Shards[i] = 0 //Set to invalid group. Will be distributed by the rebalance
		}
	}

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) rebalanceShards(newConfig *Config) {

	nShards := len(newConfig.Shards)
	nGroups := 0

	for _, _ = range newConfig.Groups {
		nGroups++
	}

	groupToShards := make(map[int64][]int)

	groupToShards[0] = []int{}
	for GUID, _ := range sm.activeGIDs {
		groupToShards[GUID] = []int{}
	}

	for i, v := range newConfig.Shards {
		GUID := v
		groupToShards[GUID] = append(groupToShards[GUID], i)
	}

	minGUID, minGroupSize := getMin(groupToShards)
	maxGUID, maxGroupSize := getMax(groupToShards)

	minShardsPerGroup := nShards / nGroups
	maxShardsPerGroup := minShardsPerGroup
	if nShards%nGroups > 0 {
		maxShardsPerGroup += 1
	}

	for len(groupToShards[0]) > 0 || minGroupSize < minShardsPerGroup || maxGroupSize > maxShardsPerGroup {
		sm.Logf("Rebalance iteration! ")
		sm.Logf("%d > 0! ", len(groupToShards[0]))
		sm.Logf("min %d < %d (GUID: %d)! ", minGroupSize, minShardsPerGroup, minGUID)
		sm.Logf("max %d > %d!(GUID: %d) ", maxGroupSize, maxShardsPerGroup, maxGUID)

		shardsInInvalidGroup := len(groupToShards[0])
		if shardsInInvalidGroup > 0 {
			for i := 0; i < shardsInInvalidGroup; i++ {
				sm.Logf("Rebalance 0 iteration!")

				moveshard := groupToShards[0][0] //Remove the first on
				groupToShards[0] = sliceDel(groupToShards[0], 0)

				minGUID, _ := getMin(groupToShards)

				groupToShards[minGUID] = append(groupToShards[minGUID], moveshard)

				newConfig.Shards[moveshard] = minGUID
				sm.Logf("Moving shard %d to group %d", moveshard, minGUID)

			}
			_, minGroupSize = getMin(groupToShards)
			_, maxGroupSize = getMax(groupToShards)
			continue
		}

		minGUID, minGroupSize = getMin(groupToShards)
		maxGUID, maxGroupSize = getMax(groupToShards)
		sm.Logf("min %d (GUID: %d) ", minGroupSize, minGUID)
		sm.Logf("max %d (GUID: %d) ", maxGroupSize, maxGUID)

		maxCanGive := maxGroupSize - minShardsPerGroup
		minNeeds := minShardsPerGroup - minGroupSize

		shardsToMove := minNeeds
		if maxCanGive < minNeeds {
			shardsToMove = maxCanGive
		}
		sm.Logf("Moving %d shards: minNeeds %d, maxCanGive  %d!", shardsToMove, minNeeds, maxCanGive)

		for i := 0; i < shardsToMove; i++ {
			moveshard := groupToShards[maxGUID][i]

			groupToShards[minGUID] = append(groupToShards[minGUID], moveshard)

			groupToShards[maxGUID] = sliceDel(groupToShards[maxGUID], i)

			newConfig.Shards[moveshard] = minGUID
			sm.Logf("Moving shard %d to group %d", moveshard, minGUID)
		}

		_, minGroupSize = getMin(groupToShards)
		_, maxGroupSize = getMax(groupToShards)
		sm.Logf("min %d < %d (GUID: %d)! ", minGroupSize, minShardsPerGroup, minGUID)
		sm.Logf("max %d > %d!(GUID: %d) ", maxGroupSize, maxShardsPerGroup, maxGUID)

	}

}

func getMax(groupToShards map[int64][]int) (GUID int64, nShards int) {
	for guid, shards := range groupToShards {
		if guid == 0 {
			//GUID 0 is invalid and should not be counted
			continue
		}
		if len(shards) >= nShards {
			GUID = guid
			nShards = len(shards)
		}
	}
	return
}

func getMin(groupToShards map[int64][]int) (GUID int64, nShards int) {
	nShards = 2147483647 //Max signed int32 int is 32 or 64, so this will fit
	for guid, shards := range groupToShards {
		if guid == 0 {
			//GUID 0 is invalid and should not be counted
			continue
		}
		if len(shards) < nShards {
			GUID = guid
			nShards = len(shards)
		}
	}
	return
}

func sliceDel(a []int, i int) []int {
	return append(a[:i], a[i+1:]...)
}

func sliceDelInt64(a []int64, i int) []int64 {
	return append(a[:i], a[i+1:]...)
}

func (sm *ShardMaster) makeNewConfig() Config {
	oldConfig := sm.configs[sm.lastConfig]
	newConfig := Config{Groups: make(map[int64][]string)}

	newConfig.Num = oldConfig.Num + 1
	sm.lastConfig = newConfig.Num

	newConfig.Shards = oldConfig.Shards //TODO: Does this work?

	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}

	return newConfig
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	op := Op{Type: JoinOp, OpID: rand.Int63(), GID: args.GID, Servers: args.Servers}

	opReq := OpReq{op, make(chan Config, 1)}

	sm.opReqChan <- opReq

	sm.Logf("Waiting on return channel!")
	<-opReq.replyChan
	sm.Logf("Got return!")
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	op := Op{Type: LeaveOp, OpID: rand.Int63(), GID: args.GID}

	opReq := OpReq{op, make(chan Config, 1)}

	sm.opReqChan <- opReq

	sm.Logf("Waiting on return channel!")
	<-opReq.replyChan
	sm.Logf("Got return!")
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	op := Op{Type: MoveOp, OpID: rand.Int63(), GID: args.GID, Shard: args.Shard}

	opReq := OpReq{op, make(chan Config, 1)}

	sm.opReqChan <- opReq

	sm.Logf("Waiting on return channel!")
	<-opReq.replyChan
	sm.Logf("Got return!")
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	op := Op{Type: QueryOp, OpID: rand.Int63(), Num: args.Num}

	opReq := OpReq{op, make(chan Config, 1)}

	sm.opReqChan <- opReq

	sm.Logf("Waiting on return channel!")
	reply.Config = <-opReq.replyChan
	sm.Logf("Got return!")

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.activeGIDs = map[int64]bool{}

	sm.opReqChan = make(chan OpReq)

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	go sm.sequentialApplier()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
