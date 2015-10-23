package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const DEBUG = false

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type AcceptorInstance struct {
	instance_ID int
	n_prep      int
	n_accept    int
	val_accept  interface{}
	lock        sync.Mutex
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	n_peers    int
	majority   int
	maxSeq     int
	decided    map[int]bool

	mins      []int
	globalMin int

	//Proposer data, organized in maps
	n_highest map[int]int
	acceptor  map[int]*AcceptorInstance

	lock sync.Mutex

	//Map of the values
	val map[int]interface{}

	nonvoter      bool
	observedPreps map[int]int
}

type PrepareArgs struct {
	N      int
	Seq    int
	Mins   []int
	Peer_n int
}

type AcceptArgs struct {
	N      int
	V      interface{}
	Seq    int
	Mins   []int
	Peer_n int
}

type PrepAcceptRet struct {
	OK     bool
	N_a    int
	V_a    interface{}
	Mins   []int
	Peer_n int
}

type DecidedArgs struct {
	N      int
	Value  interface{}
	Seq    int
	Mins   []int
	Peer_n int
}
type DecidedRet struct {
	OK     bool
	Mins   []int
	Peer_n int
}

var log_mu sync.Mutex

func (px *Paxos) Logf(format string, a ...interface{}) {
	if !DEBUG {
		return
	}

	log_mu.Lock()
	defer log_mu.Unlock()

	me := px.me

	fmt.Printf("\x1b[%dm", (me%6)+31)
	fmt.Printf("S#%d : ", me)
	fmt.Printf(format+"\n", a...)
	fmt.Printf("\x1b[0m")
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) DumpState(dir string) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	fullname := dir + "/paxos"

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(px.maxSeq)
	e.Encode(px.decided)
	e.Encode(px.mins)
	e.Encode(px.globalMin)
	e.Encode(px.val)
	e.Encode(px.n_highest)
	e.Encode(px.acceptor)

	return ioutil.WriteFile(fullname, w.Bytes(), 0666)
}

func (px *Paxos) RecoverState(dir string) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	content, err := ioutil.ReadFile(dir + "/paxos")

	r := bytes.NewBuffer(content)
	d := gob.NewDecoder(r)

	d.Decode(&px.maxSeq)
	d.Decode(&px.decided)
	d.Decode(&px.mins)
	d.Decode(&px.globalMin)
	d.Decode(&px.val)
	d.Decode(&px.n_highest)
	d.Decode(&px.acceptor)

	return err

}

func (px *Paxos) GetGlobalMin() int {
	return px.globalMin
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.Logf("Paxos start %d!", seq)
	go func() {
		px.Propose(v, seq)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.lock.Lock()
	defer px.lock.Unlock()
	px.mins[px.me] = seq
	px.updateGlobalMin()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.lock.Lock()
	defer px.lock.Unlock()
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.lock.Lock()
	defer px.lock.Unlock()
	return px.globalMin + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.lock.Lock()
	defer px.lock.Unlock()
	if seq < px.globalMin {
		px.Logf("Returning Forgotten from Status(%d)", seq)
		return Forgotten, nil
	}
	if px.decided[seq] {
		px.Logf("Returning Decided from Status(%d)", seq)
		return Decided, px.val[seq]
	}
	px.Logf("Returning Pending from Status(%d)", seq)
	return Pending, nil
}

func rndAbove(n int) int {
	randomSpace := int32(1 << 30)
	value := n + int(rand.Int31n(randomSpace))
	return int(value)
}

func (px *Paxos) minFromPeer(newMins []int, peer int) {
	px.lock.Lock()
	defer px.lock.Unlock()

	update := false
	for i, v := range newMins {

		if v > px.mins[i] {
			px.Logf("New min from %d: %d \n", i, v)
			px.mins[i] = v
			update = true
		}
	}

	if update {
		px.updateGlobalMin()
	}
}

func (px *Paxos) updateGlobalMin() {
	px.Logf("updateGlobalMin: %d! %v\n", px.globalMin, px.mins)
	oldGlobalMin := px.globalMin
	maxInt := int(^uint(0) >> 1)
	currentGlobalMin := maxInt
	for _, min := range px.mins {
		if min < currentGlobalMin {
			currentGlobalMin = min
		}
	}

	if currentGlobalMin > px.globalMin {
		px.globalMin = currentGlobalMin
		px.freeResources(oldGlobalMin)
		px.Logf("New global min: %d!\n", px.globalMin)
	}

}

func (px *Paxos) freeResources(prevMin int) {
	for i := prevMin; i < px.globalMin; i++ {
		delete(px.val, i)
		delete(px.acceptor, i)
	}
}

func (px *Paxos) attemptRPCMajority(rpcname string, args interface{}) (majority bool, highest_n int, highest_v interface{}) {

	// Keep all the OK responses
	okResponses := make(chan PrepAcceptRet, px.n_peers)
	notOkResponses := make(chan PrepAcceptRet, px.n_peers)

	//Channel to stop extra rpcs not needed
	done := make(chan bool)
	channel_closed := false

	// Start a goroutine with an rpc to every peer
	for i := range px.peers {
		peer := px.peers[i]
		me := i == px.me
		go px.RPCAttempt(peer, me, rpcname, args, okResponses, notOkResponses, done)
	}

	numberNotOk := 0
	numberOk := 0

	for true {
		if px.isdead() {
			return
		}

		select {
		case resp := <-okResponses:
			numberOk++
			if resp.N_a > highest_n {
				highest_n = resp.N_a
				highest_v = resp.V_a
			}
			px.minFromPeer(resp.Mins, resp.Peer_n)

		case resp := <-notOkResponses:
			numberNotOk++
			px.minFromPeer(resp.Mins, resp.Peer_n)
		case <-time.After(1 * time.Second):
			//Timeout!
			numberNotOk++
		}

		if numberOk >= px.majority {
			majority = true
			if !channel_closed {
				//Notify that there is no point retrying RPC any more
				close(done)
				channel_closed = true
			}
		} else if numberNotOk >= px.majority {
			majority = false
			if !channel_closed {
				//Notify that there is no point retrying RPC any more
				close(done)
				channel_closed = true
			}
		}

		//Only return when all rpc have returned.
		//Remember that not reaching the peer counts as not ok
		//So even in partitions this is fine,
		//Without this we get some speed but risk "too many files open"
		if numberOk+numberNotOk == px.n_peers {
			return
		}
	}

	return
}

func (px *Paxos) RPCAttempt(peer string, me bool, rpcname string, args interface{},
	okResponses chan PrepAcceptRet, notOkResponses chan PrepAcceptRet, done chan bool) {

	ret := PrepAcceptRet{}

	//RPC ok response
	ok := false

	//No RPC if I'm calling myself
	if me && rpcname == "Paxos.Accept" {
		ok = true
		px.Accept(args.(AcceptArgs), &ret)
	} else { //We have to RPC

		//Retry RPC up to 3 times
		for i := 0; i < 3 && !ok; i++ {

			select {
			case <-done:
				//No need to keep trying, already enough answers
				notOkResponses <- ret
				px.Logf("Stopping RPC retry!\n")
				return
			default:
				ok = call(peer, rpcname, args, &ret)
				if !ok {
					//px.Logf("Retrying call(%s,%s)\n", peer, rpcname)
				}
			}
		}
	}

	if ok && ret.OK {
		okResponses <- ret
	} else {
		notOkResponses <- ret
	}

}

func (px *Paxos) Propose(val interface{}, Seq int) {

	//	px.Logf("Propose(%d)\n", Seq)

	//Data race condition
	px.lock.Lock()
	if Seq > px.maxSeq {
		px.maxSeq = Seq
	}
	px.lock.Unlock()

	decided := false
	for !decided && !px.isdead() {

		px.lock.Lock()
		n := rndAbove(px.n_highest[Seq])

		mins_copy := make([]int, len(px.mins))
		copy(mins_copy, px.mins)
		prepArgs := PrepareArgs{n, Seq, mins_copy, px.me}
		px.lock.Unlock()

		prepMajority, highest_n, highest_v := px.attemptRPCMajority("Paxos.Prepare", prepArgs)

		if highest_n <= 0 { //No other values, use own
			px.Logf("%d: Choosing our own value! \n", Seq)
			highest_v = val
			highest_n = n
		}

		//Make sure we update the highest seen n even in case of declines
		px.lock.Lock()
		if highest_n > px.n_highest[Seq] {
			px.n_highest[Seq] = highest_n
			px.Logf("New high_n %d\n", highest_n)
		}
		px.lock.Unlock()

		if prepMajority {
			px.Logf("%d: Got majority prepare! \n", Seq)

			acceptArgs := AcceptArgs{N: n, V: highest_v, Seq: Seq}

			acceptMajority, _, _ := px.attemptRPCMajority("Paxos.Accept", acceptArgs)
			px.Logf("%d: Accept has returned! \n", Seq)

			if acceptMajority {
				px.Logf("%d Got majority accept! \n", Seq)

				px.lock.Lock()
				mins_copy := make([]int, len(px.mins))
				copy(mins_copy, px.mins)
				decidedArgs := DecidedArgs{
					N:      n,
					Value:  highest_v,
					Seq:    Seq,
					Mins:   mins_copy,
					Peer_n: px.me}
				px.lock.Unlock()

				px.sendDecidedToAll(decidedArgs)
			}
		}

		px.lock.Lock()
		decided = px.decided[Seq]
		px.lock.Unlock()
	}
}

// Concurrently send a decided message to every peer. Including self.
func (px *Paxos) sendDecidedToAll(args DecidedArgs) {
	for i, peer := range px.peers {
		ret := DecidedRet{}
		if i == px.me {
			px.Decided(args, &ret)
		} else {
			go px.sendDecided(args, ret, peer)
		}
	}

}

//Send a decided message to a specific peer and handle return of min value
//Run in multiple goroutines concurrently
func (px *Paxos) sendDecided(args DecidedArgs, ret DecidedRet, peer string) {
	ok := call(peer, "Paxos.Decided", args, &ret)

	if ok {
		px.minFromPeer(ret.Mins, ret.Peer_n)
	}

}

func (px *Paxos) Decided(args DecidedArgs, ret *DecidedRet) (err error) {
	//	px.Logf("Decided(%d)\n", args.Seq)

	px.lock.Lock()
	if px.observedPreps[args.Seq] == args.N {
		px.nonvoter = false
		fmt.Printf("Paxos no longer nonvoter! \n")
	}
	if args.Seq > px.maxSeq {
		px.maxSeq = args.Seq
	}

	if !px.decided[args.Seq] {
		px.Logf("%d: VALUE DECIDED! \n", args.Seq)
		px.val[args.Seq] = args.Value
		px.decided[args.Seq] = true
	}

	ret.OK = true
	mins_copy := make([]int, len(px.mins))
	copy(mins_copy, px.mins)
	ret.Mins = mins_copy
	ret.Peer_n = px.me

	px.lock.Unlock()
	return nil
}

func (px *Paxos) Prepare(args PrepareArgs, ret *PrepAcceptRet) (err error) {
	//	px.Logf("Prepare(%d)\n", args.Seq)

	px.minFromPeer(args.Mins, args.Peer_n)

	//For memory management
	px.lock.Lock()
	mins_copy := make([]int, len(px.mins))
	copy(mins_copy, px.mins)
	ret.Mins = mins_copy
	ret.Peer_n = px.me

	if args.Seq > px.maxSeq {
		px.maxSeq = args.Seq
	}

	ac, OK := px.acceptor[args.Seq]
	if !OK {
		ac = &AcceptorInstance{}
		px.acceptor[args.Seq] = ac
	}
	px.lock.Unlock()

	ac.lock.Lock()
	defer ac.lock.Unlock()

	if px.nonvoter {
		px.observedPreps[args.Seq] = args.N
		fmt.Printf("Nonvoter, not responding to prepare Seq: %d N: \n", args.Seq, args.N)
		return nil
	}

	if args.N > ac.n_prep {
		ac.n_prep = args.N

		px.Logf("%d prepare_ok(N_a: %d) args.N: %d, ac.n_prep: %d \n", args.Seq, ac.n_accept, args.N, ac.n_prep)
		ret.OK = true
		ret.N_a = ac.n_accept
		ret.V_a = ac.val_accept
	} else {
		px.Logf("%d prepare_decline(N: %d  N_prep: %d)! \n", args.Seq, args.N, ac.n_prep)
		ret.N_a = ac.n_prep
		px.Logf("%d ret.N_a %d \n", args.Seq, ret.N_a)
		ret.OK = false
	}
	return nil
}

func (px *Paxos) Accept(args AcceptArgs, ret *PrepAcceptRet) (err error) {
	//	px.Logf("Accept(%d)\n", args.Seq)

	px.minFromPeer(args.Mins, args.Peer_n)

	px.lock.Lock()

	if args.Seq > px.maxSeq {
		px.maxSeq = args.Seq
	}

	ac, OK := px.acceptor[args.Seq]
	if !OK {
		ac = &AcceptorInstance{}
		px.acceptor[args.Seq] = ac
	}

	px.lock.Unlock()

	n := args.N
	v := args.V

	ac.lock.Lock()
	defer ac.lock.Unlock()

	if px.nonvoter {
		ret.OK = false
		fmt.Printf("Nonvoter, not responding to accept. Seq: %d  \n", args.Seq)
		return nil
	}
	if n >= ac.n_prep {
		px.Logf("%d accept_ok(N: %d  N_prep: %d)! \n", args.Seq, n, ac.n_prep)
		ac.n_prep = n
		ac.n_accept = n
		ac.val_accept = v
		ret.OK = true
	} else {
		px.Logf("%d accept_decline(N: %d  N_prep: %d)! \n", args.Seq, n, ac.n_prep)
		ret.OK = false
	}

	return nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

func (px *Paxos) BecomeNonvoter() {
	px.nonvoter = true
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	px.val = make(map[int]interface{})
	px.decided = make(map[int]bool)
	px.n_highest = make(map[int]int)
	px.acceptor = make(map[int]*AcceptorInstance)
	px.n_peers = len(peers)
	px.majority = (px.n_peers / 2) + px.n_peers%2
	px.mins = make([]int, px.n_peers)

	px.globalMin = -1

	px.nonvoter = false
	px.observedPreps = make(map[int]int)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
