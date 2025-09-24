package pbservice

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"umich.edu/eecs491/proj2/viewservice"
)

var (
	nameInitialized  sync.Once
	nameChan         chan string
)

func nameGenerator() {
	serial := 0

	for {
		serial = serial + 1
		next := "PBClient-" + strconv.Itoa(serial)
		nameChan <- next
	}
}

func nameInitialize() {
	nameInitialized.Do(func () {
		nameChan = make(chan string)
		go nameGenerator()
	})
}

type Clerk struct {
	me        string
	seqno     int
	vs       *viewservice.Clerk
	primary   string
}


func MakeClerk(vshost string, me string) *Clerk {
	nameInitialize()

	ck := new(Clerk)

	if me == "" {
		ck.me = <-nameChan
	} else {
		ck.me = me
	}
	ck.seqno = 0
	ck.vs = viewservice.MakeClerk(me, vshost)
	ck.primary = ""

	return ck
}

func (ck *Clerk) refreshPrimary() {
	run := true
	for run {
		time.Sleep(viewservice.PingInterval)
		ck.primary = ck.vs.Primary()
		run = (ck.primary == "")
	}
}


// XXX: Abstract operation bits away, combine Get/Put/Append
//      into front-ends for a single function that handles
//      RPC bits.
//      Also: move call into rpcs.go?

//
// Perform an operation
// 
// The operation must keep trying until the (current)
// primary replies 
//
func (ck *Clerk) doOperation(op Op, key string,
	value string, reply *OpReply) {

	// ask the viewservice for the primary if not already cached
	if ck.primary == "" {
		ck.refreshPrimary()
	}

	// Increment sequence number
	ck.seqno = ck.seqno + 1

	// Create the argument/reply structs
	args := OpArgs{Op: op, Key: key, Value: value,
		Client: ck.me, SeqNo: ck.seqno, Source: ck.me}

	for true {
		// Issue until RPC succeeds
		for {
			ok := call(ck.primary, "PBServer.Operation", args, &reply)
			if ok {
				break
			}
			log.Printf("DoOp RPC issued to %s failed\n", ck.primary)
			time.Sleep(viewservice.PingInterval)
			ck.refreshPrimary()
		}
		
		if reply.Err == ErrWrongServer {
			ck.refreshPrimary()
		} else {
			return
		}
	}
}


//
// Get a value for a key
//
func (ck *Clerk) Get(key string) string {

	var reply OpReply

	log.Printf("%s: Getting value for key %s\n", ck.me, key)
	ck.doOperation(GET, key, "", &reply)

	if reply.Err == ErrNoKey {
		return ""
	} else {
		return reply.Value
	}
}

//
// tell the primary to update key's value.
//
func (ck *Clerk) Put(key string, value string) {

	var reply OpReply

	log.Printf("%s: Putting value %s for key %s\n", ck.me, value, key)
	ck.doOperation(PUT, key, value, &reply)
}

//
// tell the primary to append to key's value.
//
func (ck *Clerk) Append(key string, value string) {

	var reply OpReply

	log.Printf("%s: Appending value %s to key %s\n", ck.me, value, key)
	ck.doOperation(APPEND, key, value, &reply)
}



//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


