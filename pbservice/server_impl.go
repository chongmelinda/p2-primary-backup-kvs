package pbservice
import (
	"umich.edu/eecs491/proj2/viewservice"
    "time"
    "strconv"
    "strings"
)

type opRequest struct {
	args  OpArgs
	reply *OpReply
	done  chan bool
}

type pushRequest struct {
	args  PushArgs
	reply *PushReply
	done  chan bool
}

type tickRequest struct {
	done chan bool
}

type PBServerImpl struct {
    kv           map[string]string
    results      map[string]map[int]OpReply
    view         viewservice.View
    lastPingTime time.Time
    
    // Channels for serialization
    opChan    chan *opRequest
    pushChan  chan *pushRequest
    tickChan  chan *tickRequest
}

func (pb *PBServer) initImpl() {
	pb.impl.kv = make(map[string]string)
	pb.impl.results = make(map[string]map[int]OpReply)
    pb.impl.lastPingTime = time.Now()
    
    // Initialize channels
    pb.impl.opChan = make(chan *opRequest)
    pb.impl.pushChan = make(chan *pushRequest)
    pb.impl.tickChan = make(chan *tickRequest)
    
    // Start the serializer goroutine
    go pb.serializer()
}

// Single goroutine that processes all state modifications
func (pb *PBServer) serializer() {
	for {
		select {
		case req := <-pb.impl.opChan:
			pb.operationImpl(&req.args, req.reply)
			req.done <- true
			
		case req := <-pb.impl.pushChan:
			pb.pushImpl(&req.args, req.reply)
			req.done <- true
			
		case req := <-pb.impl.tickChan:
			pb.tickImpl()
			req.done <- true
		}
	}
}

// Operation() now just sends request through channel
func (pb *PBServer) Operation(args OpArgs, reply *OpReply) error {
	req := &opRequest{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	pb.impl.opChan <- req
	<-req.done
	return nil
}

// The actual operation logic (runs in serializer goroutine)
func (pb *PBServer) operationImpl(args *OpArgs, reply *OpReply) {
    if pb.isdead() {
        reply.Err = ErrWrongServer
        return
    }
    
    timeSinceLastPing := time.Since(pb.impl.lastPingTime)
    if timeSinceLastPing > viewservice.PingInterval * viewservice.DeadPings {
        reply.Err = ErrWrongServer
        return
    }

    isFromPrimary := (args.Source == pb.impl.view.Primary)
    if args.Source == "" {
        if pb.me != pb.impl.view.Primary {
            reply.Err = ErrWrongServer
            return
        }
    } else {
        if !(pb.me == pb.impl.view.Backup && isFromPrimary) && !(pb.me == pb.impl.view.Primary && args.Source == pb.impl.view.Primary) {
            if pb.me != pb.impl.view.Primary {
                reply.Err = ErrWrongServer
                return
            }
        }
    }
    if _, ok := pb.impl.results[args.Client]; !ok {
        pb.impl.results[args.Client] = make(map[int]OpReply)
    }

    // Check for exact sequence number match
    if cached, ok := pb.impl.results[args.Client][args.SeqNo]; ok {
        *reply = cached
        return
    }
    
    // No cached result - this is a new operation, execute it
    
    var result OpReply

    switch args.Op {
    case GET:
        if val, ok := pb.impl.kv[args.Key]; ok {
            result = OpReply{Err: OK, Value: val}
        } else {
            result = OpReply{Err: ErrNoKey, Value: ""}
        }
        *reply = result
        return  // <-- ADDED: Return early, don't cache GETs
case PUT:
    // If we're backup and this was forwarded from primary, apply locally & cache
    if pb.me == pb.impl.view.Backup && isFromPrimary {
        if _, ok := pb.impl.results[args.Client]; !ok {
            pb.impl.results[args.Client] = make(map[int]OpReply)
        }
        if cached, ok := pb.impl.results[args.Client][args.SeqNo]; ok {
            *reply = cached
            return
        }

        pb.impl.kv[args.Key] = args.Value
        result = OpReply{Err: OK}
        pb.impl.results[args.Client][args.SeqNo] = result
    } else {
        // Primary handling:
        // If there is a backup, forward first; only apply locally after backup ACKs.
        if pb.impl.view.Backup != "" && pb.impl.view.Backup != pb.me {
            fwd := *args
            fwd.Source = pb.me
            var fwdReply OpReply
            ok := call(pb.impl.view.Backup, "PBServer.Operation", &fwd, &fwdReply)
            if !ok || fwdReply.Err != OK {
                // forward failed (or backup rejected) -> tell client to retry / view change
                reply.Err = ErrWrongServer
                return
            }
            // backup applied successfully, now apply locally
            pb.impl.kv[args.Key] = args.Value
            result = OpReply{Err: OK}
            // cache only now
            pb.impl.results[args.Client][args.SeqNo] = result
        } else {
            // no backup: apply locally & cache
            pb.impl.kv[args.Key] = args.Value
            result = OpReply{Err: OK}
            pb.impl.results[args.Client][args.SeqNo] = result
        }
    }

case APPEND:
    if pb.me == pb.impl.view.Backup && isFromPrimary {
        if _, ok := pb.impl.results[args.Client]; !ok {
            pb.impl.results[args.Client] = make(map[int]OpReply)
        }
        if cached, ok := pb.impl.results[args.Client][args.SeqNo]; ok {
            *reply = cached
            return
        }
        pb.impl.kv[args.Key] += args.Value
        result = OpReply{Err: OK}
        pb.impl.results[args.Client][args.SeqNo] = result
    } else {
        if pb.impl.view.Backup != "" && pb.impl.view.Backup != pb.me {
            fwd := *args
            fwd.Source = pb.me
            var fwdReply OpReply
            ok := call(pb.impl.view.Backup, "PBServer.Operation", &fwd, &fwdReply)
            if !ok || fwdReply.Err != OK {
                reply.Err = ErrWrongServer
                return
            }
            // backup acked, now apply locally
            pb.impl.kv[args.Key] += args.Value
            result = OpReply{Err: OK}
            pb.impl.results[args.Client][args.SeqNo] = result
        } else {
            // no backup
            pb.impl.kv[args.Key] += args.Value
            result = OpReply{Err: OK}
            pb.impl.results[args.Client][args.SeqNo] = result
        }
    }

    default:
        result = OpReply{Err: ErrWrongServer}
    }

// Only cache results when handling direct client requests
    if args.Source == "" {
        pb.impl.results[args.Client][args.SeqNo] = result
    }
    
    *reply = result
}


// Push() sends request through channel
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
	req := &pushRequest{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	pb.impl.pushChan <- req
	<-req.done
	return nil
}

// The actual push logic (runs in serializer goroutine)
func (pb *PBServer) pushImpl(args *PushArgs, reply *PushReply) {
    if pb.me != pb.impl.view.Backup {
        reply.Err = ErrWrongServer
        return
    }

    if args.View.Viewnum < pb.impl.view.Viewnum {
        reply.Err = ErrWrongServer
        return
    }

    pb.impl.kv = make(map[string]string)
    for k, v := range args.KVStore {
        pb.impl.kv[k] = v
    }

    pb.impl.results = make(map[string]map[int]OpReply)
	for key, result := range args.OpCache {
		// Parse the key to extract client ID (format: "clientID-seqno")
		parts := strings.Split(key, "-")
		if len(parts) < 2 {
			continue
		}
		// The client ID might contain dashes, so rejoin all but the last part
		client := strings.Join(parts[:len(parts)-1], "-")
		
		if _, ok := pb.impl.results[client]; !ok {
			pb.impl.results[client] = make(map[int]OpReply)
		}
        pb.impl.results[client][result.SeqNo] = result.V
	}

    pb.impl.view = args.View
    reply.Err = OK
}

// tick() sends request through channel
func (pb *PBServer) tick() {
	req := &tickRequest{
		done: make(chan bool),
	}
	pb.impl.tickChan <- req
	<-req.done
}

// The actual tick logic (runs in serializer goroutine)
func (pb *PBServer) tickImpl() {
	if pb.isdead() {
		return
	}

	newView, err := pb.vs.Ping(pb.impl.view.Viewnum)

    if err != nil {
        return
    }
    
    pb.impl.lastPingTime = time.Now()

    var oldView viewservice.View
    if newView.Viewnum != pb.impl.view.Viewnum {
        oldView = pb.impl.view
        pb.impl.view = newView
    }

    if pb.me == pb.impl.view.Primary {
        if pb.impl.view.Backup != "" && pb.impl.view.Backup != oldView.Backup {
			// Copy KV store
			kvCopy := make(map[string]string)
			for k, v := range pb.impl.kv {
				kvCopy[k] = v
			}
			
			// Copy ALL cached results (not just highest)
			opCache := make(map[string]Result)
			for client, clientResults := range pb.impl.results {
				for seq, result := range clientResults {
					// Create unique key for each client+seq combination
					key := client + "-" + strconv.Itoa(seq)
					opCache[key] = Result{SeqNo: seq, V: result}
				}
			}
			
			args := PushArgs{
				KVStore: kvCopy,
				OpCache: opCache,
				View:    pb.impl.view,
			}
			var reply PushReply
			call(pb.impl.view.Backup, "PBServer.Push", &args, &reply)
        }
    }
}