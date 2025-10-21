package pbservice
import (
	"umich.edu/eecs491/proj2/viewservice"
    "time"
    "strconv"
    "strings"
)

type opReq struct {
	args  OpArgs
	reply *OpReply
	done  chan bool
}

type pushReq struct {
	args  PushArgs
	reply *PushReply
	done  chan bool
}

type tickReq struct {
	done chan bool
}

type PBServerImpl struct {
    kv           map[string]string
    results      map[string]map[int]OpReply
    view         viewservice.View
    lastpingtime time.Time
    
    // Channels for serialization
    op_chan    chan *opReq
    push_chan  chan *pushReq
    tick_chan  chan *tickReq
}

func (pb *PBServer) initImpl() {
	pb.impl.kv = make(map[string]string)
	pb.impl.results = make(map[string]map[int]OpReply)
    pb.impl.lastpingtime = time.Now()
    //this is to make sure we're not overpinging

    // initialize chans
    pb.impl.op_chan = make(chan *opReq)
    pb.impl.push_chan = make(chan *pushReq)
    pb.impl.tick_chan = make(chan *tickReq)
    
    // start run_channels goroutine
    go pb.run_channels()
}

// processes all state modifications
func (pb *PBServer) run_channels() {
	for {
		select {
		case req := <-pb.impl.op_chan:
			pb.operationImpl(&req.args, req.reply)
			req.done <- true
			
		case req := <-pb.impl.push_chan:
			pb.pushImpl(&req.args, req.reply)
			req.done <- true
			
		case req := <-pb.impl.tick_chan:
			pb.tickImpl()
			req.done <- true
		}
	}
}

// Operation() sends the req through the channel instead of doing ti all by itself
func (pb *PBServer) Operation(args OpArgs, reply *OpReply) error {
	req := &opReq{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	pb.impl.op_chan <- req
	<-req.done
	return nil
}

// what operation() does (runs in run_channels goroutine)
func (pb *PBServer) operationImpl(args *OpArgs, reply *OpReply) {
    if pb.isdead() {
        reply.Err = ErrWrongServer
        return
    }
    //if dead dont do anything
    
    time_since_last_ping := time.Since(pb.impl.lastpingtime)
    if time_since_last_ping > viewservice.PingInterval * viewservice.DeadPings {
        reply.Err = ErrWrongServer
        return
    }
    //if too long, it's dead

    from_primary := (args.Source == pb.impl.view.Primary) //if the request was from primary
    if args.Source == "" {
        if pb.me != pb.impl.view.Primary {
            reply.Err = ErrWrongServer
            return
        } // if no source id and it is not primary, error
    } else {
        if !(pb.me == pb.impl.view.Backup && from_primary) && !(pb.me == pb.impl.view.Primary && args.Source == pb.impl.view.Primary) {
            if pb.me != pb.impl.view.Primary {
                reply.Err = ErrWrongServer
                return
            }
        }// if not backup && source is from primary && not primary && source is from primary, err
    }

    _, ok := pb.impl.results[args.Client]
    if !ok {
        pb.impl.results[args.Client] = make(map[int]OpReply)
    }
    //now put it all into results

    
    cached, ok := pb.impl.results[args.Client][args.SeqNo]
    if ok {
        *reply = cached
        return
    }
    // check for the seq number match

    // no cache, then new operation
    
    var result OpReply

    switch args.Op {
    case GET:
        val, ok := pb.impl.kv[args.Key]
        if ok {
            result = OpReply{Err: OK, Value: val}
        } else {
            result = OpReply{Err: ErrNoKey, Value: ""}
        }
        *reply = result
        return  //DO NOT CACHE GETS OH MY GOD

    case PUT:
        // if we're backup and forwarded from primary, apply locally and then cache
        if pb.me == pb.impl.view.Backup && from_primary {
            _, ok := pb.impl.results[args.Client]
            if !ok {
                pb.impl.results[args.Client] = make(map[int]OpReply)
            }
            cached, ok := pb.impl.results[args.Client][args.SeqNo]
            if ok {
                *reply = cached
                return
            }//FOR AVOIDING DOUBLE APPENDS, if cached, return the cached

            pb.impl.kv[args.Key] = args.Value
            result = OpReply{Err: OK}
            pb.impl.results[args.Client][args.SeqNo] = result
        } else {
            // primary: if there is a backup we need to forward first and only apply locally after backup ack done
            if pb.impl.view.Backup != "" && pb.impl.view.Backup != pb.me {
                fwd := *args
                fwd.Source = pb.me
                var fwdReply OpReply
                ok := call(pb.impl.view.Backup, "PBServer.Operation", &fwd, &fwdReply) //if still alive
                if !ok || fwdReply.Err != OK {
                    // forward must have failed so tell the client to retry or view change
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
        if pb.me == pb.impl.view.Backup && from_primary { //if from primary and we're backup
            _, ok := pb.impl.results[args.Client]
            if !ok {
                pb.impl.results[args.Client] = make(map[int]OpReply)
            }//create cache
            cached, ok := pb.impl.results[args.Client][args.SeqNo]
            if ok {
                *reply = cached
                return
            } //FOR AVOIDING DOUBLE APPENDS, we return the cached result if true
            pb.impl.kv[args.Key] += args.Value
            result = OpReply{Err: OK}
            pb.impl.results[args.Client][args.SeqNo] = result
        } else {
            if pb.impl.view.Backup != "" && pb.impl.view.Backup != pb.me {
                fwd := *args
                fwd.Source = pb.me
                var fwdReply OpReply
                ok := call(pb.impl.view.Backup, "PBServer.Operation", &fwd, &fwdReply) //if still alive
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

// only cache the results when doing the direct client requests
    if args.Source == "" {
        pb.impl.results[args.Client][args.SeqNo] = result
    }
    
    *reply = result
}


// push() sends request through channel
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
	req := &pushReq{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	pb.impl.push_chan <- req
	<-req.done
	return nil
}

// actual push() logic (runs in goroutine)
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
    //push it

    pb.impl.results = make(map[string]map[int]OpReply)
	for key, result := range args.OpCache {
		// string parsing the key to get client ID, in the form clientID-seqno
		parts := strings.Split(key, "-")
		if len(parts) < 2 {
			continue
		}
		//client ID might have dashes, rejoin all but the last part

		client := strings.Join(parts[:len(parts)-1], "-")
		_, ok := pb.impl.results[client]
		if !ok {
			pb.impl.results[client] = make(map[int]OpReply)
		}
        pb.impl.results[client][result.SeqNo] = result.V
	}

    pb.impl.view = args.View
    reply.Err = OK
}

// tick() sends request through channel
func (pb *PBServer) tick() {
	req := &tickReq{
		done: make(chan bool),
	}
	pb.impl.tick_chan <- req
	<-req.done
}

// tick logic (runs in run_channels goroutine)
func (pb *PBServer) tickImpl() {
	if pb.isdead() {
		return
	}
    //if its dead just return

	new_view, err := pb.vs.Ping(pb.impl.view.Viewnum)

    if err != nil {
        return
    }
    //if error, return
    pb.impl.lastpingtime = time.Now()

    var old_view viewservice.View
    if new_view.Viewnum != pb.impl.view.Viewnum {
        old_view = pb.impl.view
        pb.impl.view = new_view
    }

    if pb.me == pb.impl.view.Primary {
        if pb.impl.view.Backup != "" && pb.impl.view.Backup != old_view.Backup {
			// make a copy of kv
			kvCopy := make(map[string]string)
			for k, v := range pb.impl.kv {
				kvCopy[k] = v
			}
			
			// copy EVERY cached result
			opCache := make(map[string]Result)
			for client, clientResults := range pb.impl.results {
				for seq, result := range clientResults {
					// make unique key for each client+seq combination
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
			call(pb.impl.view.Backup, "PBServer.Push", &args, &reply) //if still alive
        }
    }
}