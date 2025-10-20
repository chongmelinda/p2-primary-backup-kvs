package pbservice
import (
	"umich.edu/eecs491/proj2/viewservice"
    "time"
)
//
// additions to PBServer state.
//
type PBServerImpl struct {
    kv      map[string]string
    results map[string]map[int]OpReply
    view    viewservice.View
    lastPingTime time.Time
}

//
// your pb.impl.* initializations here.
//
func (pb *PBServer) initImpl() {
	pb.impl.kv = make(map[string]string)
	pb.impl.results = make(map[string]map[int]OpReply)
    pb.impl.lastPingTime = time.Now()
}

//
// server Operation() RPC handler.
//
//
// server Operation() RPC handler.
//
func (pb *PBServer) Operation(args OpArgs, reply *OpReply) error {
    // 0) If server is dead, don't process
    if pb.isdead() {
        reply.Err = ErrWrongServer
        return nil
    }
    // Check if our cached view is fresh enough
    // If we haven't successfully pinged recently, we might be partitioned
    timeSinceLastPing := time.Since(pb.impl.lastPingTime)
    if timeSinceLastPing > viewservice.PingInterval * viewservice.DeadPings {
        // Our view is too stale - we might be partitioned
        reply.Err = ErrWrongServer
        return nil
    }
    // 1) Refresh our view by pinging the viewservice.
    //    If we cannot contact the viewservice, we must not serve client requests:
    //    treat ourselves as not-primary (to avoid serving stale data).


    // 2) If this call is from a client (args.Source == ""), require we're the primary.
    //    If this call is forwarded from the primary, the backup should accept it.
    isFromPrimary := (args.Source == pb.impl.view.Primary)
    if args.Source == "" {
        // client call: require we are primary
        if pb.me != pb.impl.view.Primary {
            reply.Err = ErrWrongServer
            return nil
        }
    } else {
        // forwarded call: require it came from the current primary, and we are the backup
        // OR it's possible the forwarded call reached a server that is primary (rare) - handle conservatively.
        if !(pb.me == pb.impl.view.Backup && isFromPrimary) && !(pb.me == pb.impl.view.Primary && args.Source == pb.impl.view.Primary) {
            // not an expected forwarded call and we are not the primary -> refuse
            if pb.me != pb.impl.view.Primary {
                reply.Err = ErrWrongServer
                return nil
            }
        }
    }

    // 3) Ensure client cache initialized
    if _, ok := pb.impl.results[args.Client]; !ok {
        pb.impl.results[args.Client] = make(map[int]OpReply)
    }

    // 4) Deduplicate: if we already handled this client/seq, return cached reply
    if cached, ok := pb.impl.results[args.Client][args.SeqNo]; ok {
        *reply = cached
        return nil
    }

    // 5) Apply operation
    var result OpReply

    switch args.Op {
    case GET:
        // GET does not modify DB. Only primary should serve GETs (handled above).
        if val, ok := pb.impl.kv[args.Key]; ok {
            result = OpReply{Err: OK, Value: val}
        } else {
            result = OpReply{Err: ErrNoKey, Value: ""}
        }

    case PUT:
        // If this is a forwarded write (Source == primary), backup should apply locally and return OK.
        // If this is a client call to primary, primary must apply locally then forward to backup.
        if pb.me == pb.impl.view.Backup && isFromPrimary {
            // backup applying forwarded Put
            pb.impl.kv[args.Key] = args.Value
            result = OpReply{Err: OK}
        } else {
            // primary applying client Put
            pb.impl.kv[args.Key] = args.Value
            // now forward to backup if present
            if pb.impl.view.Backup != "" && pb.impl.view.Backup != pb.me {
                fwd := args
                fwd.Source = pb.me
                var fwdReply OpReply
                ok := call(pb.impl.view.Backup, "PBServer.Operation", &fwd, &fwdReply)
                if !ok || fwdReply.Err != OK {
                    // If forwarding fails, don't commit client success; tell client to retry / view may change
                    reply.Err = ErrWrongServer
                    return nil
                }
            }
            result = OpReply{Err: OK}
        }

    case APPEND:
        if pb.me == pb.impl.view.Backup && isFromPrimary {
            // backup applies
            pb.impl.kv[args.Key] += args.Value
            result = OpReply{Err: OK}
        } else {
            // primary applies, then forward
            pb.impl.kv[args.Key] += args.Value
            if pb.impl.view.Backup != "" && pb.impl.view.Backup != pb.me {
                fwd := args
                fwd.Source = pb.me
                var fwdReply OpReply
                ok := call(pb.impl.view.Backup, "PBServer.Operation", &fwd, &fwdReply)
                if !ok || fwdReply.Err != OK {
                    reply.Err = ErrWrongServer
                    return nil
                }
            }
            result = OpReply{Err: OK}
        }

    default:
        result = OpReply{Err: ErrWrongServer}
    }

    // 6) Cache result for at-most-once semantics (for client calls)
    //    (If it's a forwarded call, it still sets same cache so duplicates won't reapply.)
    pb.impl.results[args.Client][args.SeqNo] = result
    *reply = result
    return nil
}


//
// server Push() RPC handler
//
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
    // Only backup should accept a Push
    if pb.me != pb.impl.view.Backup {
        reply.Err = ErrWrongServer
        return nil
    }

    // If the push is for an outdated view, ignore it
    if args.View.Viewnum < pb.impl.view.Viewnum {
        reply.Err = ErrWrongServer
        return nil
    }

    // Replace local state with the primary’s data
    pb.impl.kv = make(map[string]string)
    for k, v := range args.KVStore {
        pb.impl.kv[k] = v
    }

    pb.impl.results = make(map[string]map[int]OpReply)
	for client, result := range args.OpCache {
		if _, ok := pb.impl.results[client]; !ok {
			pb.impl.results[client] = make(map[int]OpReply)
		}
    pb.impl.results[client][result.SeqNo] = result.V
	}

    // Update our known view to match what primary sent
    pb.impl.view = args.View

    reply.Err = OK
    return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup. (see viewservice/common.go)
//
func (pb *PBServer) tick() {
// 1. If this server is dead, stop doing anything.
	if pb.isdead() {
		return
	}

//     // 2. Ping the view service with your own address and current view number.
//     //    viewservice.Ping() returns the *latest known view* from the viewservice.
	newView, err := pb.vs.Ping(pb.impl.view.Viewnum)

// 3. Handle RPC failure to viewservice gracefully.
    if err != nil {
        // Just skip this round; maybe try again later.
        return
    }
    // Update timestamp on successful ping
    pb.impl.lastPingTime = time.Now()

//     // 4. Compare the old view vs the new view.
var oldView viewservice.View
    if newView.Viewnum != pb.impl.view.Viewnum {
        oldView = pb.impl.view
        pb.impl.view = newView
        // push to new backup if needed
    }

//     // 5. If you are the *primary* in the new view:
    if pb.me == pb.impl.view.Primary {
        // a) If the backup changed (i.e., new backup joined):
        if pb.impl.view.Backup != "" && pb.impl.view.Backup != oldView.Backup {
            // Send the complete key/value store + deduplication map to the new backup.
			args := PushArgs{
				KVStore: pb.impl.kv,
				OpCache: make(map[string]Result),
				View:    pb.impl.view,
			}
			var reply PushReply
			ok := call(pb.impl.view.Backup, "PBServer.Push", &args, &reply)
			if !ok || reply.Err != OK {
				// Could not sync backup, maybe try again next tick.
			}
        }
    }

//     // 6. If you are the backup, no special action usually needed.
//     //    Just update your view info so you know your role.

//     // 7. If you are neither primary nor backup (idle), just sit tight.
}

