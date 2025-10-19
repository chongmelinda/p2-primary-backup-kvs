package pbservice
import (
	"umich.edu/eecs491/proj2/viewservice"
)
//
// additions to PBServer state.
//
type PBServerImpl struct {
    kv      map[string]string
    results map[string]map[int]OpReply
    view    viewservice.View
}

//
// your pb.impl.* initializations here.
//
func (pb *PBServer) initImpl() {
	pb.impl.kv = make(map[string]string)
	pb.impl.results = make(map[string]map[int]OpReply)
}

//
// server Operation() RPC handler.
//
func (pb *PBServer) Operation(args OpArgs, reply *OpReply) error {
    // Only primary should accept Operation calls from clients
    if pb.me != pb.impl.view.Primary {
        reply.Err = ErrWrongServer
        return nil
    }

    // Initialize client cache if needed
    if _, ok := pb.impl.results[args.Client]; !ok {
        pb.impl.results[args.Client] = make(map[int]OpReply)
    }

    // Check for duplicate requests
    if cached, ok := pb.impl.results[args.Client][args.SeqNo]; ok {
        *reply = cached // Return previous reply directly
        return nil
    }

    // Execute the operation
    var result OpReply

    switch args.Op {
    case GET:
        value, ok := pb.impl.kv[args.Key]
        if ok {
            result = OpReply{Err: OK, Value: value}
        } else {
            result = OpReply{Err: ErrNoKey, Value: ""}
        }

    case PUT:
        pb.impl.kv[args.Key] = args.Value
        result = OpReply{Err: OK}

    case APPEND:
        pb.impl.kv[args.Key] += args.Value
        result = OpReply{Err: OK}

    default:
        result = OpReply{Err: ErrWrongServer}
    }

    // Cache result for at-most-once semantics
    pb.impl.results[args.Client][args.SeqNo] = result

    // Try to push update to backup
    if pb.impl.view.Backup != "" && pb.impl.view.Backup != pb.me {
        pushArgs := PushArgs{
            KVStore: pb.impl.kv,
            OpCache: make(map[string]Result),
            View:    pb.impl.view,
        }
        // Fill OpCache with current client results
        for client, ops := range pb.impl.results {
            lastSeq := 0
            var lastReply OpReply
            for seq, r := range ops {
                if seq > lastSeq {
                    lastSeq, lastReply = seq, r
                }
            }
            pushArgs.OpCache[client] = Result{SeqNo: lastSeq, V: lastReply}
        }

        var pushReply PushReply
        call(pb.impl.view.Backup, "PBServer.Push", &pushArgs, &pushReply)
    }

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

//     // 4. Compare the old view vs the new view.
    oldView := pb.impl.view
    pb.impl.view = newView

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

