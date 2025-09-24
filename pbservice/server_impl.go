package pbservice

//
// additions to PBServer state.
//
type PBServerImpl struct {
}

//
// your pb.impl.* initializations here.
//
func (pb *PBServer) initImpl() {
}

//
// server Operation() RPC handler.
//
func (pb *PBServer) Operation(args OpArgs, reply *OpReply) error {
	return nil
}

//
// server Push() RPC handler
//
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
}

