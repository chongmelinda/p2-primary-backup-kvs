package viewservice

//
// additions to ViewServer state.
//
type ViewServerImpl struct {
}

//
// your vs.impl.* initializations here.
//
func (vs *ViewServer) initImpl() {
}

//
// Ping() RPC handler implementation
//
func (vs *ViewServer) PingImpl(args *PingArgs, reply *PingReply) error {
	return nil
}

//
// Get() RPC handler implementation
//
func (vs *ViewServer) GetImpl(args *GetArgs, reply *GetReply) error {
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
}
