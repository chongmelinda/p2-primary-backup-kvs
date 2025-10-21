package viewservice

type pingReq struct {
	args  *PingArgs
	reply *PingReply
	done  chan bool
}
// for the ping channel

type getReq struct {
	args  *GetArgs
	reply *GetReply
	done  chan bool
}
// get channel

type tickReq struct {
	done chan bool
}
// tick channel

type ViewServerImpl struct {
	cur_view View //current view
	last_ping    map[string]int
	server_view  map[string]uint
	tick_count   int
	
	// Channels for serialization
	ping_chan chan *pingReq
	get_chan  chan *getReq
	tick_chan chan *tickReq
}

func (vs *ViewServer) initImpl() {
	vs.impl = ViewServerImpl{
		cur_view: View{Viewnum: 0, Primary: "", Backup: ""},
		last_ping:    make(map[string]int),
		server_view:  make(map[string]uint),
		tick_count:   0,
		ping_chan:    make(chan *pingReq),
		get_chan:     make(chan *getReq),
		tick_chan:    make(chan *tickReq),
	}
	
	// Start run_channels
	go vs.run_channels()
}

func (vs *ViewServer) run_channels() {
	//everything is run up here
	for {
		select {
		case req := <-vs.impl.ping_chan:
			vs.ping_impl_internal(req.args, req.reply)
			req.done <- true
			// finish up ping_chan
		case req := <-vs.impl.get_chan:
			vs.get_impl_internal(req.reply)
			req.done <- true
			//finish up get_chan
		case req := <-vs.impl.tick_chan:
			vs.tick_internal()
			req.done <- true
			//finish up tick_chan
		}
	}
}

func (vs *ViewServer) PingImpl(args *PingArgs, reply *PingReply) error {
	req := &pingReq{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	vs.impl.ping_chan <- req
	<-req.done
	return nil
	//for the channel
}

func (vs *ViewServer) ping_impl_internal(args *PingArgs, reply *PingReply) {
	vs.me = args.Me
	vs.impl.last_ping[args.Me] = vs.impl.tick_count
	//update tick_count
	if vs.impl.cur_view.Primary == "" {
		vs.impl.cur_view.Primary = args.Me
		vs.impl.cur_view.Viewnum++
	}
	//change view
	if vs.impl.cur_view.Primary != "" && vs.impl.cur_view.Backup == "" && vs.impl.cur_view.Primary != args.Me {
		primary_ack := (vs.impl.server_view[vs.impl.cur_view.Primary] == vs.impl.cur_view.Viewnum)
		if primary_ack {
			vs.impl.cur_view.Backup = args.Me
			vs.impl.cur_view.Viewnum++
		}
	}
	//check for idle server and ack, then assign backup
	vs.impl.server_view[args.Me] = args.Viewnum
	reply.View = vs.impl.cur_view
	//update views
}

func (vs *ViewServer) GetImpl(args *GetArgs, reply *GetReply) error {
	req := &getReq{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	vs.impl.get_chan <- req
	<-req.done
	return nil
	//for the get_channel
}

func (vs *ViewServer) get_impl_internal(reply *GetReply) {
	reply.View = vs.impl.cur_view
	//get current view
}

func (vs *ViewServer) tick() {
	req := &tickReq{
		done: make(chan bool),
	}
	vs.impl.tick_chan <- req
	<-req.done
	//for the tick channel
}

func (vs *ViewServer) tick_internal() {
	vs.impl.tick_count++

	changed_view := false
	for server, lastTick := range vs.impl.last_ping {
		if vs.impl.tick_count-lastTick > DeadPings {
			if vs.impl.server_view[vs.impl.cur_view.Primary] == 0 {
				vs.impl.cur_view.Primary = ""
				vs.impl.cur_view.Backup = ""
				changed_view = true
			}
			//if view restarted, reset primary and backup
			if vs.impl.cur_view.Primary != "" && (vs.impl.tick_count-vs.impl.last_ping[vs.impl.cur_view.Primary] > DeadPings) {
				primary_ack := vs.impl.server_view[vs.impl.cur_view.Primary] == vs.impl.cur_view.Viewnum
				if primary_ack && vs.impl.cur_view.Backup != "" {
					vs.impl.cur_view.Primary = vs.impl.cur_view.Backup
					vs.impl.cur_view.Backup = ""
					changed_view = true
				}
			}
			//check for nonblank primary and if it's dead, then make backup new primary
			if vs.impl.cur_view.Backup == server {
				vs.impl.cur_view.Backup = ""
				changed_view = true
			}
			//if backup dead, clear it
			if vs.impl.server_view[server] == 0 {
				if vs.impl.cur_view.Primary == server {
					vs.impl.cur_view.Primary = ""
					changed_view = true
				}
				if vs.impl.cur_view.Backup == server {
					vs.impl.cur_view.Backup = ""
					changed_view = true
				}
				continue
			}
			//if view 0, means reset, so either clear primary or backup
		}
	}

	if vs.impl.cur_view.Primary == "" {
		for server := range vs.impl.last_ping {
			if vs.impl.tick_count-vs.impl.last_ping[server] > DeadPings {
				continue
			}
			if vs.impl.server_view[server] == 0 {
				continue
			}
			vs.impl.cur_view.Primary = server
			changed_view = true
			break
		}
		//only update primary with valid servers
	}

	if vs.impl.cur_view.Backup == "" && vs.impl.cur_view.Primary != "" {
		primary_ack := (vs.impl.server_view[vs.impl.cur_view.Primary] == vs.impl.cur_view.Viewnum)
		if primary_ack {
			for server := range vs.impl.last_ping {
				if vs.impl.tick_count-vs.impl.last_ping[server] > DeadPings {
					continue
				}
				if server == vs.impl.cur_view.Primary {
					continue
				}
				vs.impl.cur_view.Backup = server
				changed_view = true
				break
			}
		}
		//update backup with valid servers and not currently primary
	}

	if changed_view {
		vs.impl.cur_view.Viewnum++
	}
	//update viewnum
}