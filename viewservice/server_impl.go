package viewservice

type pingReq struct {
	args  *PingArgs
	reply *PingReply
	done  chan bool
}

type getReq struct {
	args  *GetArgs
	reply *GetReply
	done  chan bool
}

type tickReq struct {
	done chan bool
}

type ViewServerImpl struct {
	currentView View
	lastPing    map[string]int
	serverView  map[string]uint
	tickCount   int
	
	// Channels for serialization
	pingChan chan *pingReq
	getChan  chan *getReq
	tickChan chan *tickReq
}

func (vs *ViewServer) initImpl() {
	vs.impl = ViewServerImpl{
		currentView: View{Viewnum: 0, Primary: "", Backup: ""},
		lastPing:    make(map[string]int),
		serverView:  make(map[string]uint),
		tickCount:   0,
		pingChan:    make(chan *pingReq),
		getChan:     make(chan *getReq),
		tickChan:    make(chan *tickReq),
	}
	
	// Start serializer
	go vs.serializer()
}

func (vs *ViewServer) serializer() {
	for {
		select {
		case req := <-vs.impl.pingChan:
			vs.pingImplInternal(req.args, req.reply)
			req.done <- true
			
		case req := <-vs.impl.getChan:
			vs.getImplInternal(req.args, req.reply)
			req.done <- true
			
		case req := <-vs.impl.tickChan:
			vs.tickInternal()
			req.done <- true
		}
	}
}

func (vs *ViewServer) PingImpl(args *PingArgs, reply *PingReply) error {
	req := &pingReq{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	vs.impl.pingChan <- req
	<-req.done
	return nil
}

func (vs *ViewServer) pingImplInternal(args *PingArgs, reply *PingReply) {
	vs.me = args.Me
	vs.impl.lastPing[args.Me] = vs.impl.tickCount

	if vs.impl.currentView.Primary == "" {
		vs.impl.currentView.Primary = args.Me
		vs.impl.currentView.Viewnum++
	}
	
	if vs.impl.currentView.Primary != "" && vs.impl.currentView.Backup == "" && vs.impl.currentView.Primary != args.Me {
		primaryAcked := (vs.impl.serverView[vs.impl.currentView.Primary] == vs.impl.currentView.Viewnum)
		if primaryAcked {
			vs.impl.currentView.Backup = args.Me
			vs.impl.currentView.Viewnum++
		}
	}

	vs.impl.serverView[args.Me] = args.Viewnum
	reply.View = vs.impl.currentView
}

func (vs *ViewServer) GetImpl(args *GetArgs, reply *GetReply) error {
	req := &getReq{
		args:  args,
		reply: reply,
		done:  make(chan bool),
	}
	vs.impl.getChan <- req
	<-req.done
	return nil
}

func (vs *ViewServer) getImplInternal(args *GetArgs, reply *GetReply) {
	reply.View = vs.impl.currentView
}

func (vs *ViewServer) tick() {
	req := &tickReq{
		done: make(chan bool),
	}
	vs.impl.tickChan <- req
	<-req.done
}

func (vs *ViewServer) tickInternal() {
	vs.impl.tickCount++

	changed_view := false
	for server, lastTick := range vs.impl.lastPing {
		if vs.impl.tickCount-lastTick > DeadPings {
			if vs.impl.serverView[vs.impl.currentView.Primary] == 0 {
				vs.impl.currentView.Primary = ""
				vs.impl.currentView.Backup = ""
				changed_view = true
			}

			if vs.impl.currentView.Primary != "" && (vs.impl.tickCount-vs.impl.lastPing[vs.impl.currentView.Primary] > DeadPings) {
				primaryAcked := vs.impl.serverView[vs.impl.currentView.Primary] == vs.impl.currentView.Viewnum
				if primaryAcked && vs.impl.currentView.Backup != "" {
					vs.impl.currentView.Primary = vs.impl.currentView.Backup
					vs.impl.currentView.Backup = ""
					changed_view = true
				}
			}

			if vs.impl.currentView.Backup == server {
				vs.impl.currentView.Backup = ""
				changed_view = true
			}

			if vs.impl.serverView[server] == 0 {
				if vs.impl.currentView.Primary == server {
					vs.impl.currentView.Primary = ""
					changed_view = true
				}
				if vs.impl.currentView.Backup == server {
					vs.impl.currentView.Backup = ""
					changed_view = true
				}
				continue
			}
		}
	}

	if vs.impl.currentView.Primary == "" {
		for server := range vs.impl.lastPing {
			if vs.impl.tickCount-vs.impl.lastPing[server] > DeadPings {
				continue
			}
			if vs.impl.serverView[server] == 0 {
				continue
			}
			vs.impl.currentView.Primary = server
			changed_view = true
			break
		}
	}

	if vs.impl.currentView.Backup == "" && vs.impl.currentView.Primary != "" {
		primaryAcked := (vs.impl.serverView[vs.impl.currentView.Primary] == vs.impl.currentView.Viewnum)
		if primaryAcked {
			for server := range vs.impl.lastPing {
				if vs.impl.tickCount-vs.impl.lastPing[server] > DeadPings {
					continue
				}
				if server == vs.impl.currentView.Primary {
					continue
				}
				vs.impl.currentView.Backup = server
				changed_view = true
				break
			}
		}
	}

	if changed_view {
		vs.impl.currentView.Viewnum++
	}
}