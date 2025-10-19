package viewservice

// import (
// 	"log" // Import the standard log package
// )

// additions to ViewServer state.
type ViewServerImpl struct {
	currentView View            // current view (Primary, Backup, Viewnum)
	lastPing    map[string]int  // last tick number when each server Pings
	serverView  map[string]uint // last view number each server reported
	tickCount   int             // number of ticks elapsed
}

// your vs.impl.* initializations here.
func (vs *ViewServer) initImpl() {
	vs.impl = ViewServerImpl{
		currentView: View{Viewnum: 0, Primary: "", Backup: ""},
		lastPing:    make(map[string]int),
		serverView:  make(map[string]uint),
		tickCount:   0,
	}
}

// Ping() RPC handler implementation
func (vs *ViewServer) PingImpl(args *PingArgs, reply *PingReply) error {
	// TODO 1: Extract server name (args.Me) and view number (args.Viewnum).
	vs.me = args.Me
	// TODO 2: Update the "last heard from" map for this server
	//         (so Tick can later detect timeouts).
	vs.impl.lastPing[args.Me] = vs.impl.tickCount
// log.Printf("PING: %s with viewnum %d (current Primary=%s, Backup=%s, Viewnum=%d)\n",
// 		args.Me, args.Viewnum, vs.impl.currentView.Primary, vs.impl.currentView.Backup, vs.impl.currentView.Viewnum)

	// TODO 3: If there is no primary yet, make this server the primary
	//         and increment the view number.
	if vs.impl.currentView.Primary == "" {
		vs.impl.currentView.Primary = args.Me
		vs.impl.currentView.Viewnum++
		// log.Printf("PING: Assigned %s as primary\n", args.Me)
	}
	// TODO 4: If there is a primary but no backup yet,
	//         and this server is not the primary,
	//         make this server the backup and increment the view number.
	if vs.impl.currentView.Primary != "" && vs.impl.currentView.Backup == "" && vs.impl.currentView.Primary != args.Me {
		primaryAcked := (vs.impl.serverView[vs.impl.currentView.Primary] == vs.impl.currentView.Viewnum)
		// log.Printf("PING: Backup check - primaryAcked=%v (serverView[%s]=%d, Viewnum=%d)\n",
		// 	primaryAcked, vs.impl.currentView.Primary, vs.impl.serverView[vs.impl.currentView.Primary], vs.impl.currentView.Viewnum)
		if primaryAcked {
			vs.impl.currentView.Backup = args.Me
			vs.impl.currentView.Viewnum++
			// log.Printf("PING: Assigned %s as backup\n", args.Me)
		}
	}

	// TODO 5: Record what view number this server believes it is on
	//         (so we know when the primary has acknowledged the latest view).
	vs.impl.serverView[args.Me] = args.Viewnum
	// TODO 6: Put the current view into the reply so the server
	//         knows the official view.
	reply.View = vs.impl.currentView

	// TODO 7: Return nil error.
	return nil
}

// Get() RPC handler implementation
func (vs *ViewServer) GetImpl(args *GetArgs, reply *GetReply) error {
	reply.View = vs.impl.currentView
	return nil
}

func (vs *ViewServer) tick() {
	// TODO 1: For each known server, check if (current tick - lastHeard[server]) > DeadPings.
	//         If yes, mark the server as dead.
	vs.impl.tickCount++

	changed_view := false
	for server, lastTick := range vs.impl.lastPing {
		if vs.impl.tickCount-lastTick > DeadPings {
			// log.Printf("  Loop: server=%s, isDead=%v, isRestarted=%v\n",
			// 	server,
			// 	vs.impl.tickCount-lastTick > DeadPings,
			// 	vs.impl.serverView[server] == 0)
			//if we're in here, it's dead
			// TODO 2: If the primary is dead:
			//           - Only promote the backup to primary *if* the old primary had ACKed its view. (and do other 3 items)
			//           - Increment the view number.
			//           - Set backup = "" (will be filled later by idle servers).
			if vs.impl.serverView[vs.impl.currentView.Primary] == 0 {
				// log.Printf("TICK: Removing restarted primary %s\n", vs.impl.currentView.Primary)
				vs.impl.currentView.Primary = ""
				vs.impl.currentView.Backup = ""
				changed_view = true
				// log.Printf("TICK: After removing primary - Primary=%s, Backup=%s\n", vs.impl.currentView.Primary, vs.impl.currentView.Backup)
				// do NOT promote backup yet
			}

			if vs.impl.currentView.Primary != "" && (vs.impl.tickCount-vs.impl.lastPing[vs.impl.currentView.Primary] > DeadPings) {
				// primary is dead or restarted
				primaryAcked := vs.impl.serverView[vs.impl.currentView.Primary] == vs.impl.currentView.Viewnum
				if primaryAcked && vs.impl.currentView.Backup != "" {
					// log.Printf("TICK: Promoting backup %s to primary\n", vs.impl.currentView.Backup)
					vs.impl.currentView.Primary = vs.impl.currentView.Backup
					vs.impl.currentView.Backup = ""
					changed_view = true

				}
			}

			if vs.impl.currentView.Backup == server {
				// TODO 3: If the backup is dead:
				//           - Remove it.
				//           - Increment the view number.
				// log.Printf("TICK: Removing dead backup %s\n", server)
				vs.impl.currentView.Backup = ""
				changed_view = true
			}

			if vs.impl.serverView[server] == 0 {
				// TODO 5: If a server restarted (detected because it Pings with viewnum=0):
				//           - It cannot continue as primary/backup.
				//           - Remove it from those roles.
				//           - Treat it as an idle server (eligible for backup in future views).
				// log.Printf("TICK: Removing restarted primary %s\n", server)
				if vs.impl.currentView.Primary == server {
					vs.impl.currentView.Primary = ""
					changed_view = true
				}
				if vs.impl.currentView.Backup == server {
					// log.Printf("TICK: Removing restarted backup %s\n", server)
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
			// log.Printf("TICK: Assigning %s as new primary\n", server)
			vs.impl.currentView.Primary = server
			changed_view = true

			break
		}
	}
	// log.Printf("TICK BEFORE BACKUP: Primary=%s, Backup=%s\n", 
	// 	vs.impl.currentView.Primary, vs.impl.currentView.Backup)

	if vs.impl.currentView.Backup == "" && vs.impl.currentView.Primary != "" {
		primaryAcked := (vs.impl.serverView[vs.impl.currentView.Primary] == vs.impl.currentView.Viewnum)
			// 	log.Printf("TICK BACKUP CHECK: primaryAcked=%v, serverView[%s]=%d, Viewnum=%d\n",
			// primaryAcked, vs.impl.currentView.Primary, vs.impl.serverView[vs.impl.currentView.Primary], vs.impl.currentView.Viewnum)
		if primaryAcked {
			for server := range vs.impl.lastPing {
				if vs.impl.tickCount-vs.impl.lastPing[server] > DeadPings {
					continue
				}
				if server == vs.impl.currentView.Primary {
					// log.Printf("TICK BACKUP: Skipping primary %s\n", server)
					continue
				}

				// log.Printf("TICK BACKUP: Assigning %s as backup\n", server)
				// log.Printf("TICK: Assigning %s as new backup\n", server)
				vs.impl.currentView.Backup = server
				changed_view = true
				break
			}
		}
	}

	if changed_view {
		// log.Printf("TICK: Incrementing viewnum to %d\n", vs.impl.currentView.Viewnum+1)
		vs.impl.currentView.Viewnum++
	}
	// log.Printf("TICK END: Primary=%s, Backup=%s, Viewnum=%d\n", vs.impl.currentView.Primary, vs.impl.currentView.Backup, vs.impl.currentView.Viewnum)
	// TODO 6: Make sure you don’t promote backup → primary
	//         until the primary has acknowledged the current view number.

	// TODO 7: Done.
}
