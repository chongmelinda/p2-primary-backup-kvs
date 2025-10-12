package viewservice

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
	vs.impl.serverView[args.Me] = args.Viewnum
	// TODO 2: Update the "last heard from" map for this server
	//         (so Tick can later detect timeouts).
	vs.impl.lastPing[args.Me] = vs.impl.tickCount

	// TODO 3: If there is no primary yet, make this server the primary
	//         and increment the view number.
	if vs.impl.currentView.Primary == "" {
		vs.impl.currentView.Primary = args.Me
		vs.impl.currentView.Viewnum++
	}
	// TODO 4: If there is a primary but no backup yet,
	//         and this server is not the primary,
	//         make this server the backup and increment the view number.
	if vs.impl.currentView.Primary != "" && vs.impl.currentView.Backup == "" && vs.impl.currentView.Primary != args.Me {
		vs.impl.currentView.Backup = args.Me
		vs.impl.currentView.Viewnum++
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
	// Check primary/backup separately outside of any loop
	// Only increment view number when you actually change the view
	// Check if primary has acked before promoting backup (primaryAcked variable)
	// Handle restart and death as separate conditions

	// primary := vs.impl.currentView.Primary
	// backup := vs.impl.currentView.Backup
	for server, lastTick := range vs.impl.lastPing {
		changed_view := false
		if vs.impl.tickCount-lastTick > DeadPings {
			//if we're in here, it's dead
			// TODO 2: If the primary is dead:
			//           - Only promote the backup to primary *if* the old primary had ACKed its view. (and do other 3 items)
			//           - Increment the view number.
			//           - Set backup = "" (will be filled later by idle servers).
			if vs.impl.currentView.Primary != "" && (vs.impl.tickCount-vs.impl.lastPing[vs.impl.currentView.Primary] > DeadPings || vs.impl.serverView[vs.impl.currentView.Primary] == 0) {
				// primary is dead or restarted
				if vs.impl.currentView.Backup != "" {
					vs.impl.currentView.Primary = vs.impl.currentView.Backup
					vs.impl.currentView.Backup = ""
				} else {
					vs.impl.currentView.Primary = ""
				}
				changed_view = true
			}

			if vs.impl.currentView.Backup == server {
				// TODO 3: If the backup is dead:
				//           - Remove it.
				//           - Increment the view number.
				vs.impl.currentView.Backup = ""
				changed_view = true
			}

			if vs.impl.currentView.Backup == "" {
				// TODO 4: If there is no backup:
				//           - Pick one of the idle servers (not primary, not already backup).
				//           - Assign it as backup.
				//           - Increment the view number.
				for server := range vs.impl.lastPing {
					// no dead servers
					if vs.impl.tickCount-vs.impl.lastPing[server] > DeadPings {
						continue
					}

					// no primary/backup
					if server == vs.impl.currentView.Primary || server == vs.impl.currentView.Backup {
						continue
					}

					//idle server found
					vs.impl.currentView.Backup = server
					changed_view = true
					break
				}
			}

			if vs.impl.serverView[server] == 0 {
				// TODO 5: If a server restarted (detected because it Pings with viewnum=0):
				//           - It cannot continue as primary/backup.
				//           - Remove it from those roles.
				//           - Treat it as an idle server (eligible for backup in future views).
				if vs.impl.currentView.Primary == server {
					vs.impl.currentView.Primary = ""
					changed_view = true
				}
				if vs.impl.currentView.Backup == server {
					vs.impl.currentView.Backup = ""
					changed_view = true
				}
			}

			if changed_view {
				vs.impl.currentView.Viewnum++
			}
		}
	}

	// TODO 6: Make sure you don’t promote backup → primary
	//         until the primary has acknowledged the current view number.

	// TODO 7: Done.
}
