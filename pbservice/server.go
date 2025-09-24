package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"umich.edu/eecs491/proj2/viewservice"
)

type PBServer struct {
	//mu         sync.Mutex
	l          net.Listener
	dead       <-chan interface {} // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	impl       PBServerImpl
}

// tell the server to shut itself down.
// term should be the same channel as pb.dead
func (pb *PBServer) kill(term chan interface{}) {
	fmt.Printf("Killing pbserver %v\n", pb.me)
	close(term)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	select {
	case <-pb.dead:
		return true
	default:
		return false
	}
}

func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string, term <-chan interface{}) *PBServer {
	pb := new(PBServer)
	pb.dead = term
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.initImpl()

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			// We may have been killed while waiting for a new request
			if pb.isdead() {
				if err == nil {
					conn.Close()
				}
				return
			}
			// If this accept resulted in an error, log it and try again
			if err != nil {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				continue
			}
			// We are not dead, and have a valid connection
			// Did an unreliable network  cause the request to fail?
			if pb.isunreliable() && (rand.Int63()%1000) < 100 {
				// yes: discard it and do not process
				conn.Close()
				continue
			}
			// Will an unreliable network cause the response to fail?
			if pb.isunreliable() && (rand.Int63()%1000) < 200 {
				// yes: close file descriptor over which response will be sent
				c1 := conn.(*net.UnixConn)
				f, _ := c1.File()
				err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
				if err != nil {
					fmt.Printf("shutdown: %v\n", err)
				}
			}
			go rpcs.ServeConn(conn)
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
