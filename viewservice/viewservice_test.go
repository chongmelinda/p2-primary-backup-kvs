package viewservice

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func compareViews(view View, p string, b string, n uint) {
	fmt.Printf("wanted: View{primary: \"%v\", Backup: \"%v\", Viewnum: %v}\n", p, b, n)
	fmt.Printf("got:    View{primary: \"%v\", Backup: \"%v\", Viewnum: %v}\n", view.Primary, view.Backup, view.Viewnum)
}

func check(t *testing.T, ck *Clerk, p string, b string, n uint) {
	view, _ := ck.Get()
	if view.Primary != p {
		compareViews(view, p, b, n)
		t.Fatalf("wanted primary %v, got %v", p, view.Primary)
	}
	if view.Backup != b {
		compareViews(view, p, b, n)
		t.Fatalf("wanted backup %v, got %v", b, view.Backup)
	}
	if n != 0 && n != view.Viewnum {
		compareViews(view, p, b, n)
		t.Fatalf("wanted viewnum %v, got %v", n, view.Viewnum)
	}
	if ck.Primary() != p {
		compareViews(view, p, b, n)
		t.Fatalf("wanted primary %v, got %v", p, ck.Primary())
	}
}

func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "viewserver-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}



func Test1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	vshost := port("v")
	vsterm := make(chan interface{})
	vs := StartServer(vshost, vsterm)

	ck1 := MakeClerk(port("1"), vshost)
	ck2 := MakeClerk(port("2"), vshost)
	ck3 := MakeClerk(port("3"), vshost)

	//

	if ck1.Primary() != "" {
		t.Fatalf("there was a primary too soon")
	}

	// very first primary
	fmt.Printf("Test: First primary ...\n")

	{
		ck1.Ping(0)
		time.Sleep(PingInterval)
		check(t, ck1, ck1.me, "", 1)
	}
	fmt.Printf("  ... Passed\n")

	// very first backup
	fmt.Printf("Test: First backup ...\n")

	{
		vx, _ := ck1.Get()
		ck1.Ping(1)
		ck2.Ping(0)
		time.Sleep(PingInterval)
		check(t, ck1, ck1.me, ck2.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// primary dies, backup should take over
	fmt.Printf("Test: Backup takes over if primary fails ...\n")

	{
		ck1.Ping(2)
		vx, _ := ck2.Ping(2)
		for i := 0; i < DeadPings+1; i++ {
			ck2.Ping(vx.Viewnum)
			time.Sleep(PingInterval)
		}
		check(t, ck2, ck2.me, "", vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// revive ck1, should become backup
	fmt.Printf("Test: Restarted server becomes backup ...\n")

	{
		vx, _ := ck2.Get()
		ck2.Ping(vx.Viewnum)
		ck1.Ping(0)
		time.Sleep(PingInterval)
		check(t, ck2, ck2.me, ck1.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// start ck3, kill the primary (ck2), the previous backup (ck1)
	// should become the primary, and ck3 the backup.
	fmt.Printf("Test: Idle third server becomes backup if primary fails ...\n")

	{
		vx, _ := ck2.Get()
		ck2.Ping(vx.Viewnum)
		for i := 0; i < DeadPings-1; i++ {
			ck1.Ping(vx.Viewnum)
			time.Sleep(PingInterval)
		}
		ck3.Ping(0)
		time.Sleep(PingInterval)
		time.Sleep(PingInterval)
		check(t, ck1, ck1.me, ck3.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// kill and immediately restart the primary -- does viewservice
	// conclude primary is down even though it's pinging?
	fmt.Printf("Test: Detect restarted primary ...\n")

	{
		vx, _ := ck1.Get()
		ck1.Ping(vx.Viewnum)
		ck1.Ping(0)
		ck3.Ping(vx.Viewnum)
		time.Sleep(PingInterval)
		check(t, ck3, ck3.me, ck1.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Dead backup is removed from view ...\n")

	// set up a view with just 3 as primary,
	// to prepare for the next test.
	{
		vx, _ := ck3.Get()
		ck3.Ping(vx.Viewnum)
		for i := 0; i < DeadPings+1; i++ {
			ck3.Ping(vx.Viewnum)
			time.Sleep(PingInterval)
		}
		check(t, ck3, ck3.me, "", vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// does viewserver wait for ack of previous view before
	// starting the next one?
	fmt.Printf("Test: Viewserver waits for primary to ack view ...\n")

	{
		// set up p=ck3 b=ck1, but do not ack
		vx, _ := ck1.Get()
		ck1.Ping(0)
		ck3.Ping(vx.Viewnum)
		time.Sleep(PingInterval)
		check(t, ck1, ck3.me, ck1.me, vx.Viewnum+1)

		vy, _ := ck1.Get()
		// ck3 is the primary, but it never acked.
		// let ck3 die. check that ck1 is not promoted.
		for i := 0; i < DeadPings+1; i++ {
			ck1.Ping(vy.Viewnum)
			time.Sleep(PingInterval)
		}
		check(t, ck1, ck3.me, ck1.me, vy.Viewnum)
	}
	fmt.Printf("  ... Passed\n")

	// if old servers die, check that a new (uninitialized) server
	// cannot take over.
	fmt.Printf("Test: Uninitialized server can't become primary ...\n")

	{
		v, _ := ck1.Get()
		ck1.Ping(v.Viewnum)
		ck3.Ping(v.Viewnum)

		ck2.Ping(0)
		for i := 0; i < DeadPings+1; i++ {
			time.Sleep(PingInterval)
			ck2.Ping(0)
		}
		vz, _ := ck2.Get()
		if vz.Primary == ck2.me {
			t.Fatalf("uninitialized backup promoted to primary")
		}
	}
	fmt.Printf("  ... Passed\n")

	vs.Kill(vsterm)
}
