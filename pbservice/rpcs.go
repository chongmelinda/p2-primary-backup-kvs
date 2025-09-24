package pbservice

import "umich.edu/eecs491/proj2/viewservice"

// Error values
type Err string

const (
	OK             = "OK"             // Success
	ErrNoKey       = "ErrNoKey"       // No key (Get only)
	ErrWrongServer = "ErrWrongServer" // Wrong primary
)

// Operations
type Op  string

const (
	GET        = "Get"
	PUT        = "Put"
	APPEND     = "Append"
)

// An Operation: Get, Put, or Append
//
// This can be sent from Client to Primary, or
// from Primary to Backup

// Operation Arguments
type OpArgs struct {
	Op      Op       // Operation being performed
	Key     string   // Key being fetched/modified
	Value   string   // Value to Put/Append (if modification)
	Client  string   // Identifier for client requesting this operation
	SeqNo   int      // Sequence # of this operation on this client
	Source  string   // Source of this call (Client ID or Primary ID)
}

// Operation Results
type OpReply struct {
	Err    Err       // One of the Err codes
	Value  string    // value of key (Get only)
}

// Each active server must remember the last successful response for
// at least some operations from each client. The response is tagged
// with the sequence number the client used to make the request
//
// Note that you might not need to remember *all* (types of) past
// results...

type Result struct {
	SeqNo int 
	V     OpReply
}

// Push
//
// Send a copy of the current database from Primary to (new) Backup
//
// Includes the Key/Value store.
// Must also include a way to identify repeated reqeusts
// Define this in your rpcs_impl.go

type PushArgs struct {
	KVStore  map[string]string // The current DB at the caller
	OpCache  map[string]Result // The current cache of past results
	View     viewservice.View  // The current View at the caller
}

type PushReply struct {
	Err Err
}
