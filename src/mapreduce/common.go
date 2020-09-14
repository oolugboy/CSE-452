package mapreduce

import "fmt"
import "net/rpc"

const (
	Map    = "Map"
	Reduce = "Reduce"
)

type JobType string

// RPC arguments and replies.  Field names must start with capital letters,
// otherwise RPC will break.

type DoJobArgs struct {
	File          string
	Operation     JobType
	JobNumber     int // this job's number
	NumOtherPhase int // total number of jobs in other phase (map or reduce)
}

type DoJobReply struct {
	OK bool
}

type ShutdownArgs struct {
}

type ShutdownReply struct {
	Njobs int
	OK    bool
}

type RegisterArgs struct {
	WorkerDomainSocketName string
}

type RegisterReply struct {
	OK bool
}

//
// rpcCall() sends an RPC to the rpcRequestHandlerName handler on server srv
// with arguments args, waits for the reply, and leaves the// reply in reply.
// the reply argument should be the address of a reply structure.
//
// rpcCall() returns true if the server responded, and false
// if rpcCall() was not able to contact the server. in particular,
// reply's contents are valid if and only if rpcCall() returned true.
//
// you should assume that rpcCall() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use rpcCall() to send all RPCs, in master.go, mapreduce.go,
// and worker.go.  please don't change this function.
//
func rpcCall(remoteServerName string, remoteProcedureName string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", remoteServerName)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(remoteProcedureName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
