package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, workerInfo := range mr.WorkerInfoMap {
		DPrintf("DoWork: shutdown %s\n", workerInfo.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		successfulShutdown := rpcCall(workerInfo.address, "Worker.Shutdown", args, &reply)
		if successfulShutdown == true {
			l.PushBack(reply.Njobs)
		} else {
			fmt.Printf("DoWork: RPC %s shutdown error\n", workerInfo.address)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Execution Description
	// The master should handle the registration of the workers
	// The master should then orchestrate giving map and reduce jobs to workers that
	// have registered in parallel
	// We would not be handling worker failures yet.
	// Your code here
	// Step 1: handle the workers that have registered
	// RegisterWorker gives the master the worker's Unix domain socket name on its own process
	// The Master then reads the Registration channel and updates the WorkerInfo map

	for mr.alive {
		select {
		case workerSocketDomainName := <- mr.workerRegistrationChannel:
			mr.WorkerInfoArray = append(mr.WorkerInfoArray, &WorkerInfo{ address: workerSocketDomainName })
			fmt.Printf("Just added the worker %s to the WorkerArray \n", workerSocketDomainName)
		default:
		}
	}

	return mr.KillWorkers()

}
