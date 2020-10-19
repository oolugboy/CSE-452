package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	jobStarted bool
	jobArgs DoJobArgs
	jobReply DoJobReply
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

func (mr * MapReduce) ResetWorkerInfoJobArgs(workerInfo * WorkerInfo) {
	(*workerInfo).jobStarted = false
	(*workerInfo).jobReply = DoJobReply{
		OK: false,
	}
}

func (mr * MapReduce) SetJobArgs(workerInfo * WorkerInfo, jobNumber int, jobType JobType) {
	if jobType == Map {
		(*workerInfo).jobArgs = DoJobArgs{
			mr.file,
			Map,
			jobNumber,
			mr.nReduce,
		}
	} else {
		(*workerInfo).jobArgs = DoJobArgs{
			mr.file,
			Reduce,
			jobNumber,
			mr.nMap,
		}
	}
}

func (mr *MapReduce) SendJobToWorker(workerInfo * WorkerInfo) {
	rpcCall(
		workerInfo.address,
		"Worker.DoJob",
		&(*workerInfo).jobArgs,
		&(*workerInfo).jobReply,
		)
}

func (mr *MapReduce) RunMaster() *list.List {
	mapJobsCount := 0
	mapJobsCompleted := 0
	mapDone := false

	reduceJobsCount := 0
	reduceJobsCompleted := 0
	reduceDone := false

	for mr.alive {
		select {
		case workerSocketDomainName := <- mr.workerRegistrationChannel:
			mr.WorkerInfoArray = append(
				mr.WorkerInfoArray,
				&WorkerInfo{
					address: workerSocketDomainName,
					jobStarted: false,
					jobArgs: DoJobArgs{},
					jobReply: DoJobReply{},
				})
			fmt.Printf("Just added the worker %s to the WorkerArray \n", workerSocketDomainName)
		default:
		}

		if mapDone == false {
			for i := 0; i < len(mr.WorkerInfoArray); i++ {
				currentWorkerInfo := mr.WorkerInfoArray[i]
				if mapJobsCount < mr.nMap {
					if (*currentWorkerInfo).jobStarted == false {
						mr.SetJobArgs(currentWorkerInfo, i, Map)
						go mr.SendJobToWorker(currentWorkerInfo)
						mapJobsCount++
					}
				}
				if (*currentWorkerInfo).jobReply.OK == true {
					mapJobsCompleted++
					mr.ResetWorkerInfoJobArgs(currentWorkerInfo)
				}
				if mapJobsCompleted == (mr.nMap - 1) {
					mapDone = true
				}
			}
		}

		if mapDone && reduceDone == false {
			for i := 0; i < len(mr.WorkerInfoArray); i++ {
				currentWorkerInfo := mr.WorkerInfoArray[i]
				if reduceJobsCount < mr.nReduce {
					if (*currentWorkerInfo).jobStarted == false {
						mr.SetJobArgs(currentWorkerInfo, i, Reduce)
						go mr.SendJobToWorker(currentWorkerInfo)
						reduceJobsCount++
					}
				}
				if (*currentWorkerInfo).jobReply.OK == true {
					reduceJobsCompleted++
					mr.ResetWorkerInfoJobArgs(currentWorkerInfo)
				}
				if reduceJobsCompleted == (mr.nMap - 1) {
					reduceDone = true
				}
			}
		}

		if reduceDone {
			mr.alive = false
		}
	}

	return mr.KillWorkers()

}
