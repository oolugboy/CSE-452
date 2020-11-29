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

func (mr *MapReduce) SendJobToWorker(jobArgs * DoJobArgs, jobReply * DoJobReply, workerAddress string) bool {
	return rpcCall(workerAddress,"Worker.DoJob", jobArgs, jobReply)
}

func (mr *MapReduce) WaitOnWorkerToCompleteJob(jobReply * DoJobReply) {
	for !(*jobReply).OK {}
	return
}

func (mr *MapReduce) RegisterWorkers() {
	newWorkerIndex := 0
	for {
		workerSocketDomainName := <- mr.workerRegistrationChannel
		mr.WorkerInfoArray = append(mr.WorkerInfoArray, &WorkerInfo{address: workerSocketDomainName})
		mr.ReadyChannel <- newWorkerIndex
		newWorkerIndex++
	}
}

func (mr * MapReduce) RunJobOnWorker(jobArgs DoJobArgs, jobReply DoJobReply, workerInfo * WorkerInfo) {
	successfullySentJobToWorker := mr.SendJobToWorker(&jobArgs, &jobReply, (*workerInfo).address)
	if successfullySentJobToWorker {
		mr.WaitOnWorkerToCompleteJob(&jobReply)
	} else {
		go mr.RunJob(jobArgs.JobNumber, jobArgs.Operation)
	}
}

func (mr * MapReduce) RunJob(jobNumber int, jobType JobType) {
	indexOfNextReadyWorker:= <- mr.ReadyChannel
	readyWorker := mr.WorkerInfoArray[indexOfNextReadyWorker]

	jobArgs := DoJobArgs{}
	if jobType == Map {
		jobArgs = DoJobArgs{mr.file, Map, jobNumber,mr.nReduce}
	} else {
		jobArgs = DoJobArgs{mr.file, Reduce, jobNumber, mr.nMap}
	}
	jobReply := DoJobReply{}
	mr.RunJobOnWorker(jobArgs, jobReply, readyWorker)
	mr.JobDoneChannel <- true
	mr.ReadyChannel <- indexOfNextReadyWorker
}

func (mr *MapReduce) RunMaster() *list.List {
	go mr.RegisterWorkers()

	for i := 0; i < mr.nMap; i++ {
		go mr.RunJob(i, Map)
	}

	for i := 0; i < mr.nMap; i++ {
		<- mr.JobDoneChannel
	}

	for i := 0; i < mr.nReduce; i++ {
		go mr.RunJob(i, Reduce)
	}
	for i := 0; i < mr.nReduce; i++ {
		<- mr.JobDoneChannel
	}

	mr.Merge()
	mr.DoneChannel <- true
	return mr.KillWorkers()
}
