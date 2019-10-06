package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

type Task struct {
	index int
	doing bool
	done  bool
}

func (mr *Master) schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var tasklist []Task

	for i := 0; i < ntasks; i++ {
		task := Task{}
		task.index = i
		task.doing = false
		task.done = false
		tasklist = append(tasklist, task)
	}

	doTask := func(jobname string, task *Task, worker string, ntasks int, n_other int, phase jobPhase,
		mapFiles []string, wg *sync.WaitGroup, nTasksDone *int) error {
		doTaskArgs := DoTaskArgs{}
		doTaskArgs.JobName = jobName
		doTaskArgs.NumOtherPhase = n_other
		doTaskArgs.TaskNumber = task.index
		doTaskArgs.Phase = phase
		doTaskArgs.File = mapFiles[task.index]
		doTaskReply := DoTaskReply{}
		if !call(worker, "Worker.DoTask", doTaskArgs, &doTaskReply) {
			wg.Done()
			task.doing = false
			task.done = false
			return fmt.Errorf(doTaskReply.Msg)
		} else {
			wg.Done()
			task.doing = false
			task.done = true
			*nTasksDone = *nTasksDone + 1
			return nil
		}

	}

	freeWorker := func(workers []string) string {
		for _, w := range workers {
			var wkStatus WorkerStatus
			if call(w, "Worker.GetStatus", new(struct{}), &wkStatus) {
				if wkStatus.Free {
					return w
				}
			}
		}
		return ""
	}

	//var workers []string
	wg := sync.WaitGroup{}
	//index := 0
	nTasksDone := 0

	for nTasksDone < ntasks {
		for i, _ := range tasklist {
			if !tasklist[i].doing && !tasklist[i].done {
				if w := freeWorker(mr.workers); w != "" {
					wkStatus := WorkerStatus{}
					wkStatus.Free = false
					if call(w, "Worker.SetStatus", &wkStatus, new(struct{})) {
						tasklist[i].doing = true
						wg.Add(1)
						go doTask(jobName, &tasklist[i], w, ntasks, n_other, phase, mapFiles, &wg, &nTasksDone)
					}
				}
			}
		}
		fmt.Print("")
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
