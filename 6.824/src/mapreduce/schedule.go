package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	// USING CHANNELS TO BLOCK - WORKS!
	// completedTasks := make(chan string, ntasks)
	// INSIDE DONE
	// completedTasks <- (strconv.Itoa(iter) + workerAddress)
	// AT THE END
	// for i := 0; i < ntasks; i++ {
	// 	select {
	// 	case msg := <-completedTasks:
	// 		fmt.Println("Task:", msg)
	// 	}
	// }

	var group sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		group.Add(1)
		go func(iter int) {
			defer group.Done()
			for {
				workerAddress := <-registerChan
				var file string
				if phase == mapPhase {
					file = mapFiles[iter]
				}
				done := call(workerAddress, "Worker.DoTask", DoTaskArgs{
					JobName:       jobName,
					File:          file,
					Phase:         phase,
					TaskNumber:    iter,
					NumOtherPhase: nOther,
				}, nil)

				if done {
					// As registerChan is buffered, don't block if buffer is full
					select {
					case registerChan <- workerAddress:
						fmt.Println("Worker Registered back")
					default:
					}
					break
				} else {
					// Not putting back worker in channel, as it failed (better to have a count)
					fmt.Println("Call failed for", iter, "Retrying with a different worker")
				}
			}
		}(i)
	}

	group.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
