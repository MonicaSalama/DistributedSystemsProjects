package mapreduce

import "fmt"
import "sync"



//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
var wg sync.WaitGroup

func doCall (jobName string,registerChan chan string, File string, TaskNumber int, NumOtherPhase int, phase jobPhase) {
	defer wg.Done()

	ar := DoTaskArgs{jobName, File, phase, TaskNumber, NumOtherPhase}
	var worker string
	success := false
	for (success == false) {
		worker = <-registerChan
		success = call(worker,"Worker.DoTask", ar ,nil)
		go func(worker string) {
			registerChan <- worker
		}(worker)
	} 

}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
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


	for taskNum:= 0 ; taskNum < ntasks ; taskNum++ {
		var File string
		switch phase {
		case mapPhase:
			File = mapFiles[taskNum]
		case reducePhase:
			File = ""
		}
		wg.Add(1)
		go doCall(jobName,registerChan,File, taskNum,n_other,phase)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

