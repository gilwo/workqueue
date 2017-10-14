package workerpool

import (
	"fmt"
	"github.com/gilwo/workqueue/queue"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
)

func NotImplementedYet(f string) {
	fmt.Printf("%s: not implemented yet\n", f)
}

type PoolStatus int

const (
	Phalted PoolStatus = iota
	Pready
	Prunning
	Pstopping
	Pstopped
	Pquitting
)

type JobStatus int

const (
	Jready JobStatus = iota
	Jrunning
	Jcancelled
	Jfinished
	Jinvalid
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	//log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	//log.SetLevel(log.WarnLevel)
	log.SetLevel(log.InfoLevel)
}

// TODO:
// type for check function for job function to see if it needs to stop executing

// type for job function to queue
type JobFunc func(interface{}) interface{}

type poolWorker interface{}

type WorkerJob struct {
	f       JobFunc
	status  JobStatus
	pworker poolWorker
}

func (wj *WorkerJob) StopJob() {

}

type WPool struct {
	maxWorkers    uint
	status        PoolStatus
	q             *workerqueue.Queue
	running       uint
	waiting       uint
	workerCreated uint
	shouldStop    chan struct{}
	stopped       chan struct{}
	wjPool        sync.Pool
	lock          *sync.Mutex
	workNeeded    chan struct{}
}

func (wp *WPool) maintStopAllWorkers() {
	NotImplementedYet("maintStopAllWorkers")
}

func (wp *WPool) maintFlushJobQueue() {
	NotImplementedYet("maintFlushJobQueue")
}

func (wp *WPool) poolLock(location string) {
	wp.lock.Lock()
	log.WithFields(log.Fields{"enter": location}).Debug("lock acquired")

}

func (wp *WPool) poolUnlock(location string) {
	log.WithFields(log.Fields{"leave": location}).Debug("lock released")
	wp.lock.Unlock()
}

func (wp *WPool) dispatcher() {
	//for wp.state
	for {
		switch wp.status {
		case Pquitting:
			wp.maintStopAllWorkers()
			wp.maintFlushJobQueue()
			// TODO : what else we need to do here ?
			return
		default:
			NotImplementedYet("dispatcher")
		}
	}
}

// get new pool of workers
func NewWPool(count uint) (wp *WPool, err error) {
	log.WithFields(log.Fields{"count": count}).Debug("abcd")
	err = nil

	//wp = &WPool{
	//	maxWorkers: count,
	//	status: Phalted,
	//	shouldStop: make(chan struct{}),
	//	workerFinished: make(chan struct{}, count),
	//	wjPool: sync.Pool{
	//		New: func () interface{}{
	//			return new(poolWorker)
	//		},
	//	},
	//}

	wp = &WPool{}
	wp.maxWorkers = count
	wp.status = Phalted
	wp.shouldStop = make(chan struct{})
	wp.stopped = make(chan struct{})
	wp.workNeeded = make(chan struct{}, wp.maxWorkers)

	//newWorkerPoolFuncHelper := func() func() interface{} {
	//	newWorkerPoolFunc := func() interface{} {
	//		if wp.created > wp.maxWorkers {
	//			return nil
	//		}
	//		return new(poolWorker)
	//	}
	//	return newWorkerPoolFunc
	//}
	wp.wjPool = sync.Pool{
		New: func() interface{} {
			wp.workerCreated += 1
			//fmt.Printf("new pool object called, total %d\n", wp.workerCreated)
			log.WithFields(log.Fields{
				"created workers": wp.workerCreated,
			}).Debug("new pool object called")
			return new(poolWorker)
		},
	}
	wp.lock = &sync.Mutex{}
	wp.q, err = workerqueue.NewQueue(10) // TODO : change this 10 ...
	if err == nil {
		wp.status = Pready
	}

	return
}

// dispose of the pool
func (wp *WPool) Dispose() (ok bool, err error) {
	wp.poolLock("dispose")
	defer wp.poolUnlock("dispose")

	if wp.status != Pstopped {
		ok = false
		err = fmt.Errorf("WorkerPool cannot be disposed at the momement (%d), not stopped", wp.status)
		return
	}

	ok = true
	err = nil

	wp.q.Dispose()
	wp.q = nil

	return
}

// get the pool status
func (wp *WPool) PoolStatus() (status PoolStatus) {
	wp.poolLock("PoolStatus")
	defer wp.poolUnlock("PoolStatus")

	return wp.status
}

func (wp *WPool) updateWork() {
	if wp.waiting > 0 && wp.running < wp.maxWorkers {
		wp.workNeeded <- struct{}{}
	}
}

// start the pool dispatcher
func (wp *WPool) StartDispatcher() (ok bool, err error) {
	wp.poolLock("StartDispatcher")
	defer wp.poolUnlock("StartDispatcher")

	if wp.q == nil || wp.status != Pready {
		ok = false
		err = fmt.Errorf("pool dispatcher unstartable")
		return
	}

	ok = true
	err = nil
	wp.status = Prunning

	go func() {

		dispatching := true
		for dispatching {
			wp.poolLock("dispatching loop select")

			select {
			case <-wp.workNeeded:
				//fmt.Println(wp.q.Dump())
				d, okd := wp.q.Peek()
				if !okd {
					// queue is empty
					break
				}
				job, okj := d.(*WorkerJob)
				if !okj {
					panic(fmt.Sprintf("dequeued element is not a worker job: %T:%v\n", job, job))
				}
				worker := wp.wjPool.Get()
				if worker == nil {
					// pool get did not retrieve anything ...
					//fmt.Println("pool get retrieved nothing")
					break
				} else {
					fmt.Printf("%T:%v\n", worker, worker)
				}
				job.pworker = worker
				wp.q.Dequeue()
				wp.running += 1
				job.status = Jready

				go func() {
					defer func() {
						wp.poolLock("job invoker (dispatcher helper)")
						defer wp.poolUnlock("job invoker (dispatcher helper)")

						wp.wjPool.Put(job.pworker)
						wp.running -= 1
					}()
					job.f(nil)
				}()

			case <-wp.shouldStop:
				log.Info("stopping requested")
				dispatching = false
			case <-time.After(1 * time.Second):
				log.Info("passed")
				wp.updateWork()
			}
			wp.poolUnlock("dispatching loop select")
		}
		log.Info("notifying stopped")
		wp.stopped <- struct{}{}
	}()
	// TODO: finish ...

	return
}

// stop the pool dispatcher
func (wp *WPool) StopDispatcher(stoppedFnCb func()) (ok bool, err error) {
	wp.poolLock("StopDispatcher")
	defer wp.poolUnlock("StopDispatcher")

	if wp.status == Pstopping {
		ok = false
		err = fmt.Errorf("pool already being stopped")
		return

	} else if wp.q == nil || wp.status != Prunning {
		ok = false
		err = fmt.Errorf("pool dispatcher is unstopable")
		return
	}
	wp.status = Pstopping

	ok = true
	err = nil

	go func() {
		//wp.poolLock("StopDispatcher helper")
		defer func() {
			//wp.poolUnlock("StopDispatcher helper")
			stoppedFnCb()
		}()

		wp.shouldStop <- struct{}{}
		<-wp.stopped
		wp.poolLock("StopDispatcher helper update status")
		wp.status = Pstopped
		wp.poolUnlock("StopDispatcher helper update status")
	}()

	return
}

// queue new job func to the worker pool
func (wp *WPool) JobQueue(jobfunc JobFunc) (job *WorkerJob, err error) {
	wp.poolLock("JobQueue")
	defer wp.poolUnlock("JobQueue")

	if wp.q == nil {
		err = fmt.Errorf("job cannot be queued")
		job = nil
		return
	}
	err = nil
	job = &WorkerJob{
		jobfunc,
		Jready,
		nil,
	}
	wp.q.Enqueue(job)
	wp.waiting += 1
	wp.updateWork()
	return
}

func (wp *WPool) JobStatus(jobid int) (status JobStatus) {

	return Jready
}
