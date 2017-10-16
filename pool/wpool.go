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
	Pready PoolStatus = iota
	Prunning
	Pstopping
	Pstopped
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
	log.SetLevel(log.WarnLevel)
	//log.SetLevel(log.InfoLevel)
	//log.SetLevel(log.DebugLevel)
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
	log.Debug("dispatcher started")

	wp.status = Prunning

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
				log.WithFields(log.Fields{
					"element type":  fmt.Sprintf("%T", d),
					"element value": fmt.Sprintf("%v", d),
				},
				).Warn("queued element is not a *WorkerJob")
				wp.q.Dequeue()
				wp.waiting -= 1
				break
			}
			worker := wp.wjPool.Get()
			if worker == nil {
				// pool get did not retrieve anything ...
				log.Warn("pool get did not retrieve pool element")
				break
			} else {
				log.WithFields(log.Fields{
					"element type":  fmt.Sprintf("%T", worker),
					"element value": fmt.Sprintf("%v", worker),
				},
				).Debug("pool get retrieved element")
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
			wp.maintStopAllWorkers()
			wp.maintFlushJobQueue()
			// TODO : what else we need to do here ?

		//case <-time.After(1 * time.Second):
		default:
			log.Info("passed")
			wp.poolUnlock("dispatching idle sleep")
			time.Sleep(time.Second)
			wp.poolLock("dispatching idle sleep")
			wp.updateWork()
		}
		wp.poolUnlock("dispatching loop select")
	}
	log.Info("notifying stopped")
	wp.stopped <- struct{}{}
}

// get new pool of workers
func NewWPool(workersCount uint) (wp *WPool, err error) {
	log.WithFields(log.Fields{"worker count": workersCount}).Debug("new pool requested")
	err = nil

	wp = &WPool{}
	wp.maxWorkers = workersCount
	wp.shouldStop = make(chan struct{})
	wp.stopped = make(chan struct{})
	wp.workNeeded = make(chan struct{}, wp.maxWorkers)

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
	wp.q, err = workerqueue.NewQueue(-1)
	if err == nil {
		wp.status = Pready
	} else {
		log.WithFields(log.Fields{"queue error": err}).Error("failed to allocate queue for pool")
		err = fmt.Errorf("failed to allocate pool")
		wp = nil
	}

	return
}

// dispose of the pool
func (wp *WPool) Dispose() (ok bool, err error) {
	wp.poolLock("dispose")
	defer wp.poolUnlock("dispose")

	if wp.status != Pstopped && wp.status != Pready {
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
		log.WithFields(log.Fields{
			"waiting":     wp.waiting,
			"running":     wp.running,
			"max workers": wp.maxWorkers,
		}).Debug("work is needed")
		wp.workNeeded <- struct{}{}
	}
}

// start the pool dispatcher
func (wp *WPool) StartDispatcher() (ok bool, err error) {
	wp.poolLock("StartDispatcher")
	defer wp.poolUnlock("StartDispatcher")

	if wp.q == nil || (wp.status != Pready && wp.status != Pstopped) {
		ok = false
		err = fmt.Errorf("pool dispatcher unstartable")
		return
	}

	ok = true
	err = nil
	wp.status = Prunning

	go wp.dispatcher()
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
