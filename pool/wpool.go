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
	Junassigned JobStatus = iota
	Jready
	Jrunning
	Jstopping
	Jfinished
	Jinvalid
)

const (
	PanicLevel log.Level = log.PanicLevel
	FatalLevel log.Level = log.FatalLevel
	ErrorLevel log.Level = log.ErrorLevel
	WarnLevel  log.Level = log.WarnLevel
	InfoLevel  log.Level = log.InfoLevel
	DebugLevel log.Level = log.DebugLevel
)

var workerpoolLogLevel log.Level = log.ErrorLevel

func WorkerPoolSetLogLevel(level log.Level) {
	workerpoolLogLevel = level

	log.SetLevel(workerpoolLogLevel)
}

func init() {
	// Log as JSON instead of the default ASCII formatter.
	//log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(workerpoolLogLevel)
}

// type for check function for job function to see if it needs to stop executing
type CheckStop func() bool

// type for job function to queue
type JobFunc func(interface{}, *WorkerJob, CheckStop) interface{}

type poolWorker interface{}

type WorkerJob struct {
	f          JobFunc
	fArg       interface{}
	fRet       interface{}
	lock       sync.RWMutex
	status     JobStatus
	pworker    poolWorker
	shouldStop bool
	stopChan   chan (struct{})
	pool       *WPool
}

func (w WorkerJob) String() string {
	return fmt.Sprintf(
		"Job [%p], func [%p], arg [%v], ret [%v], status [%v], worker [%p], pool [%p], shouldStop [%v]",
			&w, w.f, w.fArg, w.fRet, w.status, w.pworker, w.pool, w.shouldStop)
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
	lock          *sync.Mutex
	workNeeded2   bool
	workers       map[poolWorker]*WorkerJob
	workersCount  int
	wjQ           *workerqueue.Queue
}

func (wp *WPool) maintStopAllWorkers() {
	log.WithFields(log.Fields{"current workers count": wp.workerCreated}).Debug("stopping all workers")
	for _, v := range wp.workers {
		if v != nil {
			v.JobStop(nil)
		}
	}
}

func (wp *WPool) maintFlushJobQueue() {
	log.WithFields(log.Fields{"current workers count": wp.workerCreated}).Debug("flushing all workers")
	for k, v := range wp.workers {
		if v != nil {
			v.JobDispose()
			wp.workers[k] = nil
		}
	}
}

func (wp *WPool) poolLock(location string) {
	wp.lock.Lock()
	log.WithFields(log.Fields{"enter": location, "pool": wp}).Debug("Pool Lock acquired")

}

func (wp *WPool) poolUnlock(location string) {
	log.WithFields(log.Fields{"leave": location, "pool": wp}).Debug("Pool Lock released")
	wp.lock.Unlock()
}

func (wp *WPool) dispatcher() {
	log.Debug("dispatcher started")

	dispatching := true
	for dispatching {
		wp.poolLock("dispatching loop select")

		select {

		case <-wp.shouldStop:
			log.Info("stopping requested")
			dispatching = false
			wp.maintStopAllWorkers()
			wp.maintFlushJobQueue()
			// TODO : what else we need to do here ?

		default:
			if wp.workNeeded2 {
				wp.workNeeded2 = false
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
				worker, _ := wp.wjQ.Dequeue()
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
				wp.workersCount += 1

				job.pworker = worker
				wp.q.Dequeue()
				wp.waiting -= 1
				wp.running += 1
				job.status = Jready
				wp.workers[job.pworker] = job
				wp.updateWork()

				// job invoker
				go func() {

					//wp.poolLock("pre invoking")
					//job.jobWLock("pre invoking")
					//job.pworker = worker
					//wp.q.Dequeue()
					//wp.waiting -= 1
					//wp.running += 1
					//job.status = Jready
					//wp.workers[job.pworker] = job
					//job.jobWUnlock("pre invoking")
					//wp.updateWork()
					//wp.poolUnlock("pre invoking")

					//fmt.Println(wp.poolStats())
					//fmt.Println("++")
					defer func() {
						wp.poolLock("pool job invoker finisher (dispatcher helper)")
						defer wp.poolUnlock("pool job invoker finsher (dispatcher helper)")

						//fmt.Println("--")
						wp.workers[job.pworker] = nil
						wp.workersCount -= 1
						//wp.wjPool.Put(job.pworker)
						wp.wjQ.Enqueue(job.pworker)
						wp.running -= 1
						wp.updateWork()
					}()
					job.status = Jrunning
					fRet := job.f(job.fArg, job, func() (shouldStop bool) {
						job.jobRLock("job should stop checker")
						defer job.jobRUnlock("job should stop checker")
						shouldStop = job.shouldStop
						return
					})
					//fmt.Printf(":")
					job.jobWLock("job worker finishing")
					job.fRet = fRet
					job.status = Jfinished
					close(job.stopChan)
					job.jobWUnlock("job worker finishing")
				}()
			} else {
				log.Info("passed")
				wp.poolUnlock("dispatching idle sleep")
				time.Sleep(100 * time.Millisecond)
				wp.poolLock("dispatching idle sleep")
				wp.updateWork()
			}
			log.Info("passed")
			wp.poolUnlock("dispatching idle sleep")
			time.Sleep(100 * time.Millisecond)
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

	wp = &WPool{}
	wp.maxWorkers = workersCount
	wp.shouldStop = make(chan struct{})
	wp.stopped = make(chan struct{})
	wp.workers = make(map[poolWorker]*WorkerJob)

	wp.lock = &sync.Mutex{}

	wp.wjQ, err = workerqueue.NewQueue(-1)
	if err != nil {
		log.WithFields(log.Fields{"queue error": err}).Error("failed to allocate queue for pool")
		err = fmt.Errorf("failed to allocate worker pool queue")
		wp = nil
		return
	}
	for i := uint(0); i < wp.maxWorkers ; i++ {
		wp.wjQ.Enqueue(new(poolWorker))
		wp.workerCreated += 1
	}
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
		err = fmt.Errorf("WorkerPool cannot be disposed at the momement (%d), not stopped", wp.status)
		return
	}

	ok = true

	wp.q.Dispose()
	wp.q = nil

	wp.wjQ.Dispose()
	wp.wjQ = nil

	return
}

// get the pool status
func (wp *WPool) PoolStatus() (status PoolStatus) {
	wp.poolLock("PoolStatus")
	defer wp.poolUnlock("PoolStatus")

	return wp.status
}

func (wp *WPool) poolStats() (stats string) {
	return fmt.Sprintf("running: %v, waiting: %v, created: %v, workerscount %v",
		wp.running, wp.waiting, wp.workerCreated, wp.workersCount)
}

// get the pool status
func (wp *WPool) PoolStats() (stats string) {
	wp.poolLock("PoolStats")
	defer wp.poolUnlock("PoolStats")

	return wp.poolStats()
}

func (wp *WPool) updateWork() {
	if wp.waiting > 0 && wp.running < wp.maxWorkers {
		log.WithFields(log.Fields{
			"status":     wp.status,
			"waiting":     wp.waiting,
			"running":     wp.running,
			"max workers": wp.maxWorkers,
		}).Debug("work is needed")
		wp.workNeeded2 = true
	}
}

// start the pool dispatcher
func (wp *WPool) StartDispatcher() (ok bool, err error) {
	wp.poolLock("StartDispatcher")
	defer wp.poolUnlock("StartDispatcher")

	if wp.q == nil || (wp.status != Pready && wp.status != Pstopped) {
		err = fmt.Errorf("pool dispatcher unstartable")
		return
	}
	ok = true
	wp.status = Prunning
	go wp.dispatcher()

	return
}

// stop the pool dispatcher
func (wp *WPool) StopDispatcher(stoppedFnCb func()) (ok bool, err error) {
	wp.poolLock("StopDispatcher")
	defer wp.poolUnlock("StopDispatcher")

	if wp.status == Pstopping {
		err = fmt.Errorf("pool already being stopped")
		return

	} else if wp.q == nil || wp.status != Prunning {
		err = fmt.Errorf("pool dispatcher is unstopable")
		return
	}
	wp.status = Pstopping

	ok = true

	go func() {
		//wp.poolLock("StopDispatcher helper")
		defer func() {
			//wp.poolUnlock("StopDispatcher helper")
			if stoppedFnCb != nil {
				stoppedFnCb()
			}
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
func (wp *WPool) NewJobQueue(jobfunc JobFunc, jobArg interface{}) (job *WorkerJob, err error) {
	wp.poolLock("JobQueue")
	defer wp.poolUnlock("JobQueue")

	if wp.q == nil {
		err = fmt.Errorf("job cannot be queued")
		return
	}
	job = NewJobWorker(jobfunc, jobArg)
	job.status = Jready
	job.pool = wp
	wp.q.Enqueue(job)

	wp.waiting += 1
	wp.updateWork()
	return
}

// queue worker job to the worker pool
func (wp *WPool) JobQueue(job *WorkerJob) (err error) {
	wp.poolLock("JobQueue")
	defer wp.poolUnlock("JobQueue")

	if wp.q == nil || job == nil || job.status == Jinvalid || job.pool != nil {
		log.WithFields(log.Fields{
			"pool": wp,
			"job": job,
		}).Warn("job cannot be queued")
		err = fmt.Errorf("job cannot be queued")
		return
	}

	job.pool = wp
	job.status = Jready
	wp.q.Enqueue(job)

	wp.waiting += 1
	wp.updateWork()
	return
}

func NewJobWorker(jobfunc JobFunc, jobArg interface{}) (job *WorkerJob) {
	job = &WorkerJob{
		jobfunc,
		jobArg,
		nil,
		sync.RWMutex{},
		Junassigned,
		nil,
		false,
		make(chan (struct{})),
		nil,
	}
	return
}

func (job *WorkerJob) jobRLock(location string) {
	job.lock.RLock()
	log.WithFields(log.Fields{"enter": location, "job": job}).Debug("Job Rlock acquired")
}

func (job *WorkerJob) jobRUnlock(location string) {
	log.WithFields(log.Fields{"leave": location, "job": job}).Debug("Job Rlock released")
	job.lock.RUnlock()
}

func (job *WorkerJob) jobWLock(location string) {
	job.lock.Lock()
	log.WithFields(log.Fields{"enter": location, "job": job}).Debug("Job Wlock acquired")
}

func (job *WorkerJob) jobWUnlock(location string) {
	log.WithFields(log.Fields{"leave": location, "job": job}).Debug("Job Wlock released")
	job.lock.Unlock()
}

func (job *WorkerJob) JobRemove() (ok bool) {
	NotImplementedYet("JobRemove")
	return false
}

// disposing a WorkerJob, not usable after call
// this function acquire lock
func (job *WorkerJob) JobDispose() (err error) {
	job.jobWLock("JobDispose")
	defer func () {
		if err == nil {
			job.status = Jinvalid
			job.jobWUnlock("JobDispose")
		}
	}()

	if job.pool != nil && job.status != Junassigned {
		log.WithFields(log.Fields{
			"job status": job.status,
			"job": job,
		}).Warn("job cannot be disposed now")
		err = fmt.Errorf("unable to dispose job")
	}
	return
}

// retrieve WorkerJob status
// this function acquire lock
func (job *WorkerJob) JobStatus() (status JobStatus) {
	job.jobRLock("JobStatus")
	defer job.jobRUnlock("JobStatus")
	status = job.status
	return
}

// retrieve WorkerJob func return value
// this function acquire lock
func (job *WorkerJob) JobRetVal() (retVal interface{}) {
	job.jobRLock("JobRetVal")
	defer job.jobRUnlock("JobRetVal")
	if job.status != Jfinished {
		return
	}
	retVal = job.fRet
	return
}

// stop WorkerJob execution
// this function acquire lock
func (job *WorkerJob) JobStop(notifyStopped func(*WorkerJob)) (ok bool) {
	job.jobWLock("JobStop")
	defer job.jobWUnlock("JobStop")
	if job.status != Jrunning {
		return
	}
	job.status = Jstopping
	job.shouldStop = true
	ok = true
	if notifyStopped != nil {
		go notifyStopped(job)
	}
	return
}
