package workerpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func torbErrorfMsg(tb interface{}, msg string) {
	t, tok := tb.(*testing.T)
	b, _ := tb.(*testing.B)

	if tok {
		t.Errorf(msg)
	} else {
		b.Errorf(msg)
	}
}

func startCheckPool(tb interface{}, count uint) *WPool {
	wp, err := NewWPool(count)
	if err != nil {
		torbErrorfMsg(tb, "failed to create pool")
	}
	return wp
}

func startCheckDispatcher(tb interface{}, wp *WPool) {
	_, err := wp.StartDispatcher()
	if err != nil {
		torbErrorfMsg(tb, fmt.Sprintf("dispatcher failed to start: %v", err))
	}
}

func checkJobQueue(tb interface{}, wp *WPool, f JobFunc) {
	_, err := wp.JobQueue(f)

	if err != nil {
		torbErrorfMsg(tb, fmt.Sprintf("job queue failed, err: %v", err))
	} else {
	}
}

func checkPoolDispose(tb interface{}, wp *WPool) {
	_, err := wp.Dispose()
	if err != nil {
		torbErrorfMsg(tb, fmt.Sprintf("pool failed on dispose, err: %v\n", err))
	}
}

func stopCheckDispatcher(tb interface{}, wp *WPool, f func()) {
	_, err := wp.StopDispatcher(f)
	if err != nil {
		torbErrorfMsg(tb, fmt.Sprintf("pool failed to stop, err: %v\n", err))
	}
}

func createJobFunc(msg string) (f func(interface{}) interface{}, ch chan (struct{})) {
	ch = make(chan (struct{}))
	f = func(i interface{}) interface{} {
		fmt.Printf(msg)
		ch <- struct{}{}
		return nil
	}
	return
}

func TestNewPool(t *testing.T) {

	wp := startCheckPool(t, 10)

	startCheckDispatcher(t, wp)

	condStopDispatcher := sync.NewCond(&sync.Mutex{})

	var stopped bool

	foo := func() {
		stopCheckDispatcher(t, wp, func() {
			fmt.Println("pool dispatcher stopped")
			stopped = true
			condStopDispatcher.Signal()
		})
	}
	timerStopDispatcher := time.AfterFunc(3*time.Second, foo)

	var hello bool

	f := func(i interface{}) interface{} {
		fmt.Printf("hello " + t.Name() + "\n")
		hello = true
		timerStopDispatcher.Stop()
		go foo()
		return nil
	}

	checkJobQueue(t, wp, f)

	go func() {
		select {
		case <-time.After(3 * time.Second):
			condStopDispatcher.Signal()
		}
	}()

	// wait for stop dispatcher func to be called or timeout
	condStopDispatcher.L.Lock()
	condStopDispatcher.Wait()
	condStopDispatcher.L.Unlock()

	if !stopped {
		t.Errorf("dispatcher did not stopped in reasonable time")
	}
	if !hello {
		t.Errorf("job function have not been called in reasonable time")
	}
	checkPoolDispose(t, wp)
}

func TestJobFailToFinishInTime(t *testing.T) {

	wp := startCheckPool(t, 1)

	startCheckDispatcher(t, wp)

	var hello bool

	f := func(i interface{}) interface{} {
		fmt.Printf("hello " + t.Name() + "\n")
		time.Sleep(2 * time.Second)
		hello = true
		return true
	}

	checkJobQueue(t, wp, f)

	c := sync.NewCond(&sync.Mutex{})

	var stopped bool

	go func() {
		select {
		case <-time.After(5 * time.Millisecond):
			stopCheckDispatcher(t, wp, func() {
				fmt.Println("pool dispatcher stopped")
				stopped = true
				c.Signal()
			})
		}
	}()

	go func() {
		select {
		case <-time.After(3 * time.Second):
			c.Signal()
		}
	}()

	// wait for stop dispatcher func to be called or timeout
	c.L.Lock()
	c.Wait()
	c.L.Unlock()

	if !stopped {
		t.Errorf("dispatcher did not stopped in reasonable time")
	}
	if hello {
		t.Errorf("job function was not suppose to be called")
	}
	checkPoolDispose(t, wp)
}

func TestMultipleJobsQueue(t *testing.T) {
	wp := startCheckPool(t, 10)

	var hello [10]bool
	var hellook bool

	chello := sync.NewCond(&sync.Mutex{})

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("hello "+t.Name()+" %d", i+1)
		y := i
		fmt.Printf("### queueing job %d\n", i+1)
		checkJobQueue(t, wp, func(j interface{}) interface{} {
			chello.L.Lock()
			chello.Wait()
			chello.L.Unlock()
			fmt.Println(msg)
			//if y != 0 {
			hello[y] = true
			//}
			return nil
		})
	}

	startCheckDispatcher(t, wp)

	time.Sleep(1 * time.Second)

	chello.Broadcast()

	cfinish := sync.NewCond(&sync.Mutex{})

	var stopchecking bool

	// finish it even if not finished
	timerStop := time.AfterFunc(5*time.Second, func() {
		stopchecking = true
		cfinish.Signal()

	})

	go func() {
		for !stopchecking {
			select {
			case <-time.After(1 * time.Second):
			}
			var ok bool = true
			// check we did ok
			for _, i := range hello {
				ok = ok && i
			}
			hellook = ok
			if ok {
				timerStop.Stop()
				cfinish.Signal()
				break
			}
		}
	}()

	cfinish.L.Lock()
	cfinish.Wait()
	cfinish.L.Unlock()

	stopCheckDispatcher(t, wp, func() {
		fmt.Println("dispatcher stopped")
		cfinish.Signal()
	})
	if !hellook {
		t.Errorf("not all hellos finished: %v", hello)
	}

	cfinish.L.Lock()
	cfinish.Wait()
	cfinish.L.Unlock()

	checkPoolDispose(t, wp)
}

func TestWPool_Dispose(t *testing.T) {
	wp := startCheckPool(t, 10)

	wp.status = Prunning

	_, err := wp.Dispose()
	if err == nil {
		t.Errorf("dispose should fail when status is running")
	} else {
		exp := fmt.Sprintf("WorkerPool cannot be disposed at the momement (%d), not stopped", Prunning)
		if exp != err.Error() {
			t.Errorf("failed pool dispose mismatch exp: %v, act: %v", exp, err.Error())
		}
	}

	wp.status = Pready
	checkPoolDispose(t, wp)
}

func TestWPoolDispatcher(t *testing.T) {

	var jobFuncCalled bool
	dispatcherStopCh := make(chan (struct{}))
	wp := startCheckPool(t, 10)

	wp.wjPool = sync.Pool{}

	startCheckDispatcher(t, wp)

	f, ch := createJobFunc("hello " + t.Name() + "\n")

	go func() {
		select {
		case <-ch:
			jobFuncCalled = true
		}
	}()

	checkJobQueue(t, wp, f)

	time.Sleep(1 * time.Second)

	stopCheckDispatcher(t, wp, func() { close(dispatcherStopCh) })

	select {
	case <-dispatcherStopCh:
		if jobFuncCalled {
			t.Errorf("job was not suppose to be called")
		}
		break
	case <-time.After(3 * time.Second):
		t.Errorf("pool dispathcer did not stopped after reasonable time")
	}
	checkPoolDispose(t, wp)
}

func TestWPoolDispatcher2(t *testing.T) {

	wp := startCheckPool(t, 10)
	var jobFuncCalled bool
	dispatcherStopCh := make(chan (struct{}))

	f, ch := createJobFunc("hello " + t.Name() + "\n")

	go func() {
		select {
		case <-ch:
			jobFuncCalled = true
		}
	}()

	checkJobQueue(t, wp, f)
	wp.q.Dequeue()
	wp.q.Enqueue(1)
	startCheckDispatcher(t, wp)
	time.Sleep(2 * time.Second)

	stopCheckDispatcher(t, wp, func() { close(dispatcherStopCh) })
	select {
	case <-dispatcherStopCh:
		if jobFuncCalled {
			t.Errorf("job was not suppose to be called")
		}
		break
	case <-time.After(3 * time.Second):
		t.Errorf("pool dispathcer did not stopped after reasonable time")
	}
	checkPoolDispose(t, wp)
}
