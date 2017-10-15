package workerpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {

	wp, err := NewWPool(10)
	if err != nil {
		t.Errorf("failed to create pool")
	}

	_, err = wp.StartDispatcher()
	if err != nil {
		t.Errorf("dispatcher failed to start: %v", err)
	}

	condStopDispatcher := sync.NewCond(&sync.Mutex{})

	var stopped bool

	foo := func() {
		wp.StopDispatcher(func() {
			fmt.Println("pool dispatcher stopped")
			stopped = true
			condStopDispatcher.Signal()
		})
	}
	timerStopDispatcher := time.AfterFunc(3*time.Second, foo)

	var hello bool

	f := func(i interface{}) interface{} {
		fmt.Printf("hello world\n")
		//time.Sleep(2*time.Second)
		hello = true
		timerStopDispatcher.Stop()
		go foo()
		return nil
	}

	_, err = wp.JobQueue(f)

	if err != nil {
		t.Errorf("job queue failed, err: %v", err)
	} else {
		//fmt.Printf("%T:%v\n", job, job)
	}

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
	_, err = wp.Dispose()
	if err != nil {
		t.Errorf("pool failed on dispose, err: %v\n", err)
	}
}

func TestJobFailToFinishInTime(t *testing.T) {

	wp, err := NewWPool(1)
	if err != nil {
		t.Errorf("failed to create pool")
	}

	_, err = wp.StartDispatcher()
	if err != nil {
		t.Errorf("dispatcher failed to start: %v", err)
	}

	var hello bool

	f := func(i interface{}) interface{} {
		fmt.Printf("hello world\n")
		time.Sleep(2 * time.Second)
		hello = true
		return true
	}

	_, err = wp.JobQueue(f)

	if err != nil {
		t.Errorf("job queue failed, err: %v", err)
	}

	c := sync.NewCond(&sync.Mutex{})

	var stopped bool

	go func() {
		select {
		case <-time.After(5 * time.Millisecond):
			wp.StopDispatcher(func() {
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
		//t.Errorf("job function have not been called in reasonable time")
		t.Errorf("job function was not suppose to be called")
	}
	_, err = wp.Dispose()
	if err != nil {
		t.Errorf("pool failed on dispose, err: %v\n", err)
	}
}
