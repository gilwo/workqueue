package workerpool

import (
	"fmt"
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

	f := func(i interface{}) interface{} {
		fmt.Printf("hello world\n")
		return true
	}

	//wp.JobQueue(f)
	//job, err := wp.JobQueue(f)
	_, err = wp.JobQueue(f)

	if err != nil {
		t.Errorf("job queue failed, err: %v", err)
	} else {
		//fmt.Printf("%T:%v\n", job, job)
	}

	var stopped bool
	time.Sleep(3 * time.Second)
	wp.StopDispatcher(func() {
		fmt.Println("pool dispatcher stopped")
		stopped = true
	})

	time.Sleep(1 * time.Second)
	if !stopped {
		t.Errorf("dispatcher did not stopped in reasonable time")
	}
	_, err = wp.Dispose()
	if err != nil {
		t.Errorf("pool failed on dispose, err: %v\n", err)
	}
}
