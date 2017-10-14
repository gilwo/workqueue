package workerqueue

import (
	"fmt"
	"sync"
)

type QNode struct {
	data interface{}
}

type Queue struct {
	q        []*QNode
	count    uint
	capacity int
	lock     *sync.Mutex
}

func NewQueue(capacity int) (q *Queue, err error) {
	//TODO: add some chekcs ?
	if capacity == 0 {
		q = nil
		err = fmt.Errorf("capacity is zero")
		return
	} else if capacity > 0 {
		q = nil
		err = fmt.Errorf("not supprting capacity > 0")
		return
	}

	err = nil
	q = &Queue{}

	q.capacity = capacity
	q.q = make([]*QNode, 0)
	q.lock = &sync.Mutex{}
	return
}

func (q *Queue) Count() uint {
	return q.count
}

func (q *Queue) Enqueue(data interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()

	e := &QNode{data}
	q.q = append(q.q, e)
	q.count += 1
}

func (q *Queue) Dequeue() (data interface{}, ok bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.count == 0 {
		ok = false
		data = nil
		return
	}

	ok = true
	data = q.q[0].data
	q.q = q.q[1:]
	q.count -= 1

	return
}

func (q *Queue) Peek() (data interface{}, ok bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.count == 0 {
		ok = false
		data = nil
		return
	}

	ok = true
	data = q.q[0].data

	return
}

func (q *Queue) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.count = 0
	q.q = nil
	q.q = make([]*QNode, 0)
}

func (q *Queue) Dispose() {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.q = nil
	q = nil
}

func (q *Queue) Dump() (ret string) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.count == 0 {
		ret = "queue is empty"
	} else {
		for i, e := range q.q {
			ret += fmt.Sprintf("\tqueue[%d]: {%T - %v}\n", i, e.data, e.data)
		}
	}
	return
}
