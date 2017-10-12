package workerqueue

import (
	"testing"
)

func TestNewQueue(t *testing.T) {
	if _, err := NewQueue(0); err != nil {
		act := err.Error()
		exp := "capacity is zero"
		if act != exp {
			t.Errorf("failed queue creation reason: expected : '%s', actual: '%s'",
				act, act)
		}
	}

	q, _ := NewQueue(2)
	if q == nil {
		t.Errorf("failed to create new queue")
	}
	if c := q.Count(); c != 0 {
		t.Errorf("queue count mismatch, actual: %d, expected: %d", c, 0)
	}

	q.Dispose()
	q = nil
}

func TestPushAndPop(t *testing.T) {
	q, _ := NewQueue(2)

	var data int32 = 1
	q.Enqueue("a")
	q.Enqueue(data)

	v, _ := q.Dequeue()
	if i, ok := v.(int32); ok {
		t.Errorf("invalid type assertion: %T: %v", i, i)
	} else {
		//t.Logf("%v", ok)
	}

	v1, _ := q.Dequeue()
	if v1.(int32) != data {
		t.Errorf("value pushed and poped mismatch, pushed: %d, poped %d", data, v1)
	}

	if _, err := q.Dequeue(); err == nil {
		t.Errorf("Empty Queue invalid Dequeue")
	} else {
		act := err.Error()
		exp := "queue is empty"
		if act != exp {
			t.Errorf("Queue invalid Dequeue error mismatch, actual: '%s', expected: '%s'",
				act, exp)
		}
	}

	q.Dispose()
	q = nil
}

func TestCount(t *testing.T) {
	q, _ := NewQueue(2)

	if c := q.Count(); c != 0 {
		t.Errorf("queue count, actual: %d, expected: 0", c)
	}

	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	if c := q.Count(); c != 3 {
		t.Errorf("queue count, actual: %d, expected: 3", c)
	}

	q.Dequeue()
	if c := q.Count(); c != 2 {
		t.Errorf("queue count, actual: %d, expected: 2", c)
	}

	q.Dispose()
	q = nil
}

func TestPeek(t *testing.T) {
	q, _ := NewQueue(2)

	if c := q.Count(); c != 0 {
		t.Errorf("queue count, actual: %d, expected: 0", c)
	}

	d, err := q.Peek()
	if err == nil {
		t.Errorf("empty queue peek failed, expected: nil, actual: %v", d)
	} else {
		act := err.Error()
		exp := "queue is empty"
		if act != exp {
			t.Errorf("Queue invalid peek error mismatch, actual: '%s', expected: '%s'",
				act, exp)
		}
	}

	var data int32 = 1
	q.Enqueue(data)

	d, err = q.Peek()
	if err != nil {
		t.Errorf("queue peek failed, error %s", err)
	} else {
		if d.(int32) != data {
			t.Errorf("Queue invalid peek, actual: %v, expected: %v",
				d, 1)
		}
	}

	q.Dispose()
	q = nil
}

func TestQueueClear(t *testing.T) {
	q, _ := NewQueue(2)

	if c := q.Count(); c != 0 {
		t.Errorf("queue count, actual: %d, expected: 0", c)
	}

	q.Enqueue(1)

	q.Clear()

	d, err := q.Peek()
	if err == nil {
		t.Errorf("cleared queue peek failed, expected: nil, actual: %v", d)
	} else {
		act := err.Error()
		exp := "queue is empty"
		if act != exp {
			t.Errorf("Queue invalid peek error mismatch, actual: '%s', expected: '%s'",
				act, exp)
		}
	}
	q.Dispose()
	q = nil
}

func TestQueueDump(t *testing.T) {
	q, _ := NewQueue(2)

	if c := q.Count(); c != 0 {
		t.Errorf("queue count, actual: %d, expected: 0", c)
	}

	expdump := "queue is empty"
	actdump := q.Dump()
	if actdump != expdump {
		t.Errorf("Queue dump mismatch, actual: '%s', expected: '%s'",
			actdump, expdump)
	}

	q.Enqueue(1)

	//fmt.Println(q.Dump())

	expdump = "\tqueue[0]: {int - 1}\n"
	actdump = q.Dump()
	if actdump != expdump {
		t.Errorf("Queue dump mismatch, actual: '%s', expected: '%s'",
			actdump, expdump)
	}

	q.Dispose()
	q = nil
}
