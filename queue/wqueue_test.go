package workerqueue

import (
    "testing"
    //"fmt"
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
}

func TestPushAndPop(t *testing.T) {
    q, _ := NewQueue(2)

    var data int32 = 1
    q.Enqueue("a")
    q.Enqueue(data)

    v,_ := q.Dequeue()
    if i, ok := v.(int32); ok {
        t.Errorf("invalid type assertion: %T: %v", i, i)
    } else {
        //t.Logf("%v", ok)
    }

    v1,_ := q.Dequeue()
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
}


func TestPop(t *testing.T) {
}
func TestPeek(t *testing.T) {
}
