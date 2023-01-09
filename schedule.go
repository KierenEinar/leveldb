package leveldb

import "sync"

type Scheduler struct {
	mutex     sync.Mutex
	cv        *sync.Cond
	dummyHead scheduleNode
	closed    <-chan struct{}
}

type ScheduleFn func(args ...interface{})

func NewSchedule(closed <-chan struct{}) *Scheduler {
	s := &Scheduler{}
	s.cv = sync.NewCond(&s.mutex)
	node := scheduleNode{}
	s.closed = closed
	s.dummyHead = node
	s.dummyHead.next = &node // dummyHead.next point to oldest node
	s.dummyHead.prev = &node // dummyHead.prev point to newest node
	go s.schedule()
	return s
}

func (s *Scheduler) Enqueue(f ScheduleFn, args ...interface{}) {
	s.mutex.Lock()
	isEmpty := s.isEmpty()
	s.append(f, args)
	if isEmpty {
		s.cv.Signal()
	}
	s.mutex.Unlock()
}

func (s *Scheduler) schedule() {

	for {
		select {
		case <-s.closed:
			return
		default:
			s.mutex.Lock()
			n := s.pop()
			if n == nil {
				s.cv.Wait()
			}
			s.mutex.Unlock()
			n.f(n.args...)
		}
	}
}

type scheduleNode struct {
	next *scheduleNode
	prev *scheduleNode
	f    func(args ...interface{})
	args []interface{}
}

func (s *Scheduler) append(f func(args ...interface{}), args ...interface{}) {
	newNode := &scheduleNode{
		f:    f,
		args: args,
	}
	tail := s.dummyHead.prev
	newNode.prev = tail
	s.dummyHead.prev = newNode
	tail.next = newNode
	newNode.next = &s.dummyHead
}

func (s *Scheduler) pop() *scheduleNode {
	if s.isEmpty() {
		return nil
	}

	n := s.dummyHead.next
	s.remove(n)
	return n
}

func (s *Scheduler) remove(node *scheduleNode) {
	if node == &s.dummyHead {
		return
	}
	prev := node.prev
	next := node.next
	next.prev = prev
	prev.next = prev
}

func (s *Scheduler) isEmpty() bool {
	return &s.dummyHead.prev == &s.dummyHead.next
}
