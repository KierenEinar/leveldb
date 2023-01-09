package leveldb

import "sync"

type Scheduler struct {
	mutex sync.Mutex
	cv    *sync.Cond
	list  scheduleList
}

func NewSchedule() *Scheduler {
	s := &Scheduler{}
	s.cv = sync.NewCond(&s.mutex)
	s.dummyHead = scheduleNode{}
	return s
}

func (s *Scheduler) Enqueue(f func(args ...interface{}), args ...interface{}) {

	s.mutex.Lock()

}

type scheduleList struct {
	dummyHead scheduleNode
	tail      scheduleNode
}

type scheduleNode struct {
	next *scheduleNode
	f    func(args ...interface{})
	args []interface{}
}

func (s *scheduleNode) insert(f func(args ...interface{}), args ...interface{}) {
	newNode := &scheduleNode{
		f:    f,
		args: args,
	}

}
