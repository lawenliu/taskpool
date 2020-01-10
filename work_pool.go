package pool

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolAlreadyRunning = errors.New("the pool is already running")
	ErrPoolNotRunning     = errors.New("the pool is not running")
	ErrWorkerClosed       = errors.New("worker was closed")
	ErrJobTimedOut        = errors.New("job request timed out")
)

type (
	Worker interface {
		Job(interface{}) interface{}
		Ready() bool
	}

	AdvancedWorker interface {
		Initialize()
		Terminate()
		Interrupt()
	}

	SimpleWorker struct {
		job func(interface{}) interface{}
	}

	TaskPool struct {
		masters []*taskMaster
		selects []reflect.SelectCase

		sync.RWMutex
		running uint32
		pending int32
	}
)

func (p *SimpleWorker) Job(data interface{}) interface{} {
	return p.job(data)
}

func (p *SimpleWorker) Ready() bool {
	return true
}

func NewSimplePool(n int, job func(interface{}) interface{}) *TaskPool {
	p := &TaskPool{running: 0}
	p.masters = make([]*taskMaster, n)
	for i := range p.masters {
		p.masters[i] = &taskMaster{worker: &SimpleWorker{job}}
	}
	return p
}

func NewCustomPool(workers []Worker) *TaskPool {
	p := &TaskPool{running: 0}
	p.masters = make([]*taskMaster, len(workers))
	for i := range p.masters {
		p.masters[i] = &taskMaster{worker: workers[i]}
	}
	return p
}

func (p *TaskPool) isRunning() bool {
	return atomic.LoadUint32(&p.running) == 1
}

func (p *TaskPool) setRunning(running bool) {
	var run uint32
	if running {
		run = 1
	}
	atomic.SwapUint32(&p.running, run)
}

func (p *TaskPool) Run() error {
	p.Lock()
	defer p.Unlock()

	if !p.isRunning() {
		p.selects = make([]reflect.SelectCase, len(p.masters))
		for i, w := range p.masters {
			w.run()

			p.selects[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(w.chReady),
			}
		}
		p.setRunning(true)
		return nil
	}

	return ErrPoolAlreadyRunning
}

func (p *TaskPool) Close() error {
	p.Lock()
	defer p.Unlock()

	if p.isRunning() {
		for _, w := range p.masters {
			w.close()
		}

		for _, w := range p.masters {
			w.join()
		}

		p.setRunning(false)
		return nil
	}

	return ErrPoolNotRunning
}

func (p *TaskPool) SendWorkWithTimeout(timeout time.Duration, data interface{}) (interface{}, error) {
	p.RLock()
	defer p.RUnlock()

	if p.isRunning() {
		before := time.Now()
		selectCases := append(p.selects[:], reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(timeout)),
		})
		if chosen, _, ok := reflect.Select(selectCases); ok {
			if chosen < len(selectCases)-1 {
				p.masters[chosen].chInput <- data
				select {
				case data, open := <-p.masters[chosen].chOutput:
					if !open {
						return nil, ErrWorkerClosed
					}
					return data, nil
				case <-time.After(timeout - time.Since(before)):
					return nil, ErrJobTimedOut
				}
			} else {
				return nil, ErrJobTimedOut
			}
		} else {
			return nil, ErrWorkerClosed
		}
	} else {
		return nil, ErrPoolNotRunning
	}
}

func (p *TaskPool) SendAsyncWorkWithTimeout(timeout time.Duration, data interface{}, after func(interface{}, error)) {
	atomic.AddInt32(&p.pending, 1)
	go func() {
		defer atomic.AddInt32(&p.pending, -1)
		result, err := p.SendWorkWithTimeout(timeout, data)
		if after != nil {
			after(result, err)
		}
	}()
}

func (p *TaskPool) SendWork(data interface{}) (interface{}, error) {
	p.RLock()
	defer p.RUnlock()

	if p.isRunning() {
		if chosen, _, ok := reflect.Select(p.selects); ok {
			p.masters[chosen].chInput <- data
			result, open := <-p.masters[chosen].chOutput
			if !open {
				return nil, ErrWorkerClosed
			}
			return result, nil
		}
		return nil, ErrWorkerClosed
	}

	return nil, ErrPoolNotRunning
}

func (p *TaskPool) SendAsyncWork(data interface{}, after func(interface{}, error)) {
	atomic.AddInt32(&p.pending, 1)

	go func() {
		defer atomic.AddInt32(&p.pending, -1)
		result, err := p.SendWork(data)
		if after != nil {
			after(result, err)
		}
	}()
}

func (p *TaskPool) Pending() int32 {
	return atomic.LoadInt32(&p.pending)
}

func (p *TaskPool) Size() int {
	return len(p.masters)
}
