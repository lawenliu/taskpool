package pool

import (
	"sync/atomic"
	"time"
)

type (
	taskMaster struct {
		chReady  chan int
		chInput  chan interface{}
		chOutput chan interface{}
		open     uint32
		worker   Worker
	}
)

func (p *taskMaster) loop() {
	for !p.worker.Ready() {
		if atomic.LoadUint32(&p.open) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	p.chReady <- 1

	for data := range p.chInput {
		p.chOutput <- p.worker.Job(data)
		for !p.worker.Ready() {
			if atomic.LoadUint32(&p.open) == 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		p.chReady <- 1
	}

	close(p.chReady)
	close(p.chOutput)
}

func (p *taskMaster) run() {
	if w, ok := p.worker.(AdvancedWorker); ok {
		w.Initialize()
	}

	p.chReady = make(chan int)
	p.chInput = make(chan interface{})
	p.chOutput = make(chan interface{})

	atomic.StoreUint32(&p.open, 1)
	go p.loop()
}

func (p *taskMaster) close() {
	close(p.chInput)

	atomic.StoreUint32(&p.open, 0)
}

func (p *taskMaster) join() {
	for {
		_, ready := <-p.chReady
		_, output := <-p.chOutput
		if !ready && !output {
			break
		}
	}

	if w, ok := p.worker.(AdvancedWorker); ok {
		w.Terminate()
	}
}

func (p *taskMaster) interrupt() {
	if w, ok := p.worker.(AdvancedWorker); ok {
		w.Interrupt()
	}
}
