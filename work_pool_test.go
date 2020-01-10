package pool

import (
	"errors"
	"math/rand"
	"testing"
)

type (
	TestWorker struct {
	}
)

func (p *TestWorker) Job(data interface{}) interface{} {
	if n, ok := data.(int); ok {
		return n << 1
	}

	return errors.New("request data type not integer")
}

func (p *TestWorker) Ready() bool {
	return true
}

func TestSimpleWorker_Job(t *testing.T) {
	p := NewSimplePool(10, func(data interface{}) interface{} {
		if num, ok := data.(int); ok {
			return num * 2
		}
		return errors.New("request data type not integer")
	})

	if err := p.Run(); err != nil {
		t.Errorf("run worker pool error %v", err)
	}

	for i := 0; i < 100; i++ {
		go func(n int) {
			ret, err := p.SendWork(n)
			if err != nil {
				t.Errorf("worker pool error %v", err)
			} else {
				if j, ok := ret.(int); !ok {
					t.Errorf("result error %d != %d", n*2, j)
				} else {
					t.Logf("result %2d * 2 = %3d", n, j)
				}
			}
		}(i)
	}
}

func BenchmarkSimpleWorker_Job(b *testing.B) {
	p := NewSimplePool(20, func(data interface{}) interface{} {
		if num, ok := data.(int); ok {
			return num * 2
		}
		return errors.New("request data type not integer")
	})

	p.Run()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.SendWork(rand.Int())
		}
	})
}

func TestTaskPool_SendWork(t *testing.T) {
	workers := make([]Worker, 20)
	for i := 0; i < 20; i++ {
		workers[i] = &TestWorker{}
	}
	p := NewCustomPool(workers)
	p.Run()

	for i := 0; i < 1000000; i++ {
		go func(n int) {
			ret, err := p.SendWork(n)
			if err != nil {
				t.Errorf("worker pool error %v", err)
			} else {
				if j, ok := ret.(int); !ok {
					t.Errorf("result error %d != %d", n*2, j)
				} else {
					t.Logf("result %2d * 2 = %3d", n, j)
				}
			}
		}(i)
	}
}

func BenchmarkTaskPool_SendWork(b *testing.B) {
	workers := make([]Worker, 20)
	for i := 0; i < 20; i++ {
		workers[i] = &TestWorker{}
	}
	p := NewCustomPool(workers)
	p.Run()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.SendWork(rand.Int())
		}
	})
}
