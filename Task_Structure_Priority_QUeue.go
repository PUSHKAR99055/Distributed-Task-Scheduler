package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// Task represents a unit of work to be processed
type Task struct {
	ID          string
	Description string
	Priority    int       // Lower number means higher priority
	Dependencies []*Task  // List of tasks that must be completed before this task
}

// TaskQueue is a priority queue for scheduling tasks
type TaskQueue []*Task

// Implement heap.Interface for TaskQueue
func (tq TaskQueue) Len() int            { return len(tq) }
func (tq TaskQueue) Less(i, j int) bool  { return tq[i].Priority < tq[j].Priority }
func (tq TaskQueue) Swap(i, j int)       { tq[i], tq[j] = tq[j], tq[i] }

func (tq *TaskQueue) Push(x interface{}) {
	*tq = append(*tq, x.(*Task))
}

func (tq *TaskQueue) Pop() interface{} {
	old := *tq
	n := len(old)
	task := old[n-1]
	*tq = old[0 : n-1]
	return task
}

// Worker represents a worker that pulls tasks from the queue
type Worker struct {
	ID      string
	TaskCh  chan *Task
	QuitCh  chan bool
	Workers *sync.WaitGroup
}

// NewWorker creates a new Worker
func NewWorker(id string, workers *sync.WaitGroup) *Worker {
	return &Worker{
		ID:      id,
		TaskCh:  make(chan *Task),
		QuitCh:  make(chan bool),
		Workers: workers,
	}
}

// Start method for Worker starts processing tasks
func (w *Worker) Start(taskQueue *TaskQueue, taskQueueLock *sync.Mutex) {
	go func() {
		defer w.Workers.Done()
		for {
			taskQueueLock.Lock()
			if taskQueue.Len() > 0 {
				task := heap.Pop(taskQueue).(*Task)
				taskQueueLock.Unlock()
				fmt.Printf("Worker %s processing task %s\n", w.ID, task.ID)
				time.Sleep(2 * time.Second) // Simulate task processing
			} else {
				taskQueueLock.Unlock()
				select {
				case <-w.QuitCh:
					fmt.Printf("Worker %s stopping.\n", w.ID)
					return
				default:
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()
}

// Stop method for Worker stops the worker
func (w *Worker) Stop() {
	go func() {
		w.QuitCh <- true
	}()
}

// Scheduler handles distributing tasks to workers
type Scheduler struct {
	Workers     []*Worker
	TaskQueue   *TaskQueue
	TaskQueueLock *sync.Mutex
}

// NewScheduler creates a new Scheduler with a set number of workers
func NewScheduler(numWorkers int) *Scheduler {
	taskQueue := &TaskQueue{}
	heap.Init(taskQueue)
	workers := []*Worker{}
	var workersWG sync.WaitGroup
	taskQueueLock := &sync.Mutex{}

	for i := 1; i <= numWorkers; i++ {
		worker := NewWorker(fmt.Sprintf("Worker-%d", i), &workersWG)
		workers = append(workers, worker)
		workersWG.Add(1)
		worker.Start(taskQueue, taskQueueLock)
	}

	return &Scheduler{
		Workers:     workers,
		TaskQueue:   taskQueue,
		TaskQueueLock: taskQueueLock,
	}
}

// AddTask adds a new task to the scheduler's queue
func (s *Scheduler) AddTask(task *Task) {
	s.TaskQueueLock.Lock()
	defer s.TaskQueueLock.Unlock()
	heap.Push(s.TaskQueue, task)
	fmt.Printf("Added task %s with priority %d to the queue\n", task.ID, task.Priority)
}

// StopScheduler gracefully stops all workers
func (s *Scheduler) StopScheduler() {
	for _, worker := range s.Workers {
		worker.Stop()
	}
}

func main() {
	// Create a new scheduler with 3 workers
	scheduler := NewScheduler(3)

	// Add tasks to the scheduler
	scheduler.AddTask(&Task{ID: "task-1", Priority: 2})
	scheduler.AddTask(&Task{ID: "task-2", Priority: 1})
	scheduler.AddTask(&Task{ID: "task-3", Priority: 3})

	// Run the scheduler for a while to process tasks
	time.Sleep(10 * time.Second)

	// Stop the scheduler gracefully
	scheduler.StopScheduler()

	// Wait for all workers to finish
	scheduler.Workers[0].Workers.Wait()
}
