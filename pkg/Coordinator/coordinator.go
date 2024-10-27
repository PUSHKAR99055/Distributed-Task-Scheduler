package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/PUSHKAR99055/Distributed-Task-Scheduler/pkg/grpcapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/PUSHKAR99055/Distributed-Task-Scheduler/pkg/common"
)

const (
	shutdownTimeout  = 5 * time.Second
	defaultMaxMisses = 1
	scanInterval     = 10 * time.Second
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	serverPort          string
	listener            net.Listener
	grpcServer          *grpc.Server
	WorkerPool          map[uint32]*workerInfo
	WorkerPoolMutex     sync.Mutex
	WorkerPoolKeys      []uint32
	WorkerPoolKeysMutex sync.RWMutex
	maxHeartbeatMisses  uint8
	heartbeatInterval   time.Duration
	roundRobinIndex     uint32
	dbConnectionString  string
	dbPool              *pgxpool.Pool
	ctx                 context.Context        // The root context for all goroutines
	cancel              context.CancelFunc     // Function to cancel the context
	wg                  sync.WaitGroup         // WaitGroup to wait for all goroutines to finish
	taskQueue           chan *pb.TaskRequest   // Task queue to hold pending tasks
	workerPool          map[uint32]*workerInfo // Track worker info as before
}

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

// NewServer initializes and returns a new Server instance.
func NewServer(port string, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		WorkerPool:         make(map[uint32]*workerInfo),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  common.DefaultHeartbeat,
		dbConnectionString: dbConnectionString,
		serverPort:         port,
		ctx:                ctx,
		cancel:             cancel,
		taskQueue:          make(chan *pb.TaskRequest, 100), // Queue with a capacity of 100
	}
}

// Start initiates the server's operations.
func (s *CoordinatorServer) Start() error {
	var err error
	go s.manageWorkerPool()

	if err = s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	s.dbPool, err = common.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}

	go s.scanDatabase()

	return s.awaitShutdown()
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.listener, err = net.Listen("tcp", s.serverPort)
	if err != nil {
		return err
	}

	log.Printf("Starting gRPC server on %s\n", s.serverPort)
	s.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (s *CoordinatorServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

// Stop gracefully shuts down the server.
func (s *CoordinatorServer) Stop() error {
	// Signal all goroutines to stop
	s.cancel()
	// Wait for all goroutines to finish
	s.wg.Wait()

	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()
	for _, worker := range s.WorkerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.listener != nil {
		return s.listener.Close()
	}

	s.dbPool.Close()
	return nil
}

func (s *CoordinatorServer) SubmitTask(ctx context.Context, in *pb.ClientTaskRequest) (*pb.ClientTaskResponse, error) {
	data := in.GetData()
	taskId := uuid.New().String()
	task := &pb.TaskRequest{
		TaskId: taskId,
		Data:   data,
	}

	// Add the task to the task queue instead of assigning it to a worker immediately
	s.taskQueue <- task

	return &pb.ClientTaskResponse{
		Message: "Task submitted successfully and added to queue",
		TaskId:  taskId,
	}, nil
}

func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	status := req.GetStatus()
	taskId := req.GetTaskId()
	var timestamp time.Time
	var column string

	switch status {
	case pb.TaskStatus_STARTED:
		timestamp = time.Unix(req.GetStartedAt(), 0)
		column = "started_at"
	case pb.TaskStatus_COMPLETE:
		timestamp = time.Unix(req.GetCompletedAt(), 0)
		column = "completed_at"
	case pb.TaskStatus_FAILED:
		timestamp = time.Unix(req.GetFailedAt(), 0)
		column = "failed_at"
	default:
		log.Println("Invalid Status in UpdateStatusRequest")
		return nil, errors.ErrUnsupported
	}

	sqlStatement := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", column)
	_, err := s.dbPool.Exec(ctx, sqlStatement, timestamp, taskId)
	if err != nil {
		log.Printf("Could not update task status for task %s: %+v", taskId, err)
		return nil, err
	}

	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.WorkerPoolKeysMutex.RLock()
	defer s.WorkerPoolKeysMutex.RUnlock()

	workerCount := len(s.WorkerPoolKeys)
	if workerCount == 0 {
		return nil
	}

	worker := s.WorkerPool[s.WorkerPoolKeys[s.roundRobinIndex%uint32(workerCount)]]
	s.roundRobinIndex++
	return worker
}

func (s *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return errors.New("no workers available")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) RequestTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskRequestResponse, error) {
	// Lock to safely access the task queue
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	var task *pb.TaskRequest
	// Check if there are tasks in the queue
	if len(s.taskQueue) > 0 {
		// Pop a task from the queue
		task = s.taskQueue[0]
		s.taskQueue = s.taskQueue[1:]
	} else {
		// No task available
		task = nil
	}

	return &pb.TaskRequestResponse{
		Task: task,
	}, nil
}

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	workerID := in.GetWorkerId()

	if worker, ok := s.WorkerPool[workerID]; ok {
		// log.Println("Reset hearbeat miss for worker:", workerID)
		worker.heartbeatMisses = 0
	} else {
		log.Println("Registering worker:", workerID)
		conn, err := grpc.Dial(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		s.WorkerPool[workerID] = &workerInfo{
			address:             in.GetAddress(),
			grpcConnection:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}

		s.WorkerPoolKeysMutex.Lock()
		defer s.WorkerPoolKeysMutex.Unlock()

		workerCount := len(s.WorkerPool)
		s.WorkerPoolKeys = make([]uint32, 0, workerCount)
		for k := range s.WorkerPool {
			s.WorkerPoolKeys = append(s.WorkerPoolKeys, k)
		}

		log.Println("Registered worker:", workerID)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) scanDatabase() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go s.executeAllScheduledTasks()
		case <-s.ctx.Done():
			log.Println("Shutting down database scanner.")
			return
		}
	}
}

func (s *CoordinatorServer) executeAllScheduledTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := s.dbPool.Begin(ctx)
	if err != nil {
		log.Printf("Unable to start transaction: %v\n", err)
		return
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil && err.Error() != "tx is closed" {
			log.Printf("ERROR: %#v", err)
			log.Printf("Failed to rollback transaction: %v\n", err)
		}
	}()

	rows, err := tx.Query(ctx, `SELECT id, command FROM tasks WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED`)
	if err != nil {
		log.Printf("Error executing query: %v\n", err)
		return
	}
	defer rows.Close()

	var tasks []*pb.TaskRequest
	for rows.Next() {
		var id, command string
		if err := rows.Scan(&id, &command); err != nil {
			log.Printf("Failed to scan row: %v\n", err)
			continue
		}

		tasks = append(tasks, &pb.TaskRequest{TaskId: id, Data: command})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v\n", err)
		return
	}

	for _, task := range tasks {
		if err := s.submitTaskToWorker(task); err != nil {
			log.Printf("Failed to submit task %s: %v\n", task.GetTaskId(), err)
			continue
		}

		if _, err := tx.Exec(ctx, `UPDATE tasks SET picked_at = NOW() WHERE id = $1`, task.GetTaskId()); err != nil {
			log.Printf("Failed to update task %s: %v\n", task.GetTaskId(), err)
			continue
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v\n", err)
	}
}

func (s *CoordinatorServer) manageWorkerPool() {
	s.wg.Add(1)
	defer s.wg.Done()

	for {
		select {
		case task := <-s.taskQueue: // Pull the next task from the queue
			worker := s.getNextAvailableWorker()
			if worker == nil {
				log.Println("No available workers. Re-queueing task.")
				s.taskQueue <- task // If no worker is available, re-add the task to the queue
			} else {
				// Assign the task to the available worker
				go func(worker *workerInfo, task *pb.TaskRequest) {
					if err := s.submitTaskToWorker(task); err != nil {
						log.Printf("Failed to assign task %s to worker: %v\n", task.TaskId, err)
						s.taskQueue <- task // Re-add the task to the queue if it fails
					}
				}(worker, task)
			}
		case <-s.ctx.Done():
			return // Exit when the server is shutting down
		}
	}
}

func (s *CoordinatorServer) getNextAvailableWorker() *workerInfo {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	for _, worker := range s.WorkerPool {
		if worker.heartbeatMisses == 0 {
			return worker // Return the first worker with no missed heartbeats (indicating it's available)
		}
	}

	return nil // No available worker
}

func (s *CoordinatorServer) removeInactiveWorkers() {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	for workerID, worker := range s.WorkerPool {
		if worker.heartbeatMisses > s.maxHeartbeatMisses {

			log.Printf("Removing inactive worker: %d\n", workerID)
			worker.grpcConnection.Close()
			delete(s.WorkerPool, workerID)

			s.WorkerPoolKeysMutex.Lock()

			workerCount := len(s.WorkerPool)
			s.WorkerPoolKeys = make([]uint32, 0, workerCount)
			for k := range s.WorkerPool {
				s.WorkerPoolKeys = append(s.WorkerPoolKeys, k)
			}

			s.WorkerPoolKeysMutex.Unlock()
		} else {
			worker.heartbeatMisses++
		}
	}
}
