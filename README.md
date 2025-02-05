# Distributed-Task-Scheduler

When trying to replicate it please keep in mind of few common errors: 

* build.sh file might have CRLF line endings which will throw error
* add go to your env path --> $env:PATH += ";$(go env GOPATH)\bin"
* if needed I would suggest to regenerate api.pb.go and api_grpc.pb.go file along with go.mod and go.sum as there could be version mismatch issues

I wanted to work on a distributed task scheduler especially with golang because it gives me better understanding of how golang is in many ways self sufficent building bigger projects and distributed computing interests me. I still have to add many more fetaures keeping myself ditant from production level ones 

--TODO

Architecture

    Hierarchical Scheduling: Implement a hierarchical scheduling system where a master scheduler delegates tasks to regional schedulers, which then distribute tasks to workers.
    Pull-Based Model: Instead of a push-based model, use a pull-based model where workers pull tasks from a queue when they are ready to process more work.

Features

    Dynamic Task Prioritization: Allow tasks to have dynamic priorities that can change based on certain conditions or metrics.
    Task Dependencies: Implement support for tasks that depend on the completion of other tasks.

Communication

    Event Streaming: Use an event streaming platform like Apache Kafka or NATS for inter-service communication, allowing for more scalable and real-time data flow.
    REST and gRPC: Provide both REST and gRPC interfaces for clients to interact with the system, giving flexibility in how tasks are submitted and managed.

Scheduling Algorithms

    Priority Queues: Implement priority queues for task scheduling to ensure that high-priority tasks are processed first.
    Backpressure Handling: Incorporate mechanisms to handle backpressure when the system is overloaded.

Fault Tolerance

    Circuit Breaker Pattern: Implement the circuit breaker pattern to gracefully handle failures and prevent cascading failures across the system.
    Worker Auto-Scaling: Enable auto-scaling of worker nodes based on the load using Kubernetes or a similar orchestration tool.

User Interface

    Web Dashboard: Develop a comprehensive web dashboard for monitoring and managing tasks, viewing system metrics, and configuring settings.
    Task Visualization: Provide visualization of task execution, dependencies, and progress in real-time.
