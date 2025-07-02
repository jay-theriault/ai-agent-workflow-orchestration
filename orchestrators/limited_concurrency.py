from mock_dag import WorkflowExecutor, DAG
import asyncio
import heapq
from typing import Dict

class ParallelExecutor(WorkflowExecutor):
    """Executes tasks in parallel using critical path prioritization"""
    
    def __init__(self, dag: DAG, num_workers: int = 4):
        super().__init__(dag, num_workers)
        self.worker_queues = [asyncio.Queue() for _ in range(num_workers)]
        # Priority queue of ready tasks (min-heap using negative priority for
        # max-heap behaviour)
        self.ready_queue = []
        # Track which tasks have been dispatched to avoid scheduling them twice
        self.dispatched_tasks: set[str] = set()
        self.queued_tasks: set[str] = set()
        
    def calculate_priorities(self):
        """Calculate critical path priorities for all tasks"""
        memo = {}
        priorities = {}
        
        for task_id in self.dag.tasks:
            # Priority = critical path length from this task
            priorities[task_id] = self.dag.get_critical_path_length(task_id, memo)
            
        return priorities
    
    async def worker(self, worker_id: int):
        """Worker coroutine that processes tasks"""
        while True:
            # Check if there's a task assigned to this worker
            try:
                task_id = await asyncio.wait_for(
                    self.worker_queues[worker_id].get(), 
                    timeout=0.1
                )
                
                if task_id is None:  # Shutdown signal
                    break
                    
                # Execute the task
                await self.execute_task(task_id, worker_id)
                
            except asyncio.TimeoutError:
                continue
    
    async def scheduler(self, priorities: Dict[str, float]):
        """Scheduler that assigns tasks to workers based on priority"""
        # Initialize ready queue with tasks that have no dependencies
        for task_id, task in self.dag.tasks.items():
            if not task.predecessors:
                # Use negative priority for max-heap behavior
                heapq.heappush(self.ready_queue, (-priorities[task_id], task_id))
                self.queued_tasks.add(task_id)
        
        active_workers = set()
        
        while len(self.completed_tasks) < len(self.dag.tasks):
            # Check for newly ready tasks
            newly_ready = []
            for task_id in self.dag.get_ready_tasks(self.completed_tasks):
                if (task_id not in self.completed_tasks and
                        task_id not in self.dispatched_tasks and
                        task_id not in self.queued_tasks):
                    newly_ready.append(task_id)
            
            for task_id in newly_ready:
                heapq.heappush(self.ready_queue, (-priorities[task_id], task_id))
                self.queued_tasks.add(task_id)
            
            # Assign tasks to free workers
            for worker_id in range(self.num_workers):
                if worker_id not in active_workers and self.ready_queue:
                    _, task_id = heapq.heappop(self.ready_queue)
                    self.queued_tasks.remove(task_id)
                    self.dispatched_tasks.add(task_id)
                    await self.worker_queues[worker_id].put(task_id)
                    active_workers.add(worker_id)
            
            # Wait a bit for tasks to complete
            await asyncio.sleep(0.1)
            
            # Check which workers have completed
            active_workers = {
                w for w in active_workers 
                if not self.worker_queues[w].empty() or 
                any(t.worker_id == w and t.end_time is None 
                    for t in self.dag.tasks.values())
            }
        
        # Send shutdown signal to all workers
        for queue in self.worker_queues:
            await queue.put(None)
    
    async def run(self):
        """Run parallel execution with critical path scheduling"""
        # Calculate priorities
        priorities = self.calculate_priorities()
        
        # Log priorities
        self.log_event("priorities_calculated", "scheduler", 
                      priorities={k: v for k, v in sorted(priorities.items(), 
                                                        key=lambda x: -x[1])})
        
        # Start workers
        workers = [asyncio.create_task(self.worker(i)) 
                  for i in range(self.num_workers)]
        
        # Start scheduler
        scheduler_task = asyncio.create_task(self.scheduler(priorities))
        
        # Wait for all tasks to complete
        await scheduler_task
        await asyncio.gather(*workers)