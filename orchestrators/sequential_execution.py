from mock_dag import WorkflowExecutor, DAG
import asyncio

# Example naive sequential executor for comparison
class SequentialExecutorSingleWorker(WorkflowExecutor):
    """Executes tasks sequentially (BFS order)"""

    def __init__(self, dag: DAG, **kwargs):
        """Create a new executor.

        Args:
            dag: The workflow DAG to execute.
            num_workers: Maximum number of concurrent worker coroutines.
        """
        # Initialize parent class (sets ``self.dag`` and bookkeeping structures).
        super().__init__(dag, num_workers=1)
    
    async def run(self):
        """Run all tasks sequentially"""
        # Get tasks in BFS order
        visited = set()
        queue = [task_id for task_id, task in self.dag.tasks.items() 
                if not task.predecessors]
        
        while queue:
            # Sort for consistent ordering (simulating alphabetical processing)
            queue.sort()
            task_id = queue.pop(0)
            
            if task_id in visited:
                continue
                
            # Check if dependencies are met
            task = self.dag.tasks[task_id]
            if all(pred in self.completed_tasks for pred in task.predecessors):
                await self.execute_task(task_id, worker_id=0)
                visited.add(task_id)
                
                # Add successors to queue
                queue.extend(task.successors)


import asyncio
from typing import List


class SequentialExecutorMultipleWorkers(WorkflowExecutor):
    """Executes tasks concurrently (up to ``num_workers``) in BFS order."""

    def __init__(self, dag: DAG, num_workers: int = 4):
        """Create a new executor.

        Args:
            dag: The workflow DAG to execute.
            num_workers: Maximum number of concurrent worker coroutines.
        """
        # Initialize parent class (sets ``self.dag`` and bookkeeping structures).
        super().__init__(dag, num_workers)

        self.num_workers = num_workers
        # Per‑worker queues (not yet used for work‑stealing but available).
        self.worker_queues: List[asyncio.Queue] = [asyncio.Queue() for _ in range(num_workers)]
        # Optional ready task priority queue (min‑heap on priority).
        self.ready_queue: List[tuple[int, str]] = []

    async def run(self) -> None:
        """Run all tasks with up to ``self.num_workers`` workers in parallel."""
        visited = set()
        # Shared queue that workers pull from. We maintain BFS ordering by
        # enqueuing successors in sorted order.
        work_queue: asyncio.Queue[str] = asyncio.Queue()

        # Enqueue root tasks (no predecessors).
        roots = sorted(
            task_id
            for task_id, task in self.dag.tasks.items()
            if not task.predecessors
        )
        for task_id in roots:
            await work_queue.put(task_id)

        async def worker(worker_id: int):
            while True:
                task_id = await work_queue.get()
                if task_id is None:  # Shutdown signal.
                    work_queue.task_done()
                    break

                task = self.dag.tasks[task_id]

                # Re‑queue if dependencies are not yet complete.
                if not all(pred in self.completed_tasks for pred in task.predecessors):
                    await work_queue.put(task_id)
                    work_queue.task_done()
                    await asyncio.sleep(0)
                    continue

                # Execute the task.
                await self.execute_task(task_id, worker_id=worker_id)
                visited.add(task_id)

                # Enqueue successors so any worker can pick them up.
                for succ in sorted(task.successors):
                    if succ not in visited:
                        await work_queue.put(succ)

                work_queue.task_done()

        # Launch workers.
        worker_tasks = [asyncio.create_task(worker(i)) for i in range(self.num_workers)]

        # Wait for all tasks to be processed.
        await work_queue.join()

        # Signal shutdown.
        for _ in worker_tasks:
            await work_queue.put(None)
        await asyncio.gather(*worker_tasks)
