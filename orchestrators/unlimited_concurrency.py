import asyncio
from mock_dag import WorkflowExecutor
import time
import json

class UnlimitedConcurrencyExecutor(WorkflowExecutor):
    """Executes tasks with unlimited concurrency (external API scenario)"""
    
    async def run(self):
        """Run all ready tasks immediately when dependencies are met"""
        active_tasks = {}
        
        while len(self.completed_tasks) < len(self.dag.tasks):
            # Find all ready tasks
            ready_tasks = self.dag.get_ready_tasks(self.completed_tasks)
            
            # Start all ready tasks that aren't already running
            for task_id in ready_tasks:
                if task_id not in active_tasks and task_id not in self.completed_tasks:
                    # Start task
                    task_future = asyncio.create_task(self.execute_task(task_id))
                    active_tasks[task_id] = task_future
            
            # Wait for at least one task to complete
            if active_tasks:
                done, pending = await asyncio.wait(
                    active_tasks.values(), 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Remove completed tasks from active set
                completed_ids = []
                for task_future in done:
                    for task_id, future in active_tasks.items():
                        if future == task_future:
                            completed_ids.append(task_id)
                            break
                
                for task_id in completed_ids:
                    del active_tasks[task_id]
            else:
                # No active tasks and no ready tasks - shouldn't happen
                break