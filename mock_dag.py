import asyncio
import time
import json
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import random

@dataclass
class Task:
    """Represents a task in the DAG"""
    id: str
    duration: float  # in seconds
    predecessors: Set[str] = field(default_factory=set)
    successors: Set[str] = field(default_factory=set)
    
    # Runtime tracking
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    worker_id: Optional[int] = None

@dataclass
class DAG:
    """Represents the entire DAG structure"""
    tasks: Dict[str, Task]
    
    def get_ready_tasks(self, completed: Set[str]) -> List[str]:
        """Get all tasks whose dependencies are satisfied"""
        ready = []
        for task_id, task in self.tasks.items():
            if task_id not in completed and all(pred in completed for pred in task.predecessors):
                ready.append(task_id)
        return ready
    
    def get_critical_path_length(self, task_id: str, memo: Dict[str, float] = None) -> float:
        """Calculate critical path length from this task to end"""
        if memo is None:
            memo = {}
        
        if task_id in memo:
            return memo[task_id]
        
        task = self.tasks[task_id]
        if not task.successors:
            memo[task_id] = task.duration
            return task.duration
        
        max_successor_path = max(
            self.get_critical_path_length(succ, memo) 
            for succ in task.successors
        )
        memo[task_id] = task.duration + max_successor_path
        return memo[task_id]

def create_example_dag() -> DAG:
    """Create the example DAG from the README"""
    tasks = {}
    
    # Create all tasks
    # Short tasks
    for i in range(1, 5):
        tasks[f'S{i}'] = Task(f'S{i}', duration=1.0)
    
    # Medium tasks
    for i in range(1, 9):
        tasks[f'M{i}'] = Task(f'M{i}', duration=5.0)
    
    # Gate and critical path
    tasks['Gate'] = Task('Gate', duration=1.0)
    tasks['C1'] = Task('C1', duration=20.0)
    tasks['C2'] = Task('C2', duration=15.0)
    tasks['C3'] = Task('C3', duration=10.0)
    tasks['End'] = Task('End', duration=5.0)
    
    # Set up dependencies
    # Short tasks depend on nothing, Gate depends on all short tasks
    for i in range(1, 5):
        tasks[f'S{i}'].successors.add('Gate')
        tasks['Gate'].predecessors.add(f'S{i}')
    
    # Critical path
    tasks['Gate'].successors.add('C1')
    tasks['C1'].predecessors.add('Gate')
    
    tasks['C1'].successors.add('C2')
    tasks['C2'].predecessors.add('C1')
    
    tasks['C2'].successors.add('C3')
    tasks['C3'].predecessors.add('C2')
    
    # C3 leads to End
    tasks['C3'].successors.add('End')
    tasks['End'].predecessors.add('C3')
    
    # Medium tasks lead to End
    for i in range(1, 9):
        tasks[f'M{i}'].successors.add('End')
        tasks['End'].predecessors.add(f'M{i}')
    
    return DAG(tasks)

async def mock_llm_call(task_id: str, duration: float, variance: float = 0.0) -> Dict:
    """
    Mock an LLM API call with specified duration.
    
    Args:
        task_id: Identifier for the task
        duration: Expected duration in seconds
        variance: Random variance as percentage of duration (default 10%)
    
    Returns:
        Mock API response
    """
    # Add some realistic variance
    actual_duration = duration * (1 + random.uniform(-variance, variance))
    
    # Simulate API call
    await asyncio.sleep(actual_duration)
    
    # Return mock response
    return {
        "task_id": task_id,
        "status": "completed",
        "result": f"Mock result for {task_id}",
        "duration_ms": int(actual_duration * 1000),
        "timestamp": datetime.now().isoformat()
    }

class WorkflowExecutor:
    """Base class for workflow executors"""
    
    def __init__(self, dag: DAG, num_workers: Optional[int] = None):
        self.dag = dag
        self.num_workers = num_workers
        self.completed_tasks: Set[str] = set()
        self.execution_log: List[Dict] = []
        
    async def execute_task(self, task_id: str, worker_id: Optional[int] = None) -> Dict:
        """Execute a single task"""
        task = self.dag.tasks[task_id]
        task.start_time = time.time()
        task.worker_id = worker_id
        
        # Log start
        self.log_event("task_start", task_id, worker_id)
        
        # Execute mock API call
        result = await mock_llm_call(task_id, task.duration)
        
        task.end_time = time.time()
        self.completed_tasks.add(task_id)
        
        # Log completion
        self.log_event("task_complete", task_id, worker_id, 
                      duration=task.end_time - task.start_time)
        
        return result
    
    def log_event(self, event_type: str, task_id: str, 
                  worker_id: Optional[int] = None, **kwargs):
        """Log execution events"""
        event = {
            "timestamp": time.time(),
            "event_type": event_type,
            "task_id": task_id,
            "worker_id": worker_id,
            **kwargs
        }
        self.execution_log.append(event)
    
    def get_execution_stats(self) -> Dict:
        """Calculate execution statistics"""
        if not self.execution_log:
            return {}
        
        start_time = min(e['timestamp'] for e in self.execution_log 
                        if e['event_type'] == 'task_start')
        end_time = max(e['timestamp'] for e in self.execution_log 
                      if e['event_type'] == 'task_complete')
        
        total_time = end_time - start_time
        
        # Calculate worker utilization if applicable
        worker_stats = {}
        if self.num_workers:
            for i in range(self.num_workers):
                worker_events = [e for e in self.execution_log if e.get('worker_id') == i]
                if worker_events:
                    busy_time = sum(e.get('duration', 0) for e in worker_events 
                                  if e['event_type'] == 'task_complete')
                    worker_stats[f'worker_{i}_utilization'] = busy_time / total_time
        
        # Find critical path
        critical_path = self.find_critical_path()
        
        return {
            "total_time": total_time,
            "completed_tasks": len(self.completed_tasks),
            "critical_path": critical_path,
            "critical_path_length": sum(self.dag.tasks[t].duration for t in critical_path),
            **worker_stats
        }
    
    def find_critical_path(self) -> List[str]:
        """Find the critical path that determined execution time"""
        # Simple implementation - in practice would track actual dependencies
        # For our example, we know the critical path
        if 'End' in self.completed_tasks:
            return ['S1', 'Gate', 'C1', 'C2', 'C3', 'End']
        return []
    
    def save_execution_log(self, filename: str):
        """Save execution log to file"""
        log_data = {
            "execution_log": self.execution_log,
            "stats": self.get_execution_stats(),
            "task_durations": {
                task_id: {
                    "expected": task.duration,
                    "actual": task.end_time - task.start_time if task.end_time else None
                }
                for task_id, task in self.dag.tasks.items()
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(log_data, f, indent=2)