from mock_dag import create_example_dag
from orchestrators.sequential_execution import SequentialExecutorMultipleWorkers
import time
import asyncio
import json

# Example usage
async def test_dag_execution():
    """Test the DAG execution with mock API calls"""
    dag = create_example_dag()
    
    # Test sequential execution
    print("Testing Sequential Execution...")
    executor = SequentialExecutorMultipleWorkers(dag)
    start = time.time()
    await executor.run()
    seq_time = time.time() - start
    
    print(f"Sequential execution time: {seq_time:.2f} seconds")
    print(f"Completed tasks: {len(executor.completed_tasks)}")
    
    # Save logs
    executor.save_execution_log(f"logs/task_durations/sequential_run_4_workers.json")
    
    # Print stats
    stats = executor.get_execution_stats()
    print(f"Stats: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # Create necessary directories
    import os
    os.makedirs("logs/task_durations", exist_ok=True)
    
    # Run test
    asyncio.run(test_dag_execution())