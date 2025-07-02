from mock_dag import DAG, create_example_dag
from orchestrators.limited_concurrency import ParallelExecutor
from orchestrators.unlimited_concurrency import UnlimitedConcurrencyExecutor
from orchestrators.sequential_execution import SequentialExecutorMultipleWorkers
import time
import asyncio
import json


async def compare_executors():
    """Compare different execution strategies"""
    results = {}
    
    # Test Sequential Execution
    print("\n=== Sequential Execution (BFS) ===")
    dag = create_example_dag()
    executor = SequentialExecutorMultipleWorkers(dag)
    start = time.time()
    await executor.run()
    seq_time = time.time() - start
    results['sequential'] = {
        'time': seq_time,
        'stats': executor.get_execution_stats()
    }
    print(f"Time: {seq_time:.2f} seconds")
    executor.save_execution_log("logs/task_durations/sequential_run_4_workers.json")
    
    # Test Parallel Execution with 4 workers
    print("\n=== Parallel Execution (4 workers, Critical Path) ===")
    dag = create_example_dag()
    executor = ParallelExecutor(dag, num_workers=4)
    start = time.time()
    await executor.run()
    parallel_time = time.time() - start
    results['parallel_4_workers'] = {
        'time': parallel_time,
        'stats': executor.get_execution_stats()
    }
    print(f"Time: {parallel_time:.2f} seconds")
    executor.save_execution_log("logs/task_durations/parallel_4_workers_run.json")
    
    # Test Unlimited Concurrency
    print("\n=== Unlimited Concurrency Execution ===")
    dag = create_example_dag()
    executor = UnlimitedConcurrencyExecutor(dag)
    start = time.time()
    await executor.run()
    unlimited_time = time.time() - start
    results['unlimited'] = {
        'time': unlimited_time,
        'stats': executor.get_execution_stats()
    }
    print(f"Time: {unlimited_time:.2f} seconds")
    executor.save_execution_log("logs/task_durations/unlimited_run.json")
    
    # Print comparison
    print("\n=== Performance Comparison ===")
    print(f"Sequential: {results['sequential']['time']:.2f}s (baseline)")
    print(f"Parallel (4 workers): {results['parallel_4_workers']['time']:.2f}s "
          f"({(1 - results['parallel_4_workers']['time']/results['sequential']['time'])*100:.1f}% improvement)")
    print(f"Unlimited: {results['unlimited']['time']:.2f}s "
          f"({(1 - results['unlimited']['time']/results['sequential']['time'])*100:.1f}% improvement)")
    
    # Worker utilization for parallel execution
    if 'parallel_4_workers' in results:
        stats = results['parallel_4_workers']['stats']
        print("\n=== Worker Utilization (4 workers) ===")
        for i in range(4):
            util_key = f'worker_{i}_utilization'
            if util_key in stats:
                print(f"Worker {i}: {stats[util_key]*100:.1f}%")
    
    # Save comparison results
    with open("logs/task_durations/comparison_results.json", 'w') as f:
        json.dump(results, f, indent=2)



if __name__ == "__main__":
    import os
    os.makedirs("logs/task_durations", exist_ok=True)
    
    asyncio.run(compare_executors())