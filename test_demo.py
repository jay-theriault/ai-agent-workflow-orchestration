#!/usr/bin/env python3
"""
Demo script to test DAG execution with different scheduling strategies.
Shows the performance difference between sequential and parallel execution.
"""

import asyncio
import json
import os
from typing import Dict

from mock_dag import create_example_dag

from orchestrators.limited_concurrency import ParallelExecutor
from orchestrators.unlimited_concurrency import UnlimitedConcurrencyExecutor
from orchestrators.sequential_execution import SequentialExecutorMultipleWorkers

def print_dag_structure(dag):
    """Print the DAG structure for visualization"""
    print("\n=== DAG Structure ===")
    print(f"Total tasks: {len(dag.tasks)}")
    
    # Group tasks by type
    short_tasks = [t for t in dag.tasks if t.startswith('S')]
    medium_tasks = [t for t in dag.tasks if t.startswith('M')]
    critical_tasks = ['Gate', 'C1', 'C2', 'C3', 'End']
    
    print(f"\nShort tasks (1s each): {', '.join(sorted(short_tasks))}")
    print(f"Medium tasks (5s each): {', '.join(sorted(medium_tasks))}")
    print(f"Critical path: {' → '.join(critical_tasks)}")
    
    # Calculate theoretical minimum time
    critical_path_time = sum(dag.tasks[t].duration for t in critical_tasks if t in dag.tasks)
    print(f"\nTheoretical minimum time (critical path): {critical_path_time}s")
    print(f"Sequential worst case: ~{sum(t.duration for t in dag.tasks.values())}s")

async def run_single_test(executor_class, dag, num_workers=None, name=""):
    """Run a single test with given executor"""
    print(f"\n{'='*50}")
    print(f"Testing: {name}")
    print(f"{'='*50}")
    
    # Create executor
    if num_workers is not None:
        executor = executor_class(dag, num_workers=num_workers)
        print(f"Workers: {num_workers}")
    else:
        executor = executor_class(dag)
        print("Workers: Unlimited")
    
    # Run execution
    import time
    start_time = time.time()
    await executor.run()
    execution_time = time.time() - start_time
    
    # Get stats
    stats = executor.get_execution_stats()

    # Save logs
    executor.save_execution_log(f"logs/task_durations/{name}_log.json")
    
    # Print results
    print(f"\nExecution time: {execution_time:.2f} seconds")
    print(f"Completed tasks: {len(executor.completed_tasks)}/{len(dag.tasks)}")
    
    if 'critical_path' in stats and stats['critical_path']:
        print(f"Critical path identified: {' → '.join(stats['critical_path'])}")
        print(f"Critical path length: {stats['critical_path_length']:.1f}s")
    
    # Print worker utilization if available
    worker_utils = {k: v for k, v in stats.items() if k.startswith('worker_') and k.endswith('_utilization')}
    if worker_utils:
        print("\nWorker utilization:")
        for worker, util in sorted(worker_utils.items()):
            print(f"  {worker}: {util*100:.1f}%")
    
    return {
        'name': name,
        'execution_time': execution_time,
        'stats': stats,
        'completed_tasks': len(executor.completed_tasks)
    }

async def main():
    """Main demo function"""
    # Create directories
    os.makedirs("logs/task_durations", exist_ok=True)
    
    # Create the example DAG
    print("Creating example DAG...")
    dag = create_example_dag()
    print_dag_structure(dag)
    
    results = []
    
    # Test 1a+1b: Sequential execution (simulating BFS) Single and Multiple Workers
    dag1a = create_example_dag()
    result1a = await run_single_test(
        SequentialExecutorMultipleWorkers, 
        dag1a,
        num_workers=1,
        name="Sequential_Execution_1_Worker"  # Simulating BFS with single worker
    )
    results.append(result1a)

    dag1b = create_example_dag()
    result1b = await run_single_test(
        SequentialExecutorMultipleWorkers, 
        dag1b,
        num_workers=4,
        name="Sequential_Execution_4_Workers"  # Simulating BFS with multiple workers
    )
    results.append(result1b)
    
    # Test 2: Parallel with 4 workers (limited concurrency)
    dag2 = create_example_dag()
    result2 = await run_single_test(
        ParallelExecutor, 
        dag2, 
        num_workers=4,
        name="Optimized_Execution_4_Workers"
    )
    results.append(result2)
    
    # Test 3: Unlimited concurrency (external API scenario)
    dag3 = create_example_dag()
    result3 = await run_single_test(
        UnlimitedConcurrencyExecutor, 
        dag3,
        name="Parallel_Execution_Unlimited_Workers"
    )
    results.append(result3)
    
    # Print comparison
    print(f"\n{'='*50}")
    print("PERFORMANCE COMPARISON")
    print(f"{'='*50}")
    
    baseline = results[0]['execution_time']
    for result in results:
        speedup = (baseline / result['execution_time'] - 1) * 100
        print(f"\n{result['name']}:")
        print(f"  Time: {result['execution_time']:.2f}s")
        if speedup > 0:
            print(f"  Speedup: {speedup:.1f}% faster than sequential")
    
    # Save detailed results
    output_file = "logs/demo_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            'dag_info': {
                'total_tasks': len(dag.tasks),
                'task_durations': {tid: t.duration for tid, t in dag.tasks.items()}
            },
            'results': results
        }, f, indent=2)
    
    print(f"\nDetailed results saved to: {output_file}")
    
    # Print key insights
    print(f"\n{'='*50}")
    print("KEY INSIGHTS")
    print(f"{'='*50}")
    print("\n1. Sequential execution processes tasks in order without considering")
    print("   dependencies, leading to delayed critical path execution.")
    print("\n2. Optimized execution with critical path prioritization ensures")
    print("   the longest chain of tasks starts as early as possible.")
    print("\n3. With 4 workers, we achieve near-optimal performance despite")
    print("   limited resources by intelligent task prioritization.")
    print("\n4. Unlimited concurrency achieves theoretical minimum time")
    print("   (critical path length) when API rate limits aren't a factor.")

if __name__ == "__main__":
    asyncio.run(main())