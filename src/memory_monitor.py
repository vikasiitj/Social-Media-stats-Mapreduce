#!/usr/bin/env python3
"""
Memory Usage Monitor for MapReduce Workflow

This script monitors memory usage during MapReduce job execution.
It can be used as a wrapper around mappers and reducers to track
resource utilization.

Usage:
    python3 memory_monitor.py python3 actual_script.py

This will run actual_script.py while monitoring its memory usage.
"""

import os
import time
import psutil
import sys
import subprocess


def monitor_process(pid, interval=5):
    """
    Monitor the memory usage of a process.

    Args:
        pid: Process ID to monitor
        interval: Monitoring interval in seconds
    """
    process = psutil.Process(pid)

    # Memory tracking
    peak_memory = 0
    start_time = time.time()

    try:
        while process.is_running():
            # Get memory info
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            if memory_mb > peak_memory:
                peak_memory = memory_mb

            elapsed_time = time.time() - start_time

            # Log performance metrics
            sys.stderr.write(f"Memory usage: {memory_mb:.2f} MB, " +
                             f"Time elapsed: {elapsed_time:.2f} s\n")

            time.sleep(interval)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    # Final stats
    sys.stderr.write(f"reporter:counter:PerformanceMetrics,PeakMemoryMB,{int(peak_memory)}\n")


def monitor_stdin_process(interval=5):
    """
    Monitor memory usage while processing stdin/stdout.

    Args:
        interval: Monitoring interval in seconds
    """
    # Get process ID
    pid = os.getpid()
    process = psutil.Process(pid)

    # Memory tracking
    peak_memory = 0
    start_time = time.time()
    record_count = 0

    # Process stdin
    for line in sys.stdin:
        record_count += 1

        # Forward the line to stdout
        print(line.strip())

        # Check memory every 1000 records
        if record_count % 1000 == 0:
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            if memory_mb > peak_memory:
                peak_memory = memory_mb

            elapsed_time = time.time() - start_time
            records_per_second = record_count / elapsed_time if elapsed_time > 0 else 0

            # Log performance metrics
            sys.stderr.write(f"Memory usage: {memory_mb:.2f} MB, Records processed: {record_count}, " +
                             f"Time elapsed: {elapsed_time:.2f} s, Records/sec: {records_per_second:.2f}\n")

    # Final stats
    sys.stderr.write(f"reporter:counter:PerformanceMetrics,PeakMemoryMB,{int(peak_memory)}\n")
    sys.stderr.write(f"reporter:counter:PerformanceMetrics,TotalRecords,{record_count}\n")


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        # Run the specified command and monitor it
        command = sys.argv[1:]
        process = subprocess.Popen(command)

        # Monitor the subprocess
        monitor_process(process.pid)

        # Wait for the process to complete
        process.wait()
        return process.returncode
    else:
        # If no command provided, monitor stdin/stdout processing
        monitor_stdin_process()
        return 0


if __name__ == "__main__":
    sys.exit(main())