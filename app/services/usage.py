from ..database import get_db
from ..dynatrace import (
    query_host_full_stack_usage,
    query_host_infra_usage,
    query_real_user_monitoring_usage,
    query_real_user_monitoring_with_sr_usage,
    query_browser_monitor_usage,
    query_http_monitor_usage,
    query_3rd_party_monitor_usage
)
from ..models import DG, IS, Host
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..status import RefreshStatus

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import logging


DT_QUERIES_THREADS = 30


def retrieve_hosts_fullstack_usage(dgs=[]):
    results = []  # List to store the concatenated results
    task_queue = Queue()  # Queue to manage DGs to be processed

    # Enqueue all DGs into the task queue
    for dg in dgs:
        task_queue.put(dg)
        logger.debug(f'Enqueued DG: {dg}')

    with ThreadPoolExecutor(max_workers=DT_QUERIES_THREADS) as executor:
        futures = []  # List to keep track of futures
        future_to_dg = {}  # Dictionary to map futures to DGs

        # Start initial batch of tasks
        for _ in range(DT_QUERIES_THREADS):
            if not task_queue.empty():
                dg = task_queue.get()
                future = executor.submit(query_host_full_stack_usage, dg, "-30d", "now")
                futures.append(future)
                future_to_dg[future] = dg
                logger.debug(f'Started query for DG: {dg}')
        # Process completed futures and start new tasks as threads become available
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                dg = future_to_dg[future]
                try:
                    result = future.result()
                    results.extend(result)  # Concatenate results
                    logger.info(f'{dg} Completed (found {len(result)} FS datapoints)')
                except Exception as exc:
                    logger.error(f'{dg} generated an exception: {exc}')

                # Start a new task if there are more DGs to process
                if not task_queue.empty():
                    dg = task_queue.get()
                    future = executor.submit(query_host_full_stack_usage, dg, "-30d", "now")
                    futures.append(future)
                    future_to_dg[future] = dg
                    logger.debug(f'Started query for DG: {dg}')

    return results

def retrieve_hosts_infra_usage(dgs=[]):
    results = []  # List to store the concatenated results
    task_queue = Queue()  # Queue to manage DGs to be processed

    # Enqueue all DGs into the task queue
    for dg in dgs:
        task_queue.put(dg)
        logger.debug(f'Enqueued DG: {dg}')

    with ThreadPoolExecutor(max_workers=DT_QUERIES_THREADS) as executor:
        futures = []  # List to keep track of futures
        future_to_dg = {}  # Dictionary to map futures to DGs

        # Start initial batch of tasks
        for _ in range(DT_QUERIES_THREADS):
            if not task_queue.empty():
                dg = task_queue.get()
                future = executor.submit(query_host_infra_usage, dg, "-30d", "now")
                futures.append(future)
                future_to_dg[future] = dg
                logger.debug(f'Started query for DG: {dg}')

        # Process completed futures and start new tasks as threads become available
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                dg = future_to_dg[future]
                try:
                    result = future.result()
                    results.extend(result)  # Concatenate results
                    logger.info(f'{dg} Completed (found {len(result)} INFRA datapoints)')
                except Exception as exc:
                    logger.error(f'{dg} generated an exception: {exc}')

                # Start a new task if there are more DGs to process
                if not task_queue.empty():
                    dg = task_queue.get()
                    future = executor.submit(query_host_full_stack_usage, dg, "-30d", "now")
                    futures.append(future)
                    future_to_dg[future] = dg
                    logger.debug(f'Started query for DG: {dg}')
    return results

def retrieve_real_user_monitoring_usage(dgs=[]):
    results = []  # List to store the concatenated results
    task_queue = Queue()  # Queue to manage DGs to be processed

    # Enqueue all DGs into the task queue
    for dg in dgs:
        task_queue.put(dg)
        logger.debug(f'Enqueued DG: {dg}')

    with ThreadPoolExecutor(max_workers=DT_QUERIES_THREADS) as executor:
        futures = []  # List to keep track of futures
        future_to_dg = {}  # Dictionary to map futures to DGs

        # Start initial batch of tasks
        for _ in range(DT_QUERIES_THREADS):
            if not task_queue.empty():
                dg = task_queue.get()
                future = executor.submit(query_real_user_monitoring_usage, dg, "-30d", "now")
                futures.append(future)
                future_to_dg[future] = dg
                logger.debug(f'Started query for DG: {dg}')

        # Process completed futures and start new tasks as threads become available
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                dg = future_to_dg[future]
                try:
                    result = future.result()
                    results.extend(result)  # Concatenate results
                    logger.info(f'{dg} Completed (found {len(result)} RUM datapoints)')
                except Exception as exc:
                    logger.error(f'{dg} generated an exception: {exc}')

                # Start a new task if there are more DGs to process
                if not task_queue.empty():
                    dg = task_queue.get()
                    future = executor.submit(query_real_user_monitoring_usage, dg, "-30d", "now")
                    futures.append(future)
                    future_to_dg[future] = dg
                    logger.debug(f'Started query for DG: {dg}')
    return results

def retrieve_real_user_monitoring_with_sr_usage(dgs=[]):
    results = []  # List to store the concatenated results
    task_queue = Queue()  # Queue to manage DGs to be processed

    # Enqueue all DGs into the task queue
    for dg in dgs:
        task_queue.put(dg)
        logger.debug(f'Enqueued DG: {dg}')

    with ThreadPoolExecutor(max_workers=DT_QUERIES_THREADS) as executor:
        futures = []  # List to keep track of futures
        future_to_dg = {}  # Dictionary to map futures to DGs

        # Start initial batch of tasks
        for _ in range(DT_QUERIES_THREADS):
            if not task_queue.empty():
                dg = task_queue.get()
                future = executor.submit(query_real_user_monitoring_with_sr_usage, dg, "-30d", "now")
                futures.append(future)
                future_to_dg[future] = dg
                logger.debug(f'Started query for DG: {dg}')

        # Process completed futures and start new tasks as threads become available
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                dg = future_to_dg[future]
                try:
                    result = future.result()
                    results.extend(result)  # Concatenate results
                    logger.info(f'{dg} Completed (found {len(result)} RUM+SR datapoints)')
                except Exception as exc:
                    logger.error(f'{dg} generated an exception: {exc}')

                # Start a new task if there are more DGs to process
                if not task_queue.empty():
                    dg = task_queue.get()
                    future = executor.submit(query_real_user_monitoring_with_sr_usage, dg, "-30d", "now")
                    futures.append(future)
                    future_to_dg[future] = dg
                    logger.debug(f'Started query for DG: {dg}')
    return results

def retrieve_browser_monitor_usage(dgs=[]):
    results = []  # List to store the concatenated results
    task_queue = Queue()  # Queue to manage DGs to be processed

    # Enqueue all DGs into the task queue
    for dg in dgs:
        task_queue.put(dg)
        logger.debug(f'Enqueued DG: {dg}')

    with ThreadPoolExecutor(max_workers=DT_QUERIES_THREADS) as executor:
        futures = []  # List to keep track of futures
        future_to_dg = {}  # Dictionary to map futures to DGs

        # Start initial batch of tasks
        for _ in range(DT_QUERIES_THREADS):
            if not task_queue.empty():
                dg = task_queue.get()
                future = executor.submit(query_browser_monitor_usage, dg, "-30d", "now")
                futures.append(future)
                future_to_dg[future] = dg
                logger.debug(f'Started query for DG: {dg}')

        # Process completed futures and start new tasks as threads become available
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                dg = future_to_dg[future]
                try:
                    result = future.result()
                    results.extend(result)  # Concatenate results
                    logger.info(f'{dg} Completed (found {len(result)} browser monitor datapoints)')
                except Exception as exc:
                    logger.error(f'{dg} generated an exception: {exc}')

                # Start a new task if there are more DGs to process
                if not task_queue.empty():
                    dg = task_queue.get()
                    future = executor.submit(query_browser_monitor_usage, dg, "-30d", "now")
                    futures.append(future)
                    future_to_dg[future] = dg
                    logger.debug(f'Started query for DG: {dg}')
    return results

def retrieve_http_monitor_usage(dgs=[]):
    results = []  # List to store the concatenated results
    task_queue = Queue()  # Queue to manage DGs to be processed

    # Enqueue all DGs into the task queue
    for dg in dgs:
        task_queue.put(dg)
        logger.debug(f'Enqueued DG: {dg}')

    with ThreadPoolExecutor(max_workers=DT_QUERIES_THREADS) as executor:
        futures = []  # List to keep track of futures
        future_to_dg = {}  # Dictionary to map futures to DGs

        # Start initial batch of tasks
        for _ in range(DT_QUERIES_THREADS):
            if not task_queue.empty():
                dg = task_queue.get()
                future = executor.submit(query_http_monitor_usage, dg, "-30d", "now")
                futures.append(future)
                future_to_dg[future] = dg
                logger.debug(f'Started query for DG: {dg}')

        # Process completed futures and start new tasks as threads become available
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                dg = future_to_dg[future]
                try:
                    result = future.result()
                    results.extend(result)  # Concatenate results
                    logger.info(f'{dg} Completed (found {len(result)} HTTP monitor datapoints)')
                except Exception as exc:
                    logger.error(f'{dg} generated an exception: {exc}')

                # Start a new task if there are more DGs to process
                if not task_queue.empty():
                    dg = task_queue.get()
                    future = executor.submit(query_http_monitor_usage, dg, "-30d", "now")
                    futures.append(future)
                    future_to_dg[future] = dg
                    logger.debug(f'Started query for DG: {dg}')
    return results

def retrieve_3rd_party_monitor_usage(dgs=[]):
    results = []  # List to store the concatenated results
    task_queue = Queue()  # Queue to manage DGs to be processed

    # Enqueue all DGs into the task queue
    for dg in dgs:
        task_queue.put(dg)
        logger.debug(f'Enqueued DG: {dg}')

    with ThreadPoolExecutor(max_workers=DT_QUERIES_THREADS) as executor:
        futures = []  # List to keep track of futures
        future_to_dg = {}  # Dictionary to map futures to DGs

        # Start initial batch of tasks
        for _ in range(DT_QUERIES_THREADS):
            if not task_queue.empty():
                dg = task_queue.get()
                future = executor.submit(query_3rd_party_monitor_usage, dg, "-30d", "now")
                futures.append(future)
                future_to_dg[future] = dg
                logger.debug(f'Started query for DG: {dg}')

        # Process completed futures and start new tasks as threads become available
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                dg = future_to_dg[future]
                try:
                    result = future.result()
                    results.extend(result)  # Concatenate results
                    logger.info(f'{dg} Completed (found {len(result)} 3rd party monitor datapoints)')
                except Exception as exc:
                    logger.error(f'{dg} generated an exception: {exc}')

                # Start a new task if there are more DGs to process
                if not task_queue.empty():
                    dg = task_queue.get()
                    future = executor.submit(query_3rd_party_monitor_usage, dg, "-30d", "now")
                    futures.append(future)
                    future_to_dg[future] = dg
                    logger.debug(f'Started query for DG: {dg}')
    return results