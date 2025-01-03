from database import get_db
from dynatrace import (
    query_host_full_stack_usage,
    query_host_infra_usage,
    query_real_user_monitoring_usage,
    query_real_user_monitoring_with_sr_usage,
    query_browser_monitor_usage,
    query_http_monitor_usage,
    query_3rd_party_monitor_usage,
    query_unassigned_host_full_stack_usage,
    query_unassigned_host_infra_usage,
    query_unassigned_real_user_monitoring_usage,
    query_unassigned_real_user_monitoring_with_sr_usage,
    query_unassigned_browser_monitor_usage,
    query_unassigned_http_monitor_usage,
    query_unassigned_3rd_party_monitor_usage
)
from models import DG, Host, IS
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue
from settings import DT_QUERIES_THREADS
from settings import LOG_FORMAT, LOG_LEVEL
from sqlalchemy.orm import Session
from typing import Dict
import logging

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



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
                    logger.info(f'FS usage query for {dg} Completed (found {len(result)} FS datapoints)')
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
                    logger.info(f'INFRA usage query for {dg} Completed (found {len(result)} INFRA datapoints)')
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
                    logger.info(f'RUM usage query for {dg} Completed (found {len(result)} RUM datapoints)')
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
                    logger.info(f'RUM+SR usage query for {dg} Completed (found {len(result)} RUM+SR datapoints)')
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
                    logger.info(f'BROWSER MON. usage query for {dg} Completed (found {len(result)} browser monitor datapoints)')
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
                    logger.info(f'HTTP MON. usage query for {dg} Completed (found {len(result)} HTTP monitor datapoints)')
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
                    logger.info(f'EXT. MON. usage query for {dg} Completed (found {len(result)} 3rd party monitor datapoints)')
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


def retrieve_unassigned_hosts_fullstack_usage():
    try:
        result = query_unassigned_host_full_stack_usage("-30d", "now")
        logger.info(f'Completed unassigned fullstack query (found {len(result)} datapoints)')
        return result
    except Exception as exc:
        logger.error(f'Unassigned fullstack query generated an exception: {exc}')
        return []

def retrieve_unassigned_hosts_infra_usage():
    try:
        result = query_unassigned_host_infra_usage("-30d", "now")
        logger.info(f'Completed unassigned infra query (found {len(result)} datapoints)')
        return result
    except Exception as exc:
        logger.error(f'Unassigned infra query generated an exception: {exc}')
        return []

def retrieve_unassigned_real_user_monitoring_usage():
    try:
        result = query_unassigned_real_user_monitoring_usage("-30d", "now")
        logger.info(f'Completed unassigned RUM query (found {len(result)} datapoints)')
        return result
    except Exception as exc:
        logger.error(f'Unassigned RUM query generated an exception: {exc}')
        return []

def retrieve_unassigned_real_user_monitoring_with_sr_usage():
    try:
        result = query_unassigned_real_user_monitoring_with_sr_usage("-30d", "now")
        logger.info(f'Completed unassigned RUM+SR query (found {len(result)} datapoints)')
        return result
    except Exception as exc:
        logger.error(f'Unassigned RUM+SR query generated an exception: {exc}')
        return []

def retrieve_unassigned_browser_monitor_usage():
    try:
        result = query_unassigned_browser_monitor_usage("-30d", "now")
        logger.info(f'Completed unassigned browser monitor query (found {len(result)} datapoints)')
        return result
    except Exception as exc:
        logger.error(f'Unassigned browser monitor query generated an exception: {exc}')
        return []

def retrieve_unassigned_http_monitor_usage():
    try:
        result = query_unassigned_http_monitor_usage("-30d", "now")
        logger.info(f'Completed unassigned HTTP monitor query (found {len(result)} datapoints)')
        return result
    except Exception as exc:
        logger.error(f'Unassigned HTTP monitor query generated an exception: {exc}')
        return []

def retrieve_unassigned_3rd_party_monitor_usage():
    try:
        result = query_unassigned_3rd_party_monitor_usage("-30d", "now")
        logger.info(f'Completed unassigned 3rd party monitor query (found {len(result)} datapoints)')
        return result
    except Exception as exc:
        logger.error(f'Unassigned 3rd party monitor query generated an exception: {exc}')
        return []

