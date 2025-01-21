from settings import BASE_URL, DT_TOKEN, LOG_FORMAT, LOG_LEVEL, USER_AGENT
import logging
import os
import requests

from settings import root_logger
logger = root_logger


def get_host_tags():
    """
    Retrieve tags for all hosts from Dynatrace.
    
    Returns:
        dict: JSON response containing host tags.
    """
    logger.debug("Starting host tags retrieval from Dynatrace")
    url = f"{BASE_URL}/api/v2/tags"
    headers = {
        "Authorization": f"Api-Token {DT_TOKEN}",
        "User-Agent": USER_AGENT
    }
    params = {"entitySelector": "type(HOST)"}
    
    logger.debug(f"Making request to {url}")
    logger.debug(f"Using params: {params}")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        logger.error(f"Request failed with status code {response.status_code}")
        raise Exception(f"API request failed: {response.text}")
        
    data = response.json()
    #logger.debug(f"Received response data: {data}")
    
    if "tags" in data:
        logger.info(f"Retrieved {len(data['tags'])} tags")
    else:
        logger.warning("No tags found in response")
        
    return data

def get_hosts():
    """
    Retrieve data for all hosts from Dynatrace.
    
    Returns:
        dict: JSON response containing host data.
    """
    logger.debug("Starting host data retrieval from Dynatrace")
    
    url = f"{BASE_URL}/api/v2/entities"
    params = {
        "entitySelector": "type(HOST)", 
        "pageSize": 4000,
        "fields": "tags, properties.monitoringMode, properties.physicalMemory, properties.state",
        "from": "-30d",
        "to": "now"
    }
    headers = {
        "Authorization": f"Api-Token {DT_TOKEN}",
        "User-Agent": USER_AGENT
    }
    
    logger.debug(f"Making initial request to {url}")
    logger.debug(f"Using params: {params}")
    
    all_hosts = []
    page = 1
    
    while True:
        logger.info(f"Fetching page {page} of hosts")
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Request failed with status code {response.status_code}")
            raise Exception(f"API request failed: {response.text}")
            
        data = response.json()
        #logger.debug(f"Received response data: {data}")
        
        if "totalCount" in data:
            total_count = data["totalCount"]
            logger.info(f"Total host count: {total_count}")
            if total_count == 0:
                logger.info("No hosts found")
                return {"entities": []}
        
        if "entities" in data:
            new_hosts = data["entities"]
            logger.info(f"Retrieved {len(new_hosts)} hosts on page {page}")
            all_hosts.extend(new_hosts)
            
            # Stop if we've retrieved all hosts based on total count
            if len(all_hosts) >= total_count:
                logger.info("Retrieved all available hosts")
                break
            
        if "nextPageKey" not in data:
            logger.info("No more pages to fetch")
            break
            
        # Update params for next page
        params = {"nextPageKey": data["nextPageKey"]}
        page += 1
        logger.debug(f"Next page key: {data['nextPageKey']}")
        
    logger.info(f"Completed host retrieval. Total hosts retrieved: {len(all_hosts)}")
    return {"entities": all_hosts}

def get_applications():
    """
    Retrieve data for all applications from Dynatrace.
    
    Returns:
        dict: JSON response containing application data.
    """
    logger.debug("Starting application data retrieval from Dynatrace")
    url = f"{BASE_URL}/api/v2/entities"
    params = {
        "entitySelector": "type(APPLICATION)",
        "fields": "properties.applicationType, tags",
        "pageSize": 4000,
        "from": "-30d",
        "to": "now"
    }
    headers = {
        "Authorization": f"Api-Token {DT_TOKEN}",
        "User-Agent": USER_AGENT
    }
    
    logger.debug(f"Making request to {url}")
    logger.debug(f"Using params: {params}")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        logger.error(f"Request failed with status code {response.status_code}")
        raise Exception(f"API request failed: {response.text}")
    
    data = response.json()
    #logger.debug(f"Received response data: {data}")
    
    return data

def get_synthetics():
    """
    Retrieve data for all synthetic tests from Dynatrace.
    
    Returns:
        dict: JSON response containing synthetic test data.
    """
    logger.debug("Starting synthetic data retrieval from Dynatrace")
    synthetic_types = ["SYNTHETIC_TEST", "HTTP_CHECK", "EXTERNAL_SYNTHETIC_TEST"]
    all_synthetics = []

    for synthetic_type in synthetic_types:
        logger.info(f"Retrieving synthetic data for type {synthetic_type}")
        url = f"{BASE_URL}/api/v2/entities"
        params = {
            "entitySelector": f"type({synthetic_type})",
            "fields": "properties, tags",
            "pageSize": 4000,
            "from": "-30d",
            "to": "now"
        }
        headers = {
            "Authorization": f"Api-Token {DT_TOKEN}",
            "User-Agent": USER_AGENT
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            #logger.debug(f"Received response data: {data}")
            
            if "entities" in data:
                # Add type to each entity
                for entity in data["entities"]:
                    entity["type"] = synthetic_type
                all_synthetics.extend(data["entities"])
                logger.info(f"Retrieved {len(data['entities'])} {synthetic_type} synthetics")
        else:
            logger.error(f"Failed to retrieve {synthetic_type} synthetics: {response.status_code}")

    logger.info(f"Completed synthetic retrieval. Total synthetics retrieved: {len(all_synthetics)}")
    return {"entities": all_synthetics}

def query_metric(metricSelector=None, resolution="1h", data_from="-30d", data_to="now"):
    """
    Retrieve metric datapoints using metricselector expression
    
    Returns:
        dict: JSON response containing host data.
    """
    #logger.debug("Starting metrics API query to Dynatrace")
    
    url = f"{BASE_URL}/api/v2/metrics/query"
    params = {
        "metricSelector": metricSelector, 
        "resolution": resolution,
        "from": data_from,
        "to": data_to
    }
    headers = {
        "Authorization": f"Api-Token {DT_TOKEN}",
        "User-Agent": USER_AGENT
    }
    
    #logger.debug(f"Making initial request to {url}")
    #logger.debug(f"Using params: {params}")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        logger.error(f"Request failed with status code {response.status_code}")
        raise Exception(f"API request failed: {response.text}")
        
    data = response.json()
    
    return data["result"][0]["data"]

def query_host_full_stack_usage(dg=None, data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.full_stack_monitoring.usage_per_host
        :filter(
            and(
                or(
                    in("dt.entity.host",entitySelector("type(HOST),tag(~"DG\:{dg}~")")),
                    in("dt.entity.host",entitySelector("type(host),tag(~"DG:{dg}~")"))
                )
            )
        )
        :fold(sum)
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][0],
                       'value': datapoint['values'][0]}
                    )
    return result

def query_host_infra_usage(dg=None, data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.infrastructure_monitoring.usage_per_host
        :filter(
            and(
                or(
                    in("dt.entity.host",entitySelector("type(HOST),tag(~"DG\:{dg}~")")),
                    in("dt.entity.host",entitySelector("type(host),tag(~"DG:{dg}~")"))
                )
            )
        )
        :fold(sum)
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][0],
                       'value': datapoint['values'][0]}
                    )
    return result

def query_real_user_monitoring_usage(dg=None, data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.real_user_monitoring.web.session.usage_by_app
        :filter(
            and(
                or(
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG\:{dg}~")")),
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG:{dg}~")"))
                )
            )
        )
        :fold(sum)
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][0],
                       'value': datapoint['values'][0]}
                    )
    return result

def query_real_user_monitoring_with_sr_usage(dg=None, data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.real_user_monitoring.web.session_with_replay.usage_by_app
        :filter(
            and(
                or(
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG\:{dg}~")")),
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG:{dg}~")"))
                )
            )
        )
        :fold(sum)
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][0],
                       'value': datapoint['values'][0]}
                    )
    return result

def query_browser_monitor_usage(dg=None, data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.synthetic.actions.usage_by_browser_monitor
        :filter(
            and(
                or(
                    in("dt.entity.synthetic_test",entitySelector("type(~"SYNTHETIC_TEST~"),tag(~"DG:{dg}~")")),
                    in("dt.entity.synthetic_test",entitySelector("type(~"SYNTHETIC_TEST~"),tag(~"DG\:{dg}~")"))
                )
            )
        )
        :fold(sum)
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][0],
                       'value': datapoint['values'][0]}
                    )
    return result

def query_http_monitor_usage(dg=None, data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.synthetic.requests.usage_by_http_monitor
        :filter(
            and(
                or(
                    in("dt.entity.http_check",entitySelector("type(~"HTTP_CHECK~"),tag(~"DG:{dg}~")")),
                    in("dt.entity.http_check",entitySelector("type(~"HTTP_CHECK~"),tag(~"DG\:{dg}~")"))
                )
            )
        )
        :fold(sum)
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][0],
                       'value': datapoint['values'][0]}
                    )
    return result

def query_3rd_party_monitor_usage(dg=None, data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.synthetic.external.usage_by_third_party_monitor
        :filter(
            and(
                or(
                    in("dt.entity.external_synthetic_test",entitySelector("type(~"EXTERNAL_SYNTHETIC_TEST~"),tag(~"DG\:{dg}~")")),
                    in("dt.entity.external_synthetic_test",entitySelector("type(~"EXTERNAL_SYNTHETIC_TEST~"),tag(~"DG:{dg}~")"))
                )
            )
        )
        :fold(sum)
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][0],
                       'value': datapoint['values'][0]}
                    )
    return result



def query_unassigned_host_full_stack_usage(data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.full_stack_monitoring.usage_per_host
        :filter(
            not(and(
                or(
                    in("dt.entity.host",entitySelector("type(HOST),tag(~"DG\:~")")),
                    in("dt.entity.host",entitySelector("type(host),tag(~"DG:~")"))
                )
            ))
        )
        :fold(sum):splitBy(dt.entity.host):names
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][1],
                       'value': datapoint['values'][0], 'name': datapoint['dimensions'][0]}
                    )
    return result

def query_unassigned_host_infra_usage(data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.infrastructure_monitoring.usage_per_host
        :filter(
            and(
                not(or(
                    in("dt.entity.host",entitySelector("type(HOST),tag(~"DG\:~")")),
                    in("dt.entity.host",entitySelector("type(host),tag(~"DG:~")"))
                ))
            )
        )
        :fold(sum):names
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][1],
                       'value': datapoint['values'][0], 'name': datapoint['dimensions'][0]}
                    )
    return result

def query_unassigned_real_user_monitoring_usage(data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.real_user_monitoring.web.session.usage_by_app
        :filter(
            and(
                not(or(
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG\:~")")),
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG:~")"))
                )),
                in("dt.entity.application",entitySelector("type(APPLICATION)"))
            )
        )
        :fold(sum):names
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][1],
                       'value': datapoint['values'][0], 'name': datapoint['dimensions'][0]}
                    )
    return result

def query_unassigned_real_user_monitoring_with_sr_usage(data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.real_user_monitoring.web.session_with_replay.usage_by_app
        :filter(
            and(
                not(or(
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG\:~")")),
                    in("dt.entity.application",entitySelector("type(APPLICATION),tag(~"DG:~")"))
                )),
                in("dt.entity.application",entitySelector("type(APPLICATION)"))
            )
        )
        :fold(sum):names
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][1],
                       'value': datapoint['values'][0], 'name': datapoint['dimensions'][0]}
                    )
    return result

def query_unassigned_browser_monitor_usage(data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.synthetic.actions.usage_by_browser_monitor
        :filter(
            and(
                not(or(
                    in("dt.entity.synthetic_test",entitySelector("type(~"SYNTHETIC_TEST~"),tag(~"DG:~")")),
                    in("dt.entity.synthetic_test",entitySelector("type(~"SYNTHETIC_TEST~"),tag(~"DG\:~")"))
                )),
                in("dt.entity.synthetic_test",entitySelector("type(~"SYNTHETIC_TEST~")"))
            )
        )
        :fold(sum):names
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][1],
                       'value': datapoint['values'][0], 'name': datapoint['dimensions'][0]}
                    )
    return result

def query_unassigned_http_monitor_usage(data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.synthetic.requests.usage_by_http_monitor
        :filter(
            and(
                not(or(
                    in("dt.entity.http_check",entitySelector("type(~"HTTP_CHECK~"),tag(~"DG:~")")),
                    in("dt.entity.http_check",entitySelector("type(~"HTTP_CHECK~"),tag(~"DG\:~")"))
                )),
                in("dt.entity.http_check",entitySelector("type(~"HTTP_CHECK~")"))
            )
        )
        :fold(sum):names
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][1],
                       'value': datapoint['values'][0], 'name': datapoint['dimensions'][0]}
                    )
    return result

def query_unassigned_3rd_party_monitor_usage(data_from="-30d", data_to="now"):
    metricSelector = rf"""
        builtin:billing.synthetic.external.usage_by_third_party_monitor
        :filter(
            and(
                not(or(
                    in("dt.entity.external_synthetic_test",entitySelector("type(~"EXTERNAL_SYNTHETIC_TEST~"),tag(~"DG\:~")")),
                    in("dt.entity.external_synthetic_test",entitySelector("type(~"EXTERNAL_SYNTHETIC_TEST~"),tag(~"DG:~")"))
                )),
                in("dt.entity.external_synthetic_test",entitySelector("type(~"EXTERNAL_SYNTHETIC_TEST~")"))
            )
        )
        :fold(sum):names
    """
    data = query_metric(metricSelector=metricSelector, resolution="1h", data_from=data_from, data_to=data_to)
    result = []
    for datapoint in data:
        result.append({'dt_id': datapoint['dimensions'][1],
                       'value': datapoint['values'][0], 'name': datapoint['dimensions'][0]}
                    )
    return result
