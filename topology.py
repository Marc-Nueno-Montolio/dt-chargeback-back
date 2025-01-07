from database import get_db
from dynatrace import get_applications, get_host_tags, get_hosts, get_synthetics
from models import Application, DG, Host, IS, Synthetic
from settings import LOG_FORMAT, LOG_LEVEL, TOPOLOGY_REFRESH_THREADS
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from enum import Enum
from functools import partial
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from threading import Lock
from typing import Dict, List
from typing import Literal
import json
import logging
import re
from chargeback_logic import host_is_managed, is_is_managed

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



class RefreshStatus(str, Enum):
    IDLE = 'idle'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'


            
# Global variable to track refresh status
topology_refresh_status = {
    entity: {"status": RefreshStatus.IDLE, "last_update": None}
    for entity in ["dgs", "hosts", "applications", "synthetics"]
}

# Lock for database operations
db_lock = Lock()

def extract_is_from_tag(tag: Dict) -> tuple:
    """Extract DG and IS value from ENV tag"""
    logger.debug(f"Extracting IS from tag: {tag}")
    if tag.get("context") == "CONTEXTLESS" and tag.get("key", "").startswith("ENV:"):
        env_value = tag["key"].replace("ENV:", "").strip()
        if match := re.match(r"([^-]+)--([^-]+)--", env_value):
            logger.debug(f"Extracted DG: {match.group(1)}, IS: {match.group(2)}")
            return match.group(1), match.group(2)
    return None, None

def process_host_tags_for_dgs(tags_response):
    """Process tags to extract DG and IS values"""
    logger.info("Processing tags")
    dg_values = set()
    dg_is_mapping = {}

    with ThreadPoolExecutor(max_workers=TOPOLOGY_REFRESH_THREADS) as executor:
        def process_tag(tag):
            if tag.get("context") == "CONTEXTLESS":
                key = tag.get("key", "")
                
                if key.startswith(("DG:", "DG\\")):
                    dg = key.replace("DG:", "").replace("DG\\", "").strip()
                    if dg:
                        return (dg, None, None)
                
                elif key.startswith(("ENV:", "ENV\\")):
                    value = key.replace("ENV:", "").replace("ENV\\", "").strip()
                    if len(parts := value.split("--")) >= 2:
                        dg_name, is_name = parts[0].strip(), parts[1].strip()
                        if dg_name and is_name:
                            return (dg_name, is_name, True)
            return None

        for result in executor.map(process_tag, tags_response.get("tags", [])):
            if result:
                dg_name, is_name, has_is = result
                dg_values.add(dg_name)
                if has_is:
                    dg_is_mapping.setdefault(dg_name, set()).add(is_name)
    
    return dg_values, dg_is_mapping

def refresh_dgs_task():
    """Refresh DGs and IS mappings"""
    global topology_refresh_status
    topology_refresh_status["dgs"]["status"] = RefreshStatus.IN_PROGRESS
    
    try:
        tags_response = get_host_tags()
        dg_values, dg_is_mapping = process_host_tags_for_dgs(tags_response)

        db = next(get_db())
        with db_lock:
            for dg_value in dg_values:
                update_dg(db, dg_value, dg_is_mapping)

        topology_refresh_status["dgs"].update({
            "status": RefreshStatus.COMPLETED,
            "last_update": datetime.utcnow()
        })
        logger.info("DGs refresh completed successfully")
        
    except Exception as e:
        topology_refresh_status["dgs"].update({
            "status": RefreshStatus.FAILED,
            "last_update": datetime.utcnow()
        })
        logger.error(f"DGs refresh failed: {e}")

def refresh_hosts_task():
    """Refresh hosts data"""
    global topology_refresh_status
    topology_refresh_status["hosts"]["status"] = RefreshStatus.IN_PROGRESS
    
    try:
        hosts_data = get_hosts()
        db = next(get_db())
        
        logger.info("Hosts entities retrieved successfully, processing relationships in DB (this can take a while)")
        
        with db_lock:
            for host in hosts_data["entities"]:
                update_host(db, host)

        topology_refresh_status["hosts"].update({
            "status": RefreshStatus.COMPLETED,
            "last_update": datetime.utcnow()
        })
        logger.info("Hosts refresh completed successfully")
        
    except Exception as e:
        topology_refresh_status["hosts"].update({
            "status": RefreshStatus.FAILED, 
            "last_update": datetime.utcnow()
        })
        logger.error(f"Hosts refresh failed: {e}")

def refresh_applications_task():
    """Refresh applications data"""
    global topology_refresh_status
    topology_refresh_status["applications"]["status"] = RefreshStatus.IN_PROGRESS
    
    try:
        applications_data = get_applications()
        db = next(get_db())
        logger.info("Application entities retrieved successfully, processing relationships in DB (this can take a while)")
        with db_lock:
            for app in applications_data["entities"]:
                update_application(db, app)

        topology_refresh_status["applications"].update({
            "status": RefreshStatus.COMPLETED,
            "last_update": datetime.utcnow()
        })
        logger.info("Applications refresh completed successfully")
        
    except Exception as e:
        topology_refresh_status["applications"].update({
            "status": RefreshStatus.FAILED,
            "last_update": datetime.utcnow()
        })
        logger.error(f"Applications refresh failed: {e}")

def refresh_synthetics_task():
    """Refresh synthetics data"""
    global topology_refresh_status
    topology_refresh_status["synthetics"]["status"] = RefreshStatus.IN_PROGRESS
    logger.info("Synthetic entities retrieved successfully, processing relationships in DB (this can take a while)")
    try:
        synthetics_data = get_synthetics()
        db = next(get_db())
        
        with db_lock:
            for synthetic in synthetics_data["entities"]:
                update_synthetic(db, synthetic)

        topology_refresh_status["synthetics"].update({
            "status": RefreshStatus.COMPLETED,
            "last_update": datetime.utcnow()
        })
        logger.info("Synthetics refresh completed successfully")
        
    except Exception as e:
        topology_refresh_status["synthetics"].update({
            "status": RefreshStatus.FAILED,
            "last_update": datetime.utcnow()
        })
        logger.error(f"Synthetics refresh failed: {e}")

def update_dg(db: Session, dg_value: str, dg_is_mapping: Dict):
    """Update single DG and its IS mappings"""
    try:
        existing_dg = db.query(DG).filter(DG.name == dg_value).first()
        if existing_dg:
            existing_dg.last_updated = datetime.utcnow()
        else:
            existing_dg = DG(name=dg_value, last_updated=datetime.utcnow())
            db.add(existing_dg)
            db.flush()  # Ensure existing_dg.id is available

        if dg_value in dg_is_mapping:
            is_entries = []
            for is_name in dg_is_mapping[dg_value]:
                if not db.query(IS).filter(IS.name == is_name, IS.dg_id == existing_dg.id).first():
                    managed = is_is_managed(is_name)
                    is_entries.append(IS(name=is_name, dg_id=existing_dg.id, last_updated=datetime.utcnow(), managed=managed))
            if is_entries:
                db.bulk_save_objects(is_entries)

        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"Error updating DG: {e}")
        return False
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating DG {dg_value}: {str(e)}")
        return False

def update_host(db: Session, host_data: dict):
    """Update single host"""
    try:
        memory_bytes = host_data.get("properties", {}).get("physicalMemory", 0)
        memory_gb = memory_bytes / (1024 * 1024 * 1024) if memory_bytes else None
        monitoring_mode = host_data.get("properties", {}).get("monitoringMode", "")
        
        managed = host_is_managed({"tags": str(host_data.get("tags", []))})

        host_dict = {
            "dt_id": host_data.get("entityId"),
            "name": host_data.get("displayName"),
            "managed": managed,
            "memory_gb": memory_gb,
            "monitoring_mode": monitoring_mode,
            "state": host_data.get("properties", {}).get("state", ""),
            "tags": str(host_data.get("tags", [])),
            "last_updated": datetime.utcnow()
        }

        existing_host = db.query(Host).filter(Host.dt_id == host_dict["dt_id"]).first()
        if existing_host:
            for key, value in host_dict.items():
                setattr(existing_host, key, value)
            host = existing_host
        else:
            host = Host(**host_dict)
            db.add(host)

        host_dgs = set()
        host_is = []
        
        for tag in host_data.get("tags", []):
            if tag.get("context") == "CONTEXTLESS":
                key = tag.get("key", "")
                
                if key.startswith(("DG:", "DG\\")):
                    dg_name = key.replace("DG:", "").replace("DG\\", "").strip()
                    if dg := db.query(DG).filter(DG.name == dg_name).first():
                        host_dgs.add(dg)
                
                elif key.startswith(("ENV:", "ENV\\")):
                    value = key.replace("ENV:", "").replace("ENV\\", "").strip()
                    if len(parts := value.split("--")) >= 2:
                        dg_name, is_name = parts[0].strip(), parts[1].strip()
                        if dg := db.query(DG).filter(DG.name == dg_name).first():
                            host_dgs.add(dg)
                            if is_ := db.query(IS).filter(IS.name == is_name, IS.dg_id == dg.id).first():
                                host_is.append(is_)
        # If host is managed, and IS is not managed, mark IS as managed
        for is_ in host_is:
            if is_.managed == False:
                is_.managed = True
        host.dgs = list(host_dgs)
        host.information_systems = host_is
        db.commit()
        return True
        
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to update host {host_data.get('displayName')}: {e}")
        return False

def update_application(db: Session, app_data: dict):
    """Update single application"""
    try:
        tags = [{
            "context": tag.get("context"),
            "key": tag.get("key"),
            "value": tag.get("value")
        } for tag in app_data.get("tags", [])]

        existing_app = db.query(Application).filter(Application.dt_id == app_data["entityId"]).first()
        
        if existing_app:
            existing_app.name = app_data.get("displayName", "")
            existing_app.type = app_data.get("type", "")
            existing_app.tags = json.dumps(tags)
            existing_app.last_updated = datetime.utcnow()
            current_app = existing_app
        else:
            current_app = Application(
                dt_id=app_data["entityId"],
                name=app_data.get("displayName", ""),
                type=app_data.get("type", ""),
                tags=json.dumps(tags),
                last_updated=datetime.utcnow()
            )
            db.add(current_app)

        dgs_to_link = set()
        is_to_link = []

        for tag in tags:
            if tag["context"] == "CONTEXTLESS":
                key = tag.get("key", "")
                
                if key.startswith(("DG:", "DG\\")):
                    dg_name = key.replace("DG:", "").replace("DG\\", "").strip()
                    if dg := db.query(DG).filter(DG.name == dg_name).first():
                        dgs_to_link.add(dg)
                
                elif key.startswith(("ENV:", "ENV\\")):
                    value = key.replace("ENV:", "").replace("ENV\\", "").strip()
                    if len(parts := value.split("--")) >= 2:
                        dg_name, is_name = parts[0].strip(), parts[1].strip()
                        if dg := db.query(DG).filter(DG.name == dg_name).first():
                            dgs_to_link.add(dg)
                            if is_ := db.query(IS).filter(IS.name == is_name, IS.dg_id == dg.id).first():
                                is_to_link.append(is_)

        current_app.dgs = list(dgs_to_link)
        current_app.information_systems = is_to_link
        db.commit()
        return True
        
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to update application {app_data.get('displayName')}: {e}")
        return False

def update_synthetic(db: Session, synthetic_data: dict):
    """Update single synthetic monitor"""
    try:
        tags = [{
            "context": tag.get("context"),
            "key": tag.get("key"),
            "value": tag.get("value")
        } for tag in synthetic_data.get("tags", [])]

        existing_synthetic = db.query(Synthetic).filter(
            Synthetic.dt_id == synthetic_data["entityId"]
        ).first()

        if existing_synthetic:
            existing_synthetic.name = synthetic_data.get("displayName", "")
            existing_synthetic.type = synthetic_data.get("type", "")
            existing_synthetic.tags = json.dumps(tags)
            existing_synthetic.last_updated = datetime.utcnow()
            current_synthetic = existing_synthetic
        else:
            current_synthetic = Synthetic(
                dt_id=synthetic_data["entityId"],
                name=synthetic_data.get("displayName", ""),
                type=synthetic_data.get("type", ""),
                tags=json.dumps(tags),
                last_updated=datetime.utcnow()
            )
            db.add(current_synthetic)

        dgs_to_link = set()
        is_to_link = []
        
        for tag in tags:
            if tag["context"] == "CONTEXTLESS":
                key = tag.get("key", "")
                
                if key.startswith(("DG:", "DG\\")):
                    dg_name = key.replace("DG:", "").replace("DG\\", "").strip()
                    if dg := db.query(DG).filter(DG.name == dg_name).first():
                        dgs_to_link.add(dg)
                
                elif key.startswith(("ENV:", "ENV\\")):
                    value = key.replace("ENV:", "").replace("ENV\\", "").strip()
                    if len(parts := value.split("--")) >= 2:
                        dg_name, is_name = parts[0].strip(), parts[1].strip()
                        if dg := db.query(DG).filter(DG.name == dg_name).first():
                            dgs_to_link.add(dg)
                            if is_ := db.query(IS).filter(IS.name == is_name, IS.dg_id == dg.id).first():
                                is_to_link.append(is_)

                if key.startswith("HTTP_TYPE"):
                    current_synthetic.http_type_tag = key

                if key.startswith("HTTP_CUSTOM"):
                    current_synthetic.is_custom_monitor = True
                
        current_synthetic.dgs = list(dgs_to_link)
        current_synthetic.information_systems = is_to_link
        db.commit()
        return True
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing synthetic {synthetic_data.get('entityId')}: {e}")
        return False