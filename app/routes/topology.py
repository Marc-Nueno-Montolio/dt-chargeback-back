# Import required modules and dependencies
from ..database import SessionLocal, get_db
from ..dynatrace import get_applications, get_host_tags, get_hosts, get_synthetics
from ..models import Application, DG, Host, IS, Synthetic
from datetime import datetime
from fastapi import APIRouter, Body, Depends, HTTPException, Query, BackgroundTasks
from math import ceil
from sqlalchemy import and_, func, not_, or_
from sqlalchemy import func
from sqlalchemy.orm import Session
from typing import Dict, List, Optional
import json
import re
import threading
import time

# Import topology service functions
from ..services import topology as topology_svc

# Create FastAPI router
router = APIRouter()

# Refresh endpoints for different entity types
@router.post("/refresh/dgs")
def refresh_dgs(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Endpoint to refresh DGs from DT tags API"""
    background_tasks.add_task(topology_svc.refresh_dgs_task)
    return topology_svc.topology_refresh_status["dgs"]

@router.post("/refresh/hosts")
def refresh_hosts(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Endpoint to refresh Hosts from DT API"""
    background_tasks.add_task(topology_svc.refresh_hosts_task)
    return topology_svc.topology_refresh_status["hosts"]

@router.post("/refresh/applications")
def refresh_applications(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Endpoint to refresh Applications from DT API"""
    background_tasks.add_task(topology_svc.refresh_applications_task)
    return topology_svc.topology_refresh_status["applications"]

@router.post("/refresh/synthetics")
def refresh_synthetics(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Endpoint to refresh Synthetics from DT  API"""
    background_tasks.add_task(topology_svc.refresh_synthetics_task)
    return topology_svc.topology_refresh_status["synthetics"]

@router.get("/refresh/status")
def get_refresh_status_task():   
    """Get current status of all refresh tasks"""
    return topology_svc.topology_refresh_status
