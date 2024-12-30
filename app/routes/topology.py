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

@router.get("/dgs")
def get_all_dgs(
    page: int = 1,
    limit: int = 100,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get paginated list of Delivery Groups (DGs)
    
    Parameters:
        page: Page number for pagination
        limit: Number of items per page
        search: Optional search string to filter DGs by name
        db: Database session dependency
    """
    # Start with base query
    query = db.query(DG)
    
    # Apply search filter if provided
    if search:
        query = query.filter(DG.name.ilike(f"%{search}%"))
    
    # Calculate pagination values
    total_items = query.count()
    total_pages = (total_items + limit - 1) // limit
    
    # Validate and adjust page number if needed
    if page < 1:
        page = 1
    elif page > total_pages and total_pages > 0:
        page = total_pages
        
    # Calculate pagination offset
    skip = (page - 1) * limit
    
    # Get paginated results
    dgs = query.offset(skip).limit(limit).all()
    
    # Return formatted response with pagination metadata
    return {
        "items": dgs,
        "total_items": total_items,
        "total_pages": total_pages,
        "current_page": page,
        "items_per_page": limit,
        "has_previous": page > 1,
        "has_next": page < total_pages
    }

@router.get("/dgs/{dg_id}/details")
def get_dg_details(
    dg_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific Delivery Group
    Including associated Information Systems and Hosts
    
    Parameters:
        dg_id: ID of the Delivery Group
        db: Database session dependency
    """
    # Get DG or raise 404 if not found
    dg = db.query(DG).filter(DG.id == dg_id).first()
    if not dg:
        raise HTTPException(status_code=404, detail="DG not found")
    
    # Get all Information Systems for this DG
    information_systems = db.query(IS).filter(IS.dg_id == dg_id).all()
    
    # Get all hosts associated with this DG
    dg_hosts = dg.hosts
    
    # Find hosts that don't belong to any IS in this DG
    hosts_without_is = []
    for host in dg_hosts:
        host_is_ids = {is_.id for is_ in host.information_systems}
        dg_is_ids = {is_.id for is_ in information_systems}
        if not host_is_ids.intersection(dg_is_ids):
            hosts_without_is.append({
                "id": host.id,
                "name": host.name,
                "dt_id": host.dt_id
            })
    
    # Format Information Systems response
    is_response = [{
        "id": is_.id,
        "name": is_.name,
        "hosts": [{
            "id": host.id,
            "name": host.name,
            "dt_id": host.dt_id
        } for host in is_.hosts]
    } for is_ in information_systems]
    
    # Return complete DG details
    return {
        "id": dg.id,
        "name": dg.name,
        "last_updated": dg.last_updated,
        "information_systems": is_response,
        "hosts_without_is": hosts_without_is
    }

@router.get("/is")
def get_all_is(db: Session = Depends(get_db)):
    """Get all Information Systems"""
    information_systems = db.query(IS).all()
    return {"information_systems": information_systems}

@router.get("/hosts")
def get_all_hosts(
    page: int = 1,
    limit: int = 100,
    search: Optional[str] = None,
    managed: Optional[bool] = None,
    min_memory: Optional[float] = None,
    max_memory: Optional[float] = None,
    state: Optional[str] = None,
    monitoring_mode: Optional[str] = None,
    dg: Optional[str] = None,
    information_system: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get paginated and filtered list of hosts
    
    Parameters:
        page: Page number for pagination
        limit: Number of items per page
        search: Search string for host name or DT ID
        managed: Filter by managed status
        min_memory: Minimum memory in GB
        max_memory: Maximum memory in GB
        state: Filter by host state(s)
        monitoring_mode: Filter by monitoring mode(s)
        dg: Filter by Delivery Group(s)
        information_system: Filter by Information System(s)
        db: Database session dependency
    """
    is_ = information_system
    query = db.query(Host)

    # Apply search filter
    if search:
        query = query.filter(
            or_(
                Host.name.ilike(f"%{search}%"),
                Host.dt_id.ilike(f"%{search}%")
            )
        )

    # Apply managed status filter
    if managed is not None:
        query = query.filter(Host.managed == managed)

    # Apply memory range filters
    if min_memory is not None:
        query = query.filter(Host.memory_gb >= float(min_memory))
    if max_memory is not None:
        query = query.filter(Host.memory_gb <= float(max_memory))

    # Apply state filter with null handling
    if state:
        states = state.split(',')
        state_conditions = []
        for s in states:
            if s.lower() == 'null':
                state_conditions.extend([Host.state.is_(None), Host.state == ''])
            else:
                state_conditions.append(Host.state == s)
        query = query.filter(or_(*state_conditions))

    # Apply monitoring mode filter with null handling
    if monitoring_mode:
        modes = monitoring_mode.split(',')
        mode_conditions = []
        for m in modes:
            if m.lower() == 'null':
                mode_conditions.extend([Host.monitoring_mode.is_(None), Host.monitoring_mode == ''])
            else:
                mode_conditions.append(Host.monitoring_mode == m)
        query = query.filter(or_(*mode_conditions))

    # Apply DG filter with null handling
    if dg:
        dgs = dg.split(',')
        if 'null' in dgs:
            # Handle hosts with no DGs
            no_dg_subquery = db.query(Host).outerjoin(Host.dgs).group_by(Host.id).having(func.count(DG.id) == 0).subquery()
            if len(dgs) > 1:  # If there are other DGs selected too
                other_dgs = [d for d in dgs if d.lower() != 'null']
                dg_subquery = query.join(Host.dgs).filter(or_(*[DG.name == d for d in other_dgs])).subquery()
                query = db.query(Host).filter(or_(
                    Host.id.in_(db.query(no_dg_subquery.c.id)),
                    Host.id.in_(db.query(dg_subquery.c.id))
                ))
            else:
                query = db.query(Host).filter(Host.id.in_(db.query(no_dg_subquery.c.id)))
        else:
            dg_subquery = query.join(Host.dgs).filter(or_(*[DG.name == d for d in dgs])).subquery()
            query = db.query(Host).join(dg_subquery, Host.id == dg_subquery.c.id)

    # Apply IS filter with null handling
    if is_:
        information_systems = is_.split(',')
        if 'null' in information_systems:
            # Create a subquery for hosts with NO Information Systems
            no_is_hosts = (
                db.query(Host.id)
                .outerjoin(Host.information_systems)
                .group_by(Host.id)
                .having(func.count(IS.id) == 0)
                .subquery()
            )
            
            if len(information_systems) > 1:
                # If there are other IS selected besides 'null'
                other_is = [i for i in information_systems if i.lower() != 'null']
                # Create a subquery for hosts with specific IS
                has_is_hosts = (
                    db.query(Host.id)
                    .join(Host.information_systems)
                    .filter(IS.name.in_(other_is))
                    .group_by(Host.id)
                    .subquery()
                )
                # Combine both conditions
                query = query.filter(or_(
                    Host.id.in_(db.query(no_is_hosts)),
                    Host.id.in_(db.query(has_is_hosts))
                ))
            else:
                # If only 'null' is selected, only show hosts with NO Information Systems
                query = query.filter(Host.id.in_(db.query(no_is_hosts)))
        else:
            # For non-null values, show only hosts that have the specified IS
            query = (
                query
                .join(Host.information_systems)
                .filter(IS.name.in_(information_systems))
                .group_by(Host.id)
            )

    # Calculate pagination values
    total_items = query.count()
    total_pages = ceil(total_items / limit)

    # Apply pagination
    skip = (page - 1) * limit
    query = query.offset(skip).limit(limit)

    # Execute query and format results
    hosts = query.all()
    formatted_hosts = []

    # Format each host with its relationships and tags
    for host in hosts:
        host_dict = {
            "id": host.id,
            "dt_id": host.dt_id,
            "name": host.name,
            "managed": host.managed,
            "memory_gb": host.memory_gb,
            "state": host.state,
            "monitoring_mode": host.monitoring_mode,
            "dgs": [{"id": dg.id, "name": dg.name} for dg in host.dgs],
            "information_systems": [{"id": is_.id, "name": is_.name, "dg": {"id": is_.dg.id, "name": is_.dg.name}} for is_ in host.information_systems],
            "last_updated": host.last_updated,
            "tags": []
        }

        # Parse and format tags safely
        if host.tags:
            try:
                if isinstance(host.tags, str):
                    tags = json.loads(host.tags.replace("'", '"'))
                else:
                    tags = host.tags

                if isinstance(tags, list):
                    host_dict["tags"] = [
                        {
                            "context": tag.get("context", ""),
                            "key": tag.get("key", ""),
                            "value": tag.get("value", ""),
                            "stringRepresentation": tag.get("stringRepresentation", "") or f"{tag.get('key', '')}"
                        }
                        for tag in tags
                    ]
            except Exception as e:
                print(f"Error parsing tags for host {host.id}: {e}")
                host_dict["tags"] = []

        formatted_hosts.append(host_dict)

    # Return formatted response with pagination metadata
    return {
        "items": formatted_hosts,
        "total_items": total_items,
        "total_pages": total_pages,
        "current_page": page,
        "items_per_page": limit,
        "has_previous": page > 1,
        "has_next": page < total_pages
    }

@router.get("/hosts/filters")
def get_host_filters(db: Session = Depends(get_db)):
    """
    Get all possible filter values for hosts
    Returns unique values for states, monitoring modes, DGs and Information Systems
    """
    try:
        # Get all unique values for each filter using distinct
        states = db.query(Host.state)\
            .filter(Host.state.isnot(None))\
            .filter(Host.state != '')\
            .distinct()\
            .all()
            
        modes = db.query(Host.monitoring_mode)\
            .filter(Host.monitoring_mode.isnot(None))\
            .filter(Host.monitoring_mode != '')\
            .distinct()\
            .all()
        
        # Get all DGs from the DG table
        dgs = db.query(DG.name)\
            .filter(DG.name.isnot(None))\
            .filter(DG.name != '')\
            .distinct()\
            .all()
            
        # Get all IS from the IS table
        information_systems = db.query(IS.name)\
            .filter(IS.name.isnot(None))\
            .filter(IS.name != '')\
            .distinct()\
            .all()

        return {
            "states": sorted([state[0] for state in states if state[0]]),
            "monitoring_modes": sorted([mode[0] for mode in modes if mode[0]]),
            "dgs": sorted([dg[0] for dg in dgs if dg[0]]),
            "information_systems": sorted([is_[0] for is_ in information_systems if is_[0]])
        }
    except Exception as e:
        print(f"Error in get_host_filters: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching filters: {str(e)}")


@router.get("/applications")
def get_all_applications(
    page: int = 1,
    limit: int = 100,
    search: Optional[str] = None,
    state: Optional[str] = None,
    monitoring_mode: Optional[str] = None,
    dg: Optional[str] = None,
    information_system: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get paginated and filtered list of applications
    
    Parameters:
        page: Page number for pagination
        limit: Number of items per page
        search: Search string for application name or DT ID
        state: Filter by application state(s)
        monitoring_mode: Filter by monitoring mode(s)
        dg: Filter by Delivery Group(s)
        information_system: Filter by Information System(s)
        db: Database session dependency
    """
    is_ = information_system
    query = db.query(Application)

    # Apply search filter
    if search:
        query = query.filter(
            or_(
                Application.name.ilike(f"%{search}%"),
                Application.dt_id.ilike(f"%{search}%")
            )
        )

    # Apply state filter with null handling
    if state:
        states = state.split(',')
        state_conditions = []
        for s in states:
            if s.lower() == 'null':
                state_conditions.extend([Application.state.is_(None), Application.state == ''])
            else:
                state_conditions.append(Application.state == s)
        query = query.filter(or_(*state_conditions))

    # Apply monitoring mode filter with null handling
    if monitoring_mode:
        modes = monitoring_mode.split(',')
        mode_conditions = []
        for m in modes:
            if m.lower() == 'null':
                mode_conditions.extend([Application.monitoring_mode.is_(None), Application.monitoring_mode == ''])
            else:
                mode_conditions.append(Application.monitoring_mode == m)
        query = query.filter(or_(*mode_conditions))

    # Apply DG filter with null handling
    if dg:
        dgs = dg.split(',')
        dg_conditions = []
        
        for dg_name in dgs:
            if dg_name.lower() == 'null':
                no_dg_subquery = (
                    db.query(Application.id)
                    .outerjoin(Application.dgs)
                    .group_by(Application.id)
                    .having(func.count(DG.id) == 0)
                )
                dg_conditions.append(Application.id.in_(no_dg_subquery))
            else:
                has_dg_subquery = (
                    db.query(Application.id)
                    .join(Application.dgs)
                    .filter(DG.name == dg_name)
                    .group_by(Application.id)
                )
                dg_conditions.append(Application.id.in_(has_dg_subquery))
        
        query = query.filter(or_(*dg_conditions))

    # Apply IS filter with null handling
    if is_:
        information_systems = is_.split(',')
        is_conditions = []
        
        for is_name in information_systems:
            if is_name.lower() == 'null':
                no_is_subquery = (
                    db.query(Application.id)
                    .outerjoin(Application.information_systems)
                    .group_by(Application.id)
                    .having(func.count(IS.id) == 0)
                )
                is_conditions.append(Application.id.in_(no_is_subquery))
            else:
                has_is_subquery = (
                    db.query(Application.id)
                    .join(Application.information_systems)
                    .filter(IS.name == is_name)
                    .group_by(Application.id)
                )
                is_conditions.append(Application.id.in_(has_is_subquery))
        
        query = query.filter(or_(*is_conditions))

    # Calculate pagination values
    total_items = query.count()
    total_pages = (total_items + limit - 1) // limit

    # Apply pagination
    query = query.offset((page - 1) * limit).limit(limit)

    # Execute query and format results
    applications = query.all()

    # Parse tags for each application
    for app in applications:
        app.tags = json.loads(app.tags)

    # Format response with relationships
    formatted_applications = []
    for app in applications:
        formatted_app = {
            "id": app.id,
            "dt_id": app.dt_id,
            "name": app.name,
            "tags": app.tags,
            "type": app.type,
            "last_updated": app.last_updated,
            "dgs": [{"id": dg.id, "name": dg.name} for dg in app.dgs],
            "information_systems": [{
                "id": is_.id,
                "name": is_.name,
                "dg": {
                    "id": is_.dg.id,
                    "name": is_.dg.name
                } if is_.dg else None
            } for is_ in app.information_systems]
        }
        formatted_applications.append(formatted_app)

    # Return formatted response with pagination metadata
    return {
        "items": formatted_applications,
        "total_items": total_items,
        "total_pages": total_pages,
        "current_page": page,
        "items_per_page": limit,
        "has_previous": page > 1,
        "has_next": page < total_pages
    }

@router.get("/applications/filters")
def get_application_filters(db: Session = Depends(get_db)):
    """
    Get all possible filter values for applications
    Returns unique values for DGs, Information Systems, and application types
    """
    try:
        # Get DGs that have associated applications
        dgs = db.query(DG.name)\
            .join(DG.applications)\
            .distinct()\
            .all()
            
        # Get Information Systems that have associated applications
        information_systems = db.query(IS.name)\
            .join(IS.applications)\
            .distinct()\
            .all()
        
        # Get all unique application types
        app_types = db.query(Application.type)\
            .distinct()\
            .all()

        return {
            "dgs": sorted([dg[0] if dg[0] else "N/A" for dg in dgs]),
            "information_systems": sorted([is_[0] if is_[0] else "N/A" for is_ in information_systems]),
            "app_types": sorted([type[0] if type[0] else "N/A" for type in app_types])
        }
    except Exception as e:
        print(f"Error in get_application_filters: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching filters: {str(e)}")

@router.get("/synthetics")
def get_all_synthetics(
    page: int = 1,
    limit: int = 10,
    dg: str = None,
    information_system: str = None,
    synthetic_type: str = None,
    http_type: str = None,
    search: str = None,
    db: Session = Depends(get_db)
):
    """
    Get paginated and filtered list of synthetic monitors
    
    Parameters:
        page: Page number for pagination
        limit: Number of items per page
        dg: Filter by Delivery Group(s)
        information_system: Filter by Information System(s)
        synthetic_type: Filter by synthetic monitor type(s)
        http_type: Filter by HTTP type(s)
        search: Search string for synthetic monitor name
        db: Database session dependency
    """
    try:
        query = db.query(Synthetic)

        # Apply DG filter with multiple values support
        if dg:
            dg_values = [d.strip() for d in dg.split(',')]
            dg_filters = []
            for dg_value in dg_values:
                dg_filters.append(DG.name == dg_value)
            query = query.join(Synthetic.dgs).filter(or_(*dg_filters))
        
        # Apply IS filter with multiple values support
        if information_system:
            is_values = [is_.strip() for is_ in information_system.split(',')]
            is_filters = []
            for is_value in is_values:
                is_filters.append(IS.name == is_value)
            query = query.join(Synthetic.information_systems).filter(or_(*is_filters))
            
        # Apply synthetic type filter with null handling
        if synthetic_type:
            type_values = [t.strip() for t in synthetic_type.split(',')]
            type_filters = []
            for type_value in type_values:
                if type_value.lower() == "n/a":
                    type_filters.append(or_(Synthetic.type == None, Synthetic.type == ""))
                else:
                    type_filters.append(Synthetic.type == type_value)
            query = query.filter(or_(*type_filters))

        # Apply HTTP type filter with null handling
        if http_type:
            http_type_values = [h.strip() for h in http_type.split(',')]
            http_type_filters = []
            for http_type_value in http_type_values:
                if http_type_value.lower() == "n/a":
                    http_type_filters.append(or_(Synthetic.http_type_tag == None, Synthetic.http_type_tag == ""))
                else:
                    http_type_filters.append(Synthetic.http_type_tag == http_type_value)
            query = query.filter(or_(*http_type_filters))
            
        # Apply search filter
        if search:
            query = query.filter(Synthetic.name.ilike(f"%{search}%"))

        # Calculate pagination values
        total_items = query.count()
        total_pages = (total_items + limit - 1) // limit
        
        # Apply pagination
        synthetics = query.offset((page - 1) * limit).limit(limit).all()
        
        # Format response with relationships
        formatted_synthetics = []
        for synthetic in synthetics:
            formatted_synthetic = {
                "id": synthetic.id,
                "dt_id": synthetic.dt_id,
                "name": synthetic.name,
                "type": synthetic.type,
                "tags": json.loads(synthetic.tags) if synthetic.tags else [],
                "last_updated": synthetic.last_updated.isoformat(),
                "http_type_tag": synthetic.http_type_tag,
                "dgs": [{"id": dg.id, "name": dg.name} for dg in synthetic.dgs],
                "information_systems": [{
                    "id": is_.id,
                    "name": is_.name,
                    "dg": {
                        "id": is_.dg.id,
                        "name": is_.dg.name
                    } if is_.dg else None
                } for is_ in synthetic.information_systems]
            }
            formatted_synthetics.append(formatted_synthetic)

        # Return formatted response with pagination metadata
        return {
            "items": formatted_synthetics,
            "total_items": total_items,
            "total_pages": total_pages,
            "current_page": page,
            "items_per_page": limit,
            "has_previous": page > 1,
            "has_next": page < total_pages
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching synthetics: {str(e)}")

@router.get("/synthetics/filters")
def get_synthetic_filters(db: Session = Depends(get_db)):
    """
    Get all possible filter values for synthetic monitors
    Returns unique values for DGs, Information Systems, synthetic types and HTTP types
    """
    try:
        # Get DGs that have associated synthetic monitors
        dgs = db.query(DG.name)\
            .join(DG.synthetics)\
            .distinct()\
            .all()
            
        # Get Information Systems that have associated synthetic monitors
        information_systems = db.query(IS.name)\
            .join(IS.synthetics)\
            .distinct()\
            .all()
        
        # Get all unique synthetic monitor types
        synthetic_types = db.query(Synthetic.type)\
            .distinct()\
            .all()

        # Get all unique HTTP types
        http_types = db.query(Synthetic.http_type_tag)\
            .distinct()\
            .all()

        return {
            "dgs": sorted([dg[0] if dg[0] else "N/A" for dg in dgs]),
            "information_systems": sorted([is_[0] if is_[0] else "N/A" for is_ in information_systems]),
            "synthetic_types": sorted([type[0] if type[0] else "N/A" for type in synthetic_types]),
            "http_types": sorted([http_type[0] if http_type[0] else "N/A" for http_type in http_types])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching filters: {str(e)}")
