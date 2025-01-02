from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session, sessionmaker
from typing import List, Dict, Optional
from datetime import datetime
from pydantic import BaseModel
import logging
import json
import asyncio

from ..database import get_db, engine
from ..services.chargeback import ChargebackReport
from ..models import DG, Report

logger = logging.getLogger(__name__)

router = APIRouter()

# Request model for chargeback report generation
class ChargebackRequest(BaseModel):
    dg_ids: List[int]  # IDs of delivery groups to include in report
    name: str  # Name of the report
    description: Optional[str] = None  # Optional description
    from_date: datetime  # Start date for report period
    to_date: datetime  # End date for report period

# Global variable to track report generation status
# Values can be: "idle", "processing", "error"
report_status = "idle"

async def generate_report_background(
    dg_ids: List[int],
    name: str,
    description: Optional[str] = None,
    from_date: datetime = datetime.utcnow(),
    to_date: datetime = datetime.utcnow()
):
    """
    Background task to generate chargeback report.
    Handles database operations and report generation process.
    Updates report status and saves results to database.
    """
    global report_status
    # Create dedicated database session for background task
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    report = None
    
    try:
        report_status = "processing"
        
        # Initialize report record in database
        report = Report(
            name=name,
            status="pending", 
            from_date=from_date,
            to_date=to_date,
            last_updated=datetime.utcnow()
        )
        db.add(report)
        db.commit()
        db.refresh(report)

        # Validate and get DG information
        dgs = db.query(DG).filter(DG.id.in_(dg_ids)).all()
        if not dgs:
            raise ValueError("No valid DGs found with provided IDs")
        
        dg_names = [dg.name for dg in dgs]
        logger.info(f"Generating report for DGs: {dg_names}")

        # Initialize report generator and update status
        report_generator = ChargebackReport(db)
        report.status = "processing"
        db.commit()

        try:
            # Generate the actual report
            result = await report_generator.generate_report(dg_names)  # Service uses DG names internally
            await asyncio.sleep(0)  # Yield control back to event loop

            # Save successful results
            report.status = "completed"
            report.data = json.dumps(result)
            report.last_updated = datetime.utcnow()
            
            db.commit()
            report_status = "idle"

        except Exception as e:
            # Handle report generation errors
            logger.error(f"Error in report generation: {str(e)}", exc_info=True)
            report.status = "error"
            report.last_updated = datetime.utcnow()
            report.data = json.dumps({"error": str(e)})
            db.commit()
            report_status = "error"
            raise

    except Exception as e:
        # Handle any other errors
        logger.error(f"Error generating report: {str(e)}", exc_info=True)
        if report:
            report.status = "error"
            report.last_updated = datetime.utcnow()
            db.commit()
        report_status = "error"
    finally:
        db.close()

@router.post("/generate")
async def start_report_generation(
    request: ChargebackRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> Dict:
    """
    API endpoint to initiate report generation.
    Validates request and starts background task.
    """
    global report_status
    try:
        # Check if another report is already being generated
        if report_status == "processing":
            raise HTTPException(
                status_code=400,
                detail="A report is already being generated"
            )

        # Validate that all requested DGs exist
        dgs = db.query(DG).filter(DG.id.in_(request.dg_ids)).all()
        if not dgs:
            raise HTTPException(
                status_code=404,
                detail="No valid DGs found with provided IDs"
            )
        
        # Check for any missing DGs
        if len(dgs) != len(request.dg_ids):
            found_ids = {dg.id for dg in dgs}
            missing_ids = set(request.dg_ids) - found_ids
            raise HTTPException(
                status_code=404,
                detail=f"Some DGs not found: {missing_ids}"
            )

        # Start background task for report generation
        background_tasks.add_task(
            generate_report_background,
            dg_ids=request.dg_ids,
            name=request.name,
            description=request.description,
            from_date=request.from_date,
            to_date=request.to_date,
        )

        return {
            "status": "success", 
            "message": "Report generation started"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting report generation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error starting report generation: {str(e)}"
        )

@router.get("/status")
async def get_report_status():
    """Get current report generation status"""
    return {"status": report_status}

@router.get("/reports")
async def list_reports(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
) -> List[Dict]:
    """
    Get paginated list of all reports.
    Returns basic report info without full report data.
    """
    try:
        reports = db.query(Report)\
            .order_by(Report.last_updated.desc())\
            .offset(skip)\
            .limit(limit)\
            .all()
        
        return [{
            "id": report.id,
            "name": report.name,
            "status": report.status,
            "from_date": report.from_date,
            "to_date": report.to_date,
            "last_updated": report.last_updated,
            "data": None  # Don't include report data in list view
        } for report in reports]

    except Exception as e:
        logger.error(f"Error listing reports: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error listing reports: {str(e)}"
        )

@router.get("/reports/{report_id}")
async def get_report(
    report_id: int,
    db: Session = Depends(get_db)
) -> Dict:
    """
    Get a specific report with complete details including report data.
    """
    try:
        report = db.query(Report).get(report_id)
        if not report:
            raise HTTPException(
                status_code=404,
                detail=f"Report not found with id: {report_id}"
            )

        return {
            "id": report.id,
            "name": report.name,
            "status": report.status,
            "from_date": report.from_date,
            "to_date": report.to_date,
            "last_updated": report.last_updated,
            "data": json.loads(report.data) if report.data else None
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting report: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting report: {str(e)}"
        )

@router.delete("/reports/{report_id}")
async def delete_report(
    report_id: int,
    db: Session = Depends(get_db)
):
    """Delete a specific report from the database."""
    try:
        report = db.query(Report).get(report_id)
        if not report:
            raise HTTPException(
                status_code=404,
                detail=f"Report not found with id: {report_id}"
            )

        db.delete(report)
        db.commit()
        return {"message": "Report deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting report: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting report: {str(e)}"
        )
