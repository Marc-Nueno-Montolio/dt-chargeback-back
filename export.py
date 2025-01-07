from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
import csv
import json
from settings import LOG_FORMAT, LOG_LEVEL
from typing import Dict
import logging
import pandas as pd

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

from datetime import datetime

def export_data(data, output, format):
    if format == 'json':
        # Convert datetime objects to strings
        for item in data:
            for key, value in item.items():
                if isinstance(value, datetime):
                    item[key] = value.isoformat()
        
        with open(output, 'w') as f:
            json.dump(data, f, indent=2)
    elif format == 'csv':
        with open(output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(data[0].keys())
            for row in data:
                writer.writerow(row.values())


class ChargebackExcelExporter:
    """
    Handles the export of chargeback report data to Excel format with multiple sheets
    and formatted tables.
    """

    def __init__(self):
        # Define usage columns in order
        self.host_columns = ['Fullstack', 'Infrastructure'] 
        self.app_columns = ['RUM', 'RUM with Session Replay']
        self.synthetic_columns = ['Browser Monitor', 'HTTP Monitor', '3rd Party Monitor']

        # Define styles
        self.header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
        self.subheader_fill = PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid')
        self.host_fill = PatternFill(start_color='E2EFDA', end_color='E2EFDA', fill_type='solid')
        self.app_fill = PatternFill(start_color='DEEBF7', end_color='DEEBF7', fill_type='solid')
        self.synthetic_fill = PatternFill(start_color='F2E7F2', end_color='F2E7F2', fill_type='solid')
        self.header_font = Font(color='FFFFFF', bold=True)
        self.subheader_font = Font(bold=True)
        self.total_font = Font(bold=True)

    def export_to_excel(self, report: Dict, output_path: str):
        """
        Creates an Excel workbook with summary and detailed DG sheets.
        
        Args:
            report: The chargeback report dictionary
            output_path: Path where the Excel file should be saved
        """
        logger.info(f"Starting Excel export to {output_path}")
        
        workbook = Workbook()
        
        # Create summary sheet
        summary_sheet = workbook.active
        summary_sheet.title = 'Summary'
        self._create_summary_sheet(report, summary_sheet)
        
        # Create individual DG sheets
        for dg in report['dgs']:
            sheet_name = dg['name'][:31]  # Excel sheet names limited to 31 chars
            dg_sheet = workbook.create_sheet(sheet_name)
            self._create_dg_sheet(dg, dg_sheet)
            
        # Save workbook
        workbook.save(output_path)
        logger.info("Excel export completed successfully")

    def _create_summary_sheet(self, report: Dict, sheet):
        """Creates the summary sheet with DG totals."""
        logger.debug("Creating summary sheet")
        
        # Add headers
        headers = ['DG'] + self.host_columns + self.app_columns + self.synthetic_columns
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.fill = self.header_fill
            cell.font = self.header_font
            cell.alignment = Alignment(horizontal='left')
            
        # Add DG data
        current_row = 2
        for dg in report['dgs']:
            sheet.cell(row=current_row, column=1, value=dg['name'])
            
            # Add usage data
            usage = dg['data']['totals']['usage']
            col = 2
            for usage_type in self.host_columns + self.app_columns + self.synthetic_columns:
                value = usage.get(usage_type.lower().replace(' ', '_'), 0)
                if usage_type == 'Infrastructure' and value == 0:
                    # If Infrastructure is 0, use Fullstack value
                    value = usage.get('fullstack', 0)
                sheet.cell(row=current_row, column=col, value=value)
                col += 1
                
            current_row += 1
            
        # Add totals row
        sheet.cell(row=current_row, column=1, value='TOTAL').font = self.total_font
        usage = report['totals']['usage']
        col = 2
        for usage_type in self.host_columns + self.app_columns + self.synthetic_columns:
            value = usage.get(usage_type.lower().replace(' ', '_'), 0)
            if usage_type == 'Infrastructure' and value == 0:
                # If Infrastructure is 0, use Fullstack value
                value = usage.get('fullstack', 0)
            cell = sheet.cell(row=current_row, column=col, value=value)
            cell.font = self.total_font
            col += 1
            
        self._apply_formatting(sheet)
