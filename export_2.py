from openpyxl import Workbook
import csv
import json
from settings import LOG_FORMAT, LOG_LEVEL
from typing import Dict
import logging
import pandas as pd
from openpyxl.styles import Font, PatternFill
from openpyxl.worksheet.table import Table, TableStyleInfo

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

from datetime import datetime


class ChargebackExcelExporter:
    
    def __init__(self):
        pass

    def export_to_excel(self, report: Dict, output: str):
        """
        Export chargeback report to Excel with flattened entity view
        
        Args:
            report (Dict): Dictionary containing the report data with DGs, IS and entities
            output (str): Output file path for the Excel file
        """
        try:
            # Create empty lists to store data
            data = []
            
            # Define headers
            headers = [
                'DG', 'IS', 'IS Managed', 'Entity Type', 'Entity Name', 'DT ID', 'Managed', 'Cloud', 'Billable', 'Tagged DGs',
                'Fullstack', 'Infra', 'RUM', 'RUM with SR', 'Browser Monitor',
                'HTTP Monitor', '3rd Party Monitor', 'Managed Hosts in IS', 'Non-Managed Hosts in IS'
            ]

            # Sort DGs to put DIGIT C at the end
            sorted_dgs = sorted(report['dgs'], key=lambda x: x['name'] == 'DIGIT C')

            # Iterate through DGs and collect data
            for dg in sorted_dgs:
                dg_name = dg['name']
                
                # Process assigned entities
                for is_system in dg['data']['information_systems']:
                    is_name = is_system['name']
                    is_managed = is_system.get('managed', False)
                    
                    # Calculate managed and non-managed hosts counts for IS
                    managed_hosts_count = sum(1 for h in is_system['data']['entities'].get('hosts', []) if h.get('managed', False))
                    non_managed_hosts_count = sum(1 for h in is_system['data']['entities'].get('hosts', []) if not h.get('managed', False))
                    
                    # Process hosts
                    for host in is_system['data']['entities'].get('hosts', []):
                        # Sort tagged DGs to put DIGIT C last
                        tagged_dgs = sorted(host.get('tagged_dgs', []), key=lambda x: x == 'DIGIT C')
                        row = [
                            dg_name, is_name, is_managed, 'Host', host.get('name', ''), host.get('dt_id', ''),
                            host.get('managed', False), host.get('cloud', False), host.get('billed', False),
                            ', '.join(tagged_dgs), host.get('usage', {}).get('fullstack', 0),
                            host.get('usage', {}).get('infra', 0), 0, 0, 0, 0, 0,
                            managed_hosts_count, non_managed_hosts_count
                        ]
                        data.append(row)

                    # Process applications
                    for app in is_system['data']['entities'].get('applications', []):
                        # Sort tagged DGs to put DIGIT C last
                        tagged_dgs = sorted(app.get('tagged_dgs', []), key=lambda x: x == 'DIGIT C')
                        row = [
                            dg_name, is_name, is_managed, 'Application', app.get('name', ''), app.get('dt_id', ''),
                            False, None, app.get('billed', False), ', '.join(tagged_dgs), 0, 0,
                            app.get('usage', {}).get('rum', 0), app.get('usage', {}).get('rum_with_session_replay', 0),
                            0, 0, 0, managed_hosts_count, non_managed_hosts_count
                        ]
                        data.append(row)

                    # Process synthetics
                    for synthetic in is_system['data']['entities'].get('synthetics', []):
                        # Sort tagged DGs to put DIGIT C last
                        tagged_dgs = sorted(synthetic.get('tagged_dgs', []), key=lambda x: x == 'DIGIT C')
                        row = [
                            dg_name, is_name, is_managed, 'Synthetic', synthetic.get('name', ''), synthetic.get('dt_id', ''),
                            False, None, synthetic.get('billed', False), ', '.join(tagged_dgs), 0, 0,
                            0, 0, synthetic.get('usage', {}).get('browser_monitor', 0),
                            synthetic.get('usage', {}).get('http_monitor', 0),
                            synthetic.get('usage', {}).get('third_party_monitor', 0),
                            managed_hosts_count, non_managed_hosts_count
                        ]
                        data.append(row)

                # Process unassigned entities
                unassigned = dg['data']['unassigned_entities']['entities']
                
                # Calculate managed and non-managed hosts counts for unassigned
                unassigned_managed_hosts_count = sum(1 for h in unassigned.get('hosts', []) if h.get('managed', False))
                unassigned_non_managed_hosts_count = sum(1 for h in unassigned.get('hosts', []) if not h.get('managed', False))

                # Add unassigned entities
                for entity_type, entities in unassigned.items():
                    if entity_type == 'hosts':
                        for host in entities:
                            # Sort tagged DGs to put DIGIT C last
                            tagged_dgs = sorted(host.get('tagged_dgs', []), key=lambda x: x == 'DIGIT C')
                            row = [
                                dg_name, 'Unassigned', False, 'Host', host.get('name', ''), host.get('dt_id', ''),
                                host.get('managed', False), host.get('cloud', False), host.get('billed', False),
                                ', '.join(tagged_dgs), host.get('usage', {}).get('fullstack', 0),
                                host.get('usage', {}).get('infra', 0), 0, 0, 0, 0, 0,
                                unassigned_managed_hosts_count, unassigned_non_managed_hosts_count
                            ]
                            data.append(row)
                    elif entity_type == 'applications':
                        for app in entities:
                            # Sort tagged DGs to put DIGIT C last
                            tagged_dgs = sorted(app.get('tagged_dgs', []), key=lambda x: x == 'DIGIT C')
                            row = [
                                dg_name, 'Unassigned', False, 'Application', app.get('name', ''), app.get('dt_id', ''),
                                False, None, app.get('billed', False), ', '.join(tagged_dgs), 0, 0,
                                app.get('usage', {}).get('rum', 0), app.get('usage', {}).get('rum_with_sr', 0),
                                0, 0, 0, unassigned_managed_hosts_count, unassigned_non_managed_hosts_count
                            ]
                            data.append(row)
                    elif entity_type == 'synthetics':
                        for synthetic in entities:
                            # Sort tagged DGs to put DIGIT C last
                            tagged_dgs = sorted(synthetic.get('tagged_dgs', []), key=lambda x: x == 'DIGIT C')
                            row = [
                                dg_name, 'Unassigned', False, 'Synthetic', synthetic.get('name', ''), synthetic.get('dt_id', ''),
                                False, None, synthetic.get('billed', False), ', '.join(tagged_dgs), 0, 0,
                                0, 0, synthetic.get('usage', {}).get('browser_monitor', 0),
                                synthetic.get('usage', {}).get('http_monitor', 0),
                                synthetic.get('usage', {}).get('third_party_monitor', 0),
                                unassigned_managed_hosts_count, unassigned_non_managed_hosts_count
                            ]
                            data.append(row)

            # Create DataFrame
            df = pd.DataFrame(data, columns=headers)

            # Create Excel writer
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Write main data
                df.to_excel(writer, sheet_name='Chargeback Breakdown', index=False)
                
                # Get the workbook and worksheet
                wb = writer.book
                ws = wb['Chargeback Breakdown']

                # Define custom column widths
                column_widths = {
                    'A': 15,  # DG
                    'B': 30,  # IS
                    'C': 12,  # IS Managed
                    'D': 12,  # Entity Type
                    'E': 40,  # Entity Name
                    'F': 15,  # DT ID
                    'G': 10,  # Managed
                    'H': 10,  # Cloud
                    'I': 10,  # Billable
                    'J': 30,  # Tagged DGs
                    'K': 10,  # Fullstack
                    'L': 10,  # Infra
                    'M': 10,  # RUM
                    'N': 12,  # RUM with SR
                    'O': 15,  # Browser Monitor
                    'P': 15,  # HTTP Monitor
                    'Q': 15,  # 3rd Party Monitor
                    'R': 20,  # Managed Hosts in IS
                    'S': 20   # Non-Managed Hosts in IS
                }

                # Apply custom column widths
                for col_letter, width in column_widths.items():
                    ws.column_dimensions[col_letter].width = width

                # Create table
                tab = Table(displayName="ChargebackTable", ref=ws.dimensions)
                
                # Add a custom style with dark header and white text
                style = TableStyleInfo(
                    name="TableStyleMedium2", 
                    showFirstColumn=False,
                    showLastColumn=False, 
                    showRowStripes=True, 
                    showColumnStripes=False
                )
                tab.tableStyleInfo = style
                
                # Add the table to the worksheet
                ws.add_table(tab)

                # Style the header row with dark background and white text
                header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
                header_font = Font(color="FFFFFF", bold=True)
                
                for cell in ws[1]:
                    cell.fill = header_fill
                    cell.font = header_font

                # Freeze the header row
                ws.freeze_panes = 'A2'

                # Create Summary sheet
                summary_data = []
                for dg in sorted_dgs:
                    totals = dg['data']['totals']
                    usage = totals['usage']
                    summary_data.append([
                        dg['name'],
                        usage.get('fullstack', 0),
                        usage.get('infra', 0),
                        usage.get('rum', 0),
                        usage.get('rum_with_sr', 0),
                        usage.get('browser_monitor', 0),
                        usage.get('http_monitor', 0),
                        usage.get('3rd_party_monitor', 0),
                        totals.get('managed_hosts', 0)
                    ])

                summary_headers = [
                    'DG', 'Fullstack', 'Infra', 'RUM', 'RUM with SR',
                    'Browser Monitor', 'HTTP Monitor', '3rd Party Monitor',
                    'Total Managed Hosts'
                ]

                summary_df = pd.DataFrame(summary_data, columns=summary_headers)
                summary_df.to_excel(writer, sheet_name='DG Summary', index=False)

                # Get the summary worksheet
                summary_ws = wb['DG Summary']

                # Create table for summary with matching style
                summary_tab = Table(displayName="SummaryTable", ref=summary_ws.dimensions)
                summary_tab.tableStyleInfo = style
                summary_ws.add_table(summary_tab)

                # Style the summary header row to match
                for cell in summary_ws[1]:
                    cell.fill = header_fill
                    cell.font = header_font

                # Set column widths for summary sheet
                summary_ws.column_dimensions['A'].width = 20  # DG name
                for col in range(ord('B'), ord('I')+1):
                    summary_ws.column_dimensions[chr(col)].width = 15

            logger.info(f"Excel report successfully exported to {output}")
            
        except Exception as e:
            logger.error(f"Error exporting to Excel: {str(e)}")
            raise