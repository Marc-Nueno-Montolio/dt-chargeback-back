import usage as usage_svc
from models import Application, DG, Host, IS, Synthetic
from collections import defaultdict
from settings import LOG_FORMAT, LOG_LEVEL
from sqlalchemy.orm import Session
from typing import Dict, List
import logging
from chargeback_logic import  *

logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class ChargebackReport:
    """
    Generates detailed chargeback reports for Dynatrace usage across DGs and Information Systems.
    """

    def __init__(self, db: Session, process_unassigned: bool = True, include_non_charged_entities_in_dg: bool = False, entity_types: List[str] = None):
        """
        Initialize the chargeback report generator.
        
        Args:
            db: Database session for querying entities
            process_unassigned: Whether to include unassigned entities in report
            entity_types: List of entity types to process, defaults to all types if None
        """
        self.db = db
        # Define all monitored usage types with their descriptions
        self.usage_types = [
            'fullstack',         # Full-stack monitoring for hosts (includes infrastructure)
            'infra',            # Infrastructure-only monitoring for hosts
            'rum',              # Real User Monitoring for applications
            'rum_with_sr',      # RUM with Session Replay capabilities
            'browser_monitor',   # Browser-based synthetic monitors
            'http_monitor',      # HTTP/API synthetic monitors
            '3rd_party_monitor'  # Third-party synthetic monitors
        ]
        self.processed_entities = set()
        self.process_unassigned = process_unassigned
        self.include_non_charged_entities_in_dg = include_non_charged_entities_in_dg
        # Default to all entity types if none specified

        self.process_entity_types = entity_types or ['hosts', 'applications', 'synthetics']
        self.entity_types = ['hosts', 'applications', 'synthetics']
        
        logger.info(f"Initialized ChargebackReport - Processing unassigned: {self.process_unassigned}, Including non-charged entities in DG: {self.include_non_charged_entities_in_dg}, Processing: {self.process_entity_types}")

    async def generate_report(self, dg_names: List[str]) -> Dict:
        """
        Main entry point to generate a complete chargeback report.
        
        Args:
            dg_names: List of DG names to include in report
            
        Returns:
            Dict containing the complete chargeback report structure
        """
        
        
        # Process first DIGIT_C and DIGIT, if they are in dg_names, move them to first positions, otherwise, add them
        # Ensure DIGIT_C and DIGIT are processed first (We add them even if they were not requested since most managed hosts are hosted there)
        priority_dgs = ['DIGIT C']
        sorted_dg_names = ['DIGIT C'] + [dg for dg in dg_names if dg not in priority_dgs]

        # Update dg_names to the sorted list
        dg_names = sorted_dg_names

        logger.info(f"Starting chargeback report generation for DGs: {dg_names}")
        
        
        try:
            # First collect all usage data from Dynatrace
            logger.debug("Initiating usage data collection from Dynatrace")
            usage_data = await self._collect_usage_data(dg_names)
            logger.info(f"Successfully collected usage data for {len(usage_data)} capabilities")

            # Initialize report structure with zeroed totals
            report = {
                'dgs': [],
                'totals': {
                    'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                    'entities': {entity_type: 0 for entity_type in self.entity_types},
                    'managed_hosts': 0
                },
                'unassigned_totals': {
                    'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                    'entities': {entity_type: 0 for entity_type in self.entity_types},
                    'managed_hosts': 0
                }
            }

            # Process each requested DG
            for dg_name in dg_names:
                logger.info(f"Processing DG: {dg_name}")
                dg = self.db.query(DG).filter(DG.name == dg_name).first()
                if not dg:
                    logger.warning(f"DG {dg_name} not found in database - Skipping")
                    continue
                
                # Create the DG report structure if it doesn't exist yet
                if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                    report['dgs'].append(self._create_dg_report_structure(dg))
                    logger.debug(f"Created new report structure for DG: {dg.name}")

                # Process each entity type for this DG
                for entity_type in self.process_entity_types:
                    logger.debug(f"Processing {entity_type} for DG {dg.name}")
                    entities = getattr(dg, entity_type)
                    logger.info(f"Found {len(entities)} {entity_type} in DG {dg.name}")
                    
                    for entity in entities:
                        logger.debug(f"Processing {entity_type[:-1]} '{entity.name}' (ID: {entity.dt_id})")

                        # Get appropriate processor method and process entity
                        processor = getattr(self, f'_process_{entity_type[:-1]}')
                        processor(entity, usage_data, report)


            # Handle entities not assigned to any DG if enabled
            if self.process_unassigned:
                logger.info("Processing unassigned entities")
                unassigned_usage_data = await self._collect_unassigned_usage_data()
                logger.info(f"Collected unassigned usage data for {len(unassigned_usage_data)} usage types")
                
                # Create unassigned DG structure
                unassigned_dg = {
                    'name': 'Unassigned',
                    'data': {
                        'information_systems': [],
                        'unassigned_entities': {
                            'entities': {
                                'hosts': [],
                                'applications': [], 
                                'synthetics': []
                            }
                        },
                        'totals': {
                            'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                            'entities': {entity_type: 0 for entity_type in self.entity_types},
                            'managed_hosts': 0
                        }
                    }
                }



                # Process each unassigned entity by type
                for usage_type, usage_values in unassigned_usage_data.items():
                    logger.debug(f"Processing unassigned {usage_type} entities: {len(usage_values)} found")
                    entity_type = self._determine_entity_type(usage_type)
                    if entity_type:
                        for dt_id, value in usage_values.items():
                            # Skip if already processed to avoid duplicates
                            if dt_id in self.processed_entities:
                                logger.debug(f"Entity {entity_type[:-1]} {dt_id} already processed as unassigned - Skipping")
                                continue
                            else:
                                
                                entity_data = {
                                    'dt_id': dt_id,
                                    'name': value['name'],
                                    'usage': {usage_type: value['value']},
                                    'tagged_dgs': [],
                                    'billed': True
                                }
                                unassigned_dg['data']['unassigned_entities']['entities'][entity_type].append(entity_data)
                                self.processed_entities.add(dt_id)
                                logger.debug(f"Added unassigned entity {dt_id} of type {entity_type} with {usage_type} usage: {value}")

                report['dgs'].append(unassigned_dg)
                logger.debug(f"Added unassigned entities to report - Processed {len(self.processed_entities)} unique entities")
                logger.debug(f"Unassigned entity counts - Hosts: {len(unassigned_dg['data']['unassigned_entities']['entities']['hosts'])}, " f"Applications: {len(unassigned_dg['data']['unassigned_entities']['entities']['applications'])}, "f"Synthetics: {len(unassigned_dg['data']['unassigned_entities']['entities']['synthetics'])}")

            logger.info("Report generation completed successfully") 
            # Calculate totals at all levels
            self._calculate_totals(report)
            logger.info(f"Final report contains data for {len(report['dgs'])} DGs")
            logger.debug("Report totals calculated successfully")

            
            # remove dgs that are not in dg_names, keeping Unassigned if process_unassigned is True
            report['dgs'] = [dg for dg in report['dgs'] if dg['name'] in dg_names or (self.process_unassigned and dg['name'] == 'Unassigned')]
            return report

        except Exception as e:
            logger.error(f"Error generating chargeback report: {str(e)}", exc_info=True)
            raise

    async def _collect_usage_data(self, dgs: List[str]) -> Dict:
        """
        Collects usage data for all monitoring capabilities from Dynatrace.
        Maps each collector function to its corresponding usage type.
        
        Args:
            dgs: List of DG names to collect data for
            
        Returns:
            Dict mapping usage types to their collected data
        """
        logger.info("Starting usage data collection from all monitoring sources")
        
        # Map usage types to their collector functions
        usage_collectors = {
            'fullstack': usage_svc.retrieve_hosts_fullstack_usage,
            'infra': usage_svc.retrieve_hosts_infra_usage,
            'rum': usage_svc.retrieve_real_user_monitoring_usage,
            'rum_with_sr': usage_svc.retrieve_real_user_monitoring_with_sr_usage,
            'browser_monitor': usage_svc.retrieve_browser_monitor_usage,
            'http_monitor': usage_svc.retrieve_http_monitor_usage,
            '3rd_party_monitor': usage_svc.retrieve_3rd_party_monitor_usage
        }

        # Collect data for each usage type
        usage_data = {}
        for usage_type, collector in usage_collectors.items():
            # Only collect data for enabled entity types
            entity_type = self._determine_entity_type(usage_type)
            if entity_type in self.process_entity_types:
                logger.debug(f"Collecting {usage_type} usage data")
                data = collector(dgs=dgs)
                usage_data[usage_type] = {item['dt_id']: item['value'] for item in data}
                logger.info(f"Collected {len(data)} {usage_type} usage records")
        
        return usage_data

    async def _collect_unassigned_usage_data(self) -> Dict:
        """
        Collects usage data for unassigned entities.
        Maps each collector function to its corresponding usage type.
        
        Returns:
            Dict mapping usage types to their unassigned entity data
        """
        logger.info("Starting collection of unassigned entity usage data")
        
        # Map usage types to their collector functions for unassigned entities
        usage_collectors = {
            'fullstack': usage_svc.retrieve_unassigned_hosts_fullstack_usage,
            'infra': usage_svc.retrieve_unassigned_hosts_infra_usage,
            'rum': usage_svc.retrieve_unassigned_real_user_monitoring_usage,
            'rum_with_sr': usage_svc.retrieve_unassigned_real_user_monitoring_with_sr_usage,
            'browser_monitor': usage_svc.retrieve_unassigned_browser_monitor_usage,
            'http_monitor': usage_svc.retrieve_unassigned_http_monitor_usage
        }

        # Collect data for each usage type
        usage_data = {}
        for usage_type, collector in usage_collectors.items():
            # Only collect data for enabled entity types
            entity_type = self._determine_entity_type(usage_type)
            if entity_type in self.process_entity_types:
                logger.debug(f"Collecting unassigned {usage_type} usage data")
                data = collector()
                usage_data[usage_type] = {item['dt_id']: {'value': item['value'], 'name': item['name']} for item in data}
                logger.info(f"Collected {len(data)} unassigned {usage_type} usage records")
        
        return usage_data

    def _create_dg_report_structure(self, dg: DG) -> Dict:
        """
        Creates a report structure for a single DG.
        
        Args:
            dg: The DG to create structure for
            
        Returns:
            Dict containing initialized report structure for the DG
        """
        return {
            'name': dg.name,
            'id': dg.id,
            'data': {
                'information_systems': [],
                'unassigned_entities': {
                    'entities': {entity_type: [] for entity_type in self.entity_types},
                    'usage': {usage_type: 0.0 for usage_type in self.usage_types}
                },
                'totals': {
                    'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                    'entities': {entity_type: 0 for entity_type in self.entity_types},
                    'managed_hosts': 0
                }
            }
        }

    def _create_is_report_structure(self, information_system: IS) -> Dict:
        """
        Creates a report structure for a single Information System.
        
        Args:
            information_system: The IS to create structure for
            
        Returns:
            Dict containing initialized report structure for the IS
        """
        return {
            'name': information_system.name,
            'id': information_system.id,
            'managed': information_system.managed,
            'data': {
                'entities': {entity_type: [] for entity_type in self.entity_types},
                'usage': {usage_type: 0.0 for usage_type in self.usage_types}
            }
        }

    def _process_host(self, host: Host, usage_data: Dict, report: Dict) -> Dict:
        """
        Processes a single host entity.
        A host can only be either fullstack or infrastructure monitored, not both.
        Handles hosts that belong to multiple DGs with special priority rules.
        
        Args:
            host: The host entity to process
            usage_data: Dict containing all collected usage data
            report: The report structure to update
        """
        logger.debug(f"Processing host: {host.name} (ID: {host.dt_id})")
        
        fullstack_usage = usage_data['fullstack'].get(host.dt_id, 0.0)
        infra_usage = usage_data['infra'].get(host.dt_id, 0.0)
        
        # Get all DGs this host belongs to
        dgs = [dg.name for dg in host.dgs]
        logger.debug(f"Host {host.name} belongs to DGs: {dgs}")
        logger.debug(f"Host {host.name} usage - Fullstack: {fullstack_usage}, Infrastructure: {infra_usage}")
        
        tagged_dgs = [dg.name for dg in host.dgs]
        processed_dgs = host.dgs
        charged_dgs = []
        
        # Host should be charged to DIGIT C if:
        # 1. It has DIGIT C as a DG, or
        # 2. It's not billable in any of its assigned DGs
        if any(dg.name == 'DIGIT C' for dg in host.dgs) or not any(host_is_billable(host) for dg in host.dgs):
            charged_dgs = [next((dg for dg in processed_dgs if dg.name == 'DIGIT C'), processed_dgs[0])]
        else:
            charged_dgs = host.dgs

        logger.debug(f'DGs to be processed {[dg.name for dg in processed_dgs]}')
        logger.debug(f'DGs to be charged {[dg.name for dg in charged_dgs]}')
        
        # Ensure report contains all relevant DGs
        for dg in processed_dgs:
            if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                logger.debug(f"DG {dg.name} missing from report for host {host.name} - Creating structure")
                report['dgs'].append(self._create_dg_report_structure(dg))
        
        if self.include_non_charged_entities_in_dg == False:
            processed_dgs = charged_dgs
        
        # Process each DG the host belongs to
        for dg in processed_dgs:
            logger.debug(f"Processing dg {dg.name}")
            
            # Always set actual usage values
            usage = {
                'fullstack': fullstack_usage / len(charged_dgs) if fullstack_usage > 0 else 0.0,
                'infra': infra_usage / len(charged_dgs) if infra_usage > 0 and fullstack_usage == 0 else 0.0
            }
            
            logger.debug(f"Host {host.name} consumes  - Fullstack: {fullstack_usage}, Infrastructure: {infra_usage} in DG {dg.name}")

            host_data = {
                'id': host.id,
                'name': host.name,
                'dt_id': host.dt_id,
                'usage': usage,
                'managed': host.managed,
                'cloud': host.cloud,
                'billed': host_is_billable(host) and (dg in charged_dgs),
                'tagged_dgs': tagged_dgs
            }            
                
            dg_index = next((i for i, dg_report in enumerate(report['dgs']) if dg_report['name'] == dg.name), None)
            if dg_index is None:
                logger.warning(f"DG {dg.name} not found in report - Skipping")
                continue

            # Check if host belongs to an IS in this DG
            matching_is = next((is_obj for is_obj in dg.information_systems if host in is_obj.hosts), None)
            if matching_is:
                logger.debug(f"Found matching IS {matching_is.name} for host {host.name} in DG {dg.name}")
                is_index = next((i for i, is_report in enumerate(report['dgs'][dg_index]['data']['information_systems']) if is_report['name'] == matching_is.name), None)
                
                if is_index is None:
                    logger.debug(f"Creating new IS structure for {matching_is.name}")
                    report['dgs'][dg_index]['data']['information_systems'].append(
                        self._create_is_report_structure(matching_is))
                    is_index = -1
                
                if any(d['dt_id'] == host_data['dt_id'] for d in report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['hosts']):
                    continue
                else:
                    report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['hosts'].append(host_data)
                    logger.debug(f"Added host {host.name} to IS {matching_is.name} in DG {dg.name}")
            else:
                # Avoid duplicate entries
                if any(d['dt_id'] == host_data['dt_id'] for d in report['dgs'][dg_index]['data']['unassigned_entities']['entities']['hosts']):
                    continue
                else:
                    logger.debug(f"No matching IS found for host {host.name} in DG {dg.name} - Adding to unassigned")
                    report['dgs'][dg_index]['data']['unassigned_entities']['entities']['hosts'].append(host_data)
                logger.debug(f"Added host {host.name} to unassigned entities in DG {dg.name}")
            
        self.processed_entities.add(host.dt_id)

    def _process_application(self, app: Application, usage_data: Dict, report: Dict) -> Dict:
        """
        Processes a single application entity.
        Handles RUM and Session Replay usage.
        Handles applications that belong to multiple DGs with special priority rules.
        
        Args:
            app: The application entity to process
            usage_data: Dict containing all collected usage data
            report: The report structure to update
        """
        logger.debug(f"Processing application: {app.name} (ID: {app.dt_id})")

        if app.dt_id in self.processed_entities:
            logger.debug(f"Application {app.dt_id} already processed - Skipping")
            return

        rum_usage = usage_data['rum'].get(app.dt_id, 0.0)
        rum_sr_usage = usage_data['rum_with_sr'].get(app.dt_id, 0.0)
        
        logger.debug(f"Application {app.name} usage - RUM: {rum_usage}, RUM+SR: {rum_sr_usage}")

        # Get all DGs this application belongs to
        dgs = [dg.name for dg in app.dgs]
        logger.debug(f"Application {app.name} belongs to DGs: {dgs}")

        tagged_dgs = [dg.name for dg in app.dgs]
        processed_dgs = app.dgs
        charged_dgs = []
        
        # Apply DG priority rules - DIGIT C > Others
        if any(dg.name == 'DIGIT C' for dg in app.dgs):
            charged_dgs = [next(dg for dg in app.dgs if dg.name == 'DIGIT C')]
            logger.debug(f"Application {app.name} prioritized to DIGIT C")
        else:
            charged_dgs = app.dgs
            logger.debug(f"Application {app.name} processed for all assigned DGs")

        # Ensure report contains all relevant DGs
        for dg in processed_dgs:
            if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                logger.debug(f"DG {dg.name} missing from report for application {app.name} - Creating structure")
                report['dgs'].append(self._create_dg_report_structure(dg))

        if self.include_non_charged_entities_in_dg == False:
            processed_dgs = charged_dgs

        # Process each DG the application belongs to
        for dg in processed_dgs:
            usage = {}
            logger.debug(f"Processing dg {dg.name}")
            if dg in charged_dgs:
                usage = {
                    'rum': (rum_usage / len(charged_dgs)) if rum_usage > 0 else 0.0,
                    'rum_with_sr': (rum_sr_usage / len(charged_dgs)) if rum_sr_usage > 0 else 0.0
                }
            else:
                usage = {
                    'rum': 0.0,
                    'rum_with_sr': 0.0
                }

            app_data = {
                'id': app.id,
                'name': app.name,
                'dt_id': app.dt_id,
                'usage': usage,
                'managed': False,
                'billed': app_is_billable(app),
                'tagged_dgs': tagged_dgs
            }

            dg_index = next((i for i, dg_report in enumerate(report['dgs']) if dg_report['name'] == dg.name), None)
            if dg_index is None:
                logger.warning(f"DG {dg.name} not found in report - Skipping")
                continue

            # Check if application belongs to an IS in this DG
            matching_is = next((is_obj for is_obj in dg.information_systems if app in is_obj.applications), None)
            if matching_is:
                logger.debug(f"Found matching IS {matching_is.name} for application {app.name} in DG {dg.name}")
                is_index = next((i for i, is_report in enumerate(report['dgs'][dg_index]['data']['information_systems']) 
                               if is_report['name'] == matching_is.name), None)
                
                if is_index is None:
                    logger.debug(f"Creating new IS structure for {matching_is.name}")
                    report['dgs'][dg_index]['data']['information_systems'].append(
                        self._create_is_report_structure(matching_is))
                    is_index = -1
                
                if any(d['dt_id'] == app_data['dt_id'] for d in report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['applications']):
                    continue
                else:
                    report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['applications'].append(app_data)
                    logger.debug(f"Added application {app.name} to IS {matching_is.name} in DG {dg.name}")
            else:
                # Avoid duplicate entries
                if any(d['dt_id'] == app_data['dt_id'] for d in report['dgs'][dg_index]['data']['unassigned_entities']['entities']['applications']):
                    continue
                else:
                    logger.debug(f"No matching IS found for application {app.name} in DG {dg.name} - Adding to unassigned")
                    report['dgs'][dg_index]['data']['unassigned_entities']['entities']['applications'].append(app_data)
                logger.debug(f"Added application {app.name} to unassigned entities in DG {dg.name}")

        self.processed_entities.add(app.dt_id)

    def _process_synthetic(self, synthetic: Synthetic, usage_data: Dict, report: Dict) -> Dict:
        """
        Processes a single synthetic monitor entity.
        Handles browser, HTTP, and third-party monitor usage.
        Handles synthetic monitors that belong to multiple DGs with special priority rules.
        
        Args:
            synthetic: The synthetic monitor entity to process
            usage_data: Dict containing all collected usage data
            report: The report structure to update
        """
        logger.debug(f"Processing synthetic monitor: {synthetic.name} (ID: {synthetic.dt_id})")

        if synthetic.dt_id in self.processed_entities:
            logger.debug(f"Synthetic monitor {synthetic.dt_id} already processed - Skipping")
            return

        browser_usage = usage_data['browser_monitor'].get(synthetic.dt_id, 0.0)
        http_usage = usage_data['http_monitor'].get(synthetic.dt_id, 0.0)
        third_party_usage = usage_data['3rd_party_monitor'].get(synthetic.dt_id, 0.0)
        
        logger.debug(f"Synthetic {synthetic.name} usage - Browser: {browser_usage}, HTTP: {http_usage}, 3rd Party: {third_party_usage}")

        # Get all DGs this synthetic belongs to
        dgs = [dg.name for dg in synthetic.dgs]
        logger.debug(f"Synthetic {synthetic.name} belongs to DGs: {dgs}")

        tagged_dgs = [dg.name for dg in synthetic.dgs]
        processed_dgs = synthetic.dgs
        charged_dgs = []
        
        # Apply DG priority rules - DIGIT C > Others
        if any(dg.name == 'DIGIT C' for dg in synthetic.dgs):
            charged_dgs = [next(dg for dg in synthetic.dgs if dg.name == 'DIGIT C')]
            logger.debug(f"Synthetic {synthetic.name} prioritized to DIGIT C")
        
        else:
            charged_dgs = synthetic.dgs
            logger.debug(f"Synthetic {synthetic.name} processed for all assigned DGs")

        # Ensure report contains all relevant DGs
        for dg in processed_dgs:
            if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                logger.debug(f"DG {dg.name} missing from report for synthetic {synthetic.name} - Creating structure")
                report['dgs'].append(self._create_dg_report_structure(dg))

        if self.include_non_charged_entities_in_dg == False:
            processed_dgs = charged_dgs

        # Process each DG the synthetic belongs to
        for dg in processed_dgs:
            usage = {}
            logger.debug(f"Processing dg {dg.name}")
            if dg in charged_dgs:
                usage = {
                    'browser_monitor': (browser_usage / len(charged_dgs)) if browser_usage > 0 else 0.0,
                    'http_monitor': (http_usage / len(charged_dgs)) if http_usage > 0 else 0.0,
                    '3rd_party_monitor': (third_party_usage / len(charged_dgs)) if third_party_usage > 0 else 0.0
                }
            else:
                usage = {
                    'browser_monitor': 0.0,
                    'http_monitor': 0.0,
                    '3rd_party_monitor': 0.0
                }

            synthetic_data = {
                'id': synthetic.id,
                'managed': False,
                'name': synthetic.name,
                'dt_id': synthetic.dt_id,
                'usage': usage,
                'billed': synthetic_is_billable(synthetic),
                'tagged_dgs': tagged_dgs
            }

            dg_index = next((i for i, dg_report in enumerate(report['dgs']) if dg_report['name'] == dg.name), None)
            if dg_index is None:
                logger.warning(f"DG {dg.name} not found in report - Skipping")
                continue

            # Check if synthetic belongs to an IS in this DG
            matching_is = next((is_obj for is_obj in dg.information_systems if synthetic in is_obj.synthetics), None)
            if matching_is:
                logger.debug(f"Found matching IS {matching_is.name} for synthetic {synthetic.name} in DG {dg.name}")
                is_index = next((i for i, is_report in enumerate(report['dgs'][dg_index]['data']['information_systems']) 
                               if is_report['name'] == matching_is.name), None)
                
                if is_index is None:
                    logger.debug(f"Creating new IS structure for {matching_is.name}")
                    report['dgs'][dg_index]['data']['information_systems'].append(
                        self._create_is_report_structure(matching_is))
                    is_index = -1
                
                if any(d['dt_id'] == synthetic_data['dt_id'] for d in report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['synthetics']):
                    continue
                else:
                    report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['synthetics'].append(synthetic_data)
                    logger.debug(f"Added synthetic {synthetic.name} to IS {matching_is.name} in DG {dg.name}")
            else:
                # Avoid duplicate entries
                if any(d['dt_id'] == synthetic_data['dt_id'] for d in report['dgs'][dg_index]['data']['unassigned_entities']['entities']['synthetics']):
                    continue
                else:
                    logger.debug(f"No matching IS found for synthetic {synthetic.name} in DG {dg.name} - Adding to unassigned")
                    report['dgs'][dg_index]['data']['unassigned_entities']['entities']['synthetics'].append(synthetic_data)
                logger.debug(f"Added synthetic {synthetic.name} to unassigned entities in DG {dg.name}")

        self.processed_entities.add(synthetic.dt_id)

    def _calculate_totals(self, report: Dict) -> Dict:
        """
        Calculates usage and entity totals at all levels (Report, DG, IS).
        Handles both assigned and unassigned entities.
        Only includes usage for entities marked as billed=True.
        
        Args:
            report: The report structure to calculate totals for
            
        Returns:
            Updated report with calculated totals
        """
        # Initialize report totals
        report['totals']['usage'] = {usage_type: 0.0 for usage_type in self.usage_types}
        report['totals']['entities'] = {entity_type: 0 for entity_type in self.entity_types}
        report['totals']['managed_hosts'] = 0
        
        report['unassigned_totals']['usage'] = {usage_type: 0.0 for usage_type in self.usage_types}
        report['unassigned_totals']['entities'] = {entity_type: 0 for entity_type in self.entity_types}
        report['unassigned_totals']['managed_hosts'] = 0

        for dg in report['dgs']:
            dg_totals = {usage_type: 0.0 for usage_type in self.usage_types}
            dg_entity_totals = {entity_type: 0 for entity_type in self.entity_types}
            dg_managed_hosts = 0
            
            # Calculate IS totals
            for is_system in dg['data']['information_systems']:
                is_totals = {usage_type: 0.0 for usage_type in self.usage_types}
                is_entity_totals = {entity_type: 0 for entity_type in self.entity_types}
                
                # Sum host usage and count
                hosts = is_system['data']['entities'].get('hosts', [])
                for host in hosts:
                    if host.get('billed', False):  # Only count if billed
                        for usage_type, value in host['usage'].items():
                            is_totals[usage_type] += value
                            dg_totals[usage_type] += value
                    if host.get('managed', False):
                        dg_managed_hosts += 1
                is_entity_totals['hosts'] = len(hosts)
                dg_entity_totals['hosts'] += len(hosts)
                
                # Sum application usage and count
                apps = is_system['data']['entities'].get('applications', [])
                for app in apps:
                    if app.get('billed', False):  # Only count if billed
                        for usage_type, value in app['usage'].items():
                            is_totals[usage_type] += value
                            dg_totals[usage_type] += value
                is_entity_totals['applications'] = len(apps)
                dg_entity_totals['applications'] += len(apps)
                        
                # Sum synthetic usage and count
                synthetics = is_system['data']['entities'].get('synthetics', [])
                for synthetic in synthetics:
                    if synthetic.get('billed', False):  # Only count if billed
                        for usage_type, value in synthetic['usage'].items():
                            is_totals[usage_type] += value
                            dg_totals[usage_type] += value
                is_entity_totals['synthetics'] = len(synthetics)
                dg_entity_totals['synthetics'] += len(synthetics)
                        
                is_system['data']['usage'] = is_totals

            
            # Add unassigned entities to DG totals
            unassigned = dg['data']['unassigned_entities']['entities']
            unassigned_usage = {usage_type: 0.0 for usage_type in self.usage_types}
            
            # Sum unassigned host usage and count
            unassigned_hosts = unassigned.get('hosts', [])
            for host in unassigned_hosts:
                if host.get('billed', False):  # Only count if billed
                    for usage_type, value in host['usage'].items():
                        unassigned_usage[usage_type] += value
                        dg_totals[usage_type] += value
                if host.get('managed', False):
                    dg_managed_hosts += 1
            dg_entity_totals['hosts'] += len(unassigned_hosts)
                    
            # Sum unassigned application usage and count
            unassigned_apps = unassigned.get('applications', [])
            for app in unassigned_apps:
                if app.get('billed', False):  # Only count if billed
                    for usage_type, value in app['usage'].items():
                        dg_totals[usage_type] += value
            dg_entity_totals['applications'] += len(unassigned_apps)
                    
            # Sum unassigned synthetic usage and count
            unassigned_synthetics = unassigned.get('synthetics', [])
            for synthetic in unassigned_synthetics:
                if synthetic.get('billed', False):  # Only count if billed
                    for usage_type, value in synthetic['usage'].items():
                        dg_totals[usage_type] += value
            dg_entity_totals['synthetics'] += len(unassigned_synthetics)
            
            dg['data']['totals']['usage'] = dg_totals
            dg['data']['totals']['entities'] = dg_entity_totals
            dg['data']['totals']['managed_hosts'] = dg_managed_hosts
            
            # Add DG totals to either report totals or unassigned totals based on DG name
            if dg['name'] == 'Unassigned':
                for usage_type, value in dg_totals.items():
                    report['unassigned_totals']['usage'][usage_type] += value
                for entity_type, count in dg_entity_totals.items():
                    report['unassigned_totals']['entities'][entity_type] += count
                report['unassigned_totals']['managed_hosts'] += dg_managed_hosts
            else:
                for usage_type, value in dg_totals.items():
                    report['totals']['usage'][usage_type] += value
                for entity_type, count in dg_entity_totals.items():
                    report['totals']['entities'][entity_type] += count
                report['totals']['managed_hosts'] += dg_managed_hosts
            
        return report

    def _determine_entity_type(self, usage_type: str) -> str:
        """
        Maps usage types to their corresponding entity types.
        Used for categorizing unassigned entities.
        """
        usage_to_entity = {
            'fullstack': 'hosts',
            'infra': 'hosts',
            'rum': 'applications',
            'rum_with_sr': 'applications',
            'browser_monitor': 'synthetics',
            'http_monitor': 'synthetics',
            '3rd_party_monitor': 'synthetics'
        }
        entity_type = usage_to_entity.get(usage_type)
        logger.debug(f"Mapped usage type {usage_type} to entity type {entity_type}")
        return entity_type