from typing import List, Dict
from collections import defaultdict
import logging
from sqlalchemy.orm import Session
from ..models import DG, IS, Host, Application, Synthetic
from . import usage as usage_svc

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ChargebackReport:
    """
    Generates chargeback reports for Dynatrace usage across DGs and Information Systems.
    Calculates usage metrics for hosts, applications and synthetic monitors.
    """

    def __init__(self, db: Session, process_unassigned: bool = False, entity_types: List[str] = None):
        self.db = db
        # Define all possible usage types that we track
        self.usage_types = [
            'fullstack',        # Full-stack monitoring for hosts
            'infra',           # Infrastructure-only monitoring for hosts  
            'rum',             # Real User Monitoring for applications
            'rum_with_sr',     # RUM with Session Replay for applications
            'browser_monitor', # Browser synthetic monitors
            'http_monitor',    # HTTP synthetic monitors
            '3rd_party_monitor' # Third-party synthetic monitors
        ]

        self.process_unassigned = process_unassigned
        self.process_entity_types = entity_types or ['hosts', 'applications', 'synthetics']
        self.entity_types = ['hosts', 'applications', 'synthetics']
        logger.info(f"Initialized ChargebackReport with process_unassigned={process_unassigned}, entity_types={entity_types}")

    async def generate_report(self, dg_names: List[str]) -> Dict:
        """
        Main entry point to generate a complete chargeback report.
        Takes a list of DG names and returns a structured report of all usage.
        """
        logger.info(f"Starting report generation for DGs: {dg_names}")
        
        try:
            # First collect all usage data from Dynatrace
            logger.debug("Collecting usage data from Dynatrace")
            usage_data = await self._collect_usage_data(dg_names)
            logger.debug(f"Collected usage data for {len(usage_data)} usage types")

            # Initialize report structure
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
                    logger.warning(f"DG {dg_name} not found in database, skipping")
                    continue
                
                # Create the DG report structure if it doesn't exist yet
                if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                    report['dgs'].append(self._create_dg_report_structure(dg))

                for entity_type in self.process_entity_types:
                    logger.debug(f"Processing {entity_type} for DG {dg.name}")
                    entities = getattr(dg, entity_type)
                    for entity in entities:
                        logger.debug(f"Processing {entity_type[:-1]} {entity.name} (ID: {entity.dt_id})")

                        # Process entity and add to report
                        processor = getattr(self, f'_process_{entity_type[:-1]}')
                        processor(entity, usage_data, report)


            # Handle entities not assigned to any DG if enabled
            if self.process_unassigned:
                unassigned_usage_data = await self._collect_unassigned_usage_data()
                logger.info("Processing unassigned entities")
                unassigned_dg = await self._process_unassigned_dg(usage_data, dg_names)
                report['dgs'].append({
                    'name': 'Unassigned',
                    'data': unassigned_dg
                })
                logger.debug("Updating unassigned totals")
                self._update_unassigned_totals(report, unassigned_dg)

            logger.info("Report generation completed successfully")
            
            # Calculate totals at all levels....
            self._calculate_totals(report)
            logger.debug(f"Final report contains data for {len(report['dgs'])} DGs")


            return report

        except Exception as e:
            logger.error(f"Error generating chargeback report: {str(e)}", exc_info=True)
            raise


    async def _collect_usage_data(self, dgs: List[str]) -> Dict:
        """
        Collects usage data for all monitoring capabilities from Dynatrace.
        Maps each collector function to its corresponding usage type.
        """
        logger.info("Collecting usage data from all sources")
        
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
                logger.debug(f"Collected {len(data)} {usage_type} usage records")
        
        return usage_data

    async def _collect_unassigned_usage_data(self) -> Dict:
        """
        Collects usage data for unassigned entities.
        """
        logger.info("Collecting unassigned usage data")
        data = await usage_svc.retrieve_unassigned_usage()
        return data


    def _create_dg_report_structure(self, dg: DG) -> Dict:
        """
        Creates a report structure for a single DG.
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
        Creates a report structure for a single IS.
        """
        return {
            'name': information_system.name,
            'id': information_system.id,
            'data': {
                'entities': {entity_type: [] for entity_type in self.entity_types},
                'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                'totals': {
                    'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                    'entities': {entity_type: 0 for entity_type in self.entity_types},
                    'managed_hosts': 0
                }
            }
        }

    def _process_host(self, host: Host, usage_data: Dict, report: Dict) -> Dict:
        """
        Processes a single host entity.
        A host can only be either fullstack or infrastructure monitored, not both.
        Now handles hosts that belong to multiple DGs.
        """
        logger.debug(f"Processing host: {host.name} (ID: {host.dt_id})")

        fullstack_usage = usage_data['fullstack'].get(host.dt_id, 0.0)
        infra_usage = usage_data['infra'].get(host.dt_id, 0.0)
        
        logger.debug(f"Host {host.name} fullstack usage: {fullstack_usage}, infra usage: {infra_usage}")
                
        
        # ToDo: Break if this host is already included in the report
        
        # Get all DGs this host belongs to
        dgs = [dg.name for dg in host.dgs]
        logger.debug(f"Host {host.name} belongs to DGs: {dgs}")

        
        tagged_dgs = [dg.name for dg in host.dgs]
        processed_dgs = []
        # Check if the host belongs to DIGIT or DIGIT C, prioritize DIGIT C
        if any(dg.name == 'DIGIT C' for dg in host.dgs):
            processed_dgs = [next(dg for dg in host.dgs if dg.name == 'DIGIT C')]
        elif any(dg.name == 'DIGIT' for dg in host.dgs):
            processed_dgs = [next(dg for dg in host.dgs if dg.name == 'DIGIT')]
        else:
            processed_dgs = host.dgs

        # Check that the report contains all DGs the host belongs to
        for dg in processed_dgs:
            if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                logger.warning(f"DG {dg.name} not found in report for host {host.name}, creating new DG report structure")
                # Create a new DG report structure
                report['dgs'].append(self._create_dg_report_structure(dg))
        
        # Only include usage if it's greater than 0, and if there's more than one DG, distribute the usage across the DGs
        usage = {
            'fullstack': (fullstack_usage / len(processed_dgs)) if fullstack_usage > 0 else 0.0,
            'infra': (infra_usage / len(processed_dgs)) if infra_usage > 0 and fullstack_usage == 0 else 0.0
        }

        host_data = {
            'id': host.id,
            'name': host.name,
            'dt_id': host.dt_id,
            'usage': usage,
            'managed': host.managed,
            'tagged_dgs': tagged_dgs
        }

        # for each DG, add the host to the DG in the report, but also process if it belongs to an IS
        for dg in processed_dgs:
            logger.debug(f"Processing DG {dg.name} for host {host.name}")
            
            # Find the index of the DG in the report
            dg_index = next((i for i, dg_report in enumerate(report['dgs']) if dg_report['name'] == dg.name), None)
            if dg_index is None:
                logger.warning(f"DG {dg.name} not found in report, skipping")
                continue

            matching_is = next((is_obj for is_obj in dg.information_systems if host in is_obj.hosts), None)
            if matching_is:
                logger.debug(f"Found matching IS {matching_is.name} for host {host.name} in DG {dg.name}")
                # Find if IS already exists in report
                is_index = next((i for i, is_report in enumerate(report['dgs'][dg_index]['data']['information_systems']) 
                               if is_report['name'] == matching_is.name), None)
                
                if is_index is None:
                    logger.debug(f"Creating new IS report structure for {matching_is.name}")
                    report['dgs'][dg_index]['data']['information_systems'].append(
                        self._create_is_report_structure(matching_is))
                    is_index = -1
                
                # Add the host_data to the matching IS in the report
                report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['hosts'].append(host_data)
                logger.debug(f"Successfully added host {host.name} to IS {matching_is.name}")
            else:
                logger.debug(f"No matching IS found for host {host.name} in DG {dg.name}, adding to unassigned entities")
                # If no matching IS found for this DG, add to unassigned entities
                report['dgs'][dg_index]['data']['unassigned_entities']['entities']['hosts'].append(host_data)
                logger.debug(f"Added host {host.name} to unassigned entities for DG {dg.name}")

    def _process_application(self, app: Application, usage_data: Dict, report: Dict) -> Dict:
        """
        Processes a single application entity.
        Handles RUM and Session Replay usage.
        Now handles applications that belong to multiple DGs.
        """
        logger.debug(f"Processing application: {app.name} (ID: {app.dt_id})")

        rum_usage = usage_data['rum'].get(app.dt_id, 0.0)
        rum_sr_usage = usage_data['rum_with_sr'].get(app.dt_id, 0.0)
        
        logger.debug(f"Application {app.name} RUM usage: {rum_usage}, RUM+SR usage: {rum_sr_usage}")

        # Get all DGs this application belongs to
        dgs = [dg.name for dg in app.dgs]
        logger.debug(f"Application {app.name} belongs to DGs: {dgs}")

        tagged_dgs = [dg.name for dg in app.dgs]
        processed_dgs = []
        # Check if the application belongs to DIGIT or DIGIT C, prioritize DIGIT C
        if any(dg.name == 'DIGIT C' for dg in app.dgs):
            processed_dgs = [next(dg for dg in app.dgs if dg.name == 'DIGIT C')]
        elif any(dg.name == 'DIGIT' for dg in app.dgs):
            processed_dgs = [next(dg for dg in app.dgs if dg.name == 'DIGIT')]
        else:
            processed_dgs = app.dgs

        # Check that the report contains all DGs the application belongs to
        for dg in processed_dgs:
            if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                logger.warning(f"DG {dg.name} not found in report for application {app.name}, creating new DG report structure")
                report['dgs'].append(self._create_dg_report_structure(dg))

        # Distribute usage across DGs if there's more than one
        usage = {
            'rum': (rum_usage / len(processed_dgs)) if rum_usage > 0 else 0.0,
            'rum_with_sr': (rum_sr_usage / len(processed_dgs)) if rum_sr_usage > 0 else 0.0
        }

        app_data = {
            'id': app.id,
            'name': app.name,
            'dt_id': app.dt_id,
            'usage': usage,
            'tagged_dgs': tagged_dgs
        }

        # Process each DG the application belongs to
        for dg in processed_dgs:
            logger.debug(f"Processing DG {dg.name} for application {app.name}")
            
            dg_index = next((i for i, dg_report in enumerate(report['dgs']) if dg_report['name'] == dg.name), None)
            if dg_index is None:
                logger.warning(f"DG {dg.name} not found in report, skipping")
                continue

            matching_is = next((is_obj for is_obj in dg.information_systems if app in is_obj.applications), None)
            if matching_is:
                logger.debug(f"Found matching IS {matching_is.name} for application {app.name} in DG {dg.name}")
                is_index = next((i for i, is_report in enumerate(report['dgs'][dg_index]['data']['information_systems']) 
                               if is_report['name'] == matching_is.name), None)
                
                if is_index is None:
                    logger.debug(f"Creating new IS report structure for {matching_is.name}")
                    report['dgs'][dg_index]['data']['information_systems'].append(
                        self._create_is_report_structure(matching_is))
                    is_index = -1
                
                report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['applications'].append(app_data)
                logger.debug(f"Successfully added application {app.name} to IS {matching_is.name}")
            else:
                logger.debug(f"No matching IS found for application {app.name} in DG {dg.name}, adding to unassigned entities")
                report['dgs'][dg_index]['data']['unassigned_entities']['entities']['applications'].append(app_data)
                logger.debug(f"Added application {app.name} to unassigned entities for DG {dg.name}")

    def _process_synthetic(self, synthetic: Synthetic, usage_data: Dict, report: Dict) -> Dict:
        """
        Processes a single synthetic monitor entity.
        Handles browser, HTTP, and third-party monitor usage.
        Now handles synthetic monitors that belong to multiple DGs.
        """
        logger.debug(f"Processing synthetic monitor: {synthetic.name} (ID: {synthetic.dt_id})")

        browser_usage = usage_data['browser_monitor'].get(synthetic.dt_id, 0.0)
        http_usage = usage_data['http_monitor'].get(synthetic.dt_id, 0.0)
        third_party_usage = usage_data['3rd_party_monitor'].get(synthetic.dt_id, 0.0)
        
        logger.debug(f"Synthetic {synthetic.name} usage - Browser: {browser_usage}, HTTP: {http_usage}, 3rd Party: {third_party_usage}")

        # Get all DGs this synthetic belongs to
        dgs = [dg.name for dg in synthetic.dgs]
        logger.debug(f"Synthetic {synthetic.name} belongs to DGs: {dgs}")

        tagged_dgs = [dg.name for dg in synthetic.dgs]
        processed_dgs = []
        # Check if the synthetic belongs to DIGIT or DIGIT C, prioritize DIGIT C
        if any(dg.name == 'DIGIT C' for dg in synthetic.dgs):
            processed_dgs = [next(dg for dg in synthetic.dgs if dg.name == 'DIGIT C')]
        elif any(dg.name == 'DIGIT' for dg in synthetic.dgs):
            processed_dgs = [next(dg for dg in synthetic.dgs if dg.name == 'DIGIT')]
        else:
            processed_dgs = synthetic.dgs

        # Check that the report contains all DGs the synthetic belongs to
        for dg in processed_dgs:
            if not any(dg_report['name'] == dg.name for dg_report in report['dgs']):
                logger.warning(f"DG {dg.name} not found in report for synthetic {synthetic.name}, creating new DG report structure")
                report['dgs'].append(self._create_dg_report_structure(dg))

        # Distribute usage across DGs if there's more than one
        usage = {
            'browser_monitor': (browser_usage / len(processed_dgs)) if browser_usage > 0 else 0.0,
            'http_monitor': (http_usage / len(processed_dgs)) if http_usage > 0 else 0.0,
            '3rd_party_monitor': (third_party_usage / len(processed_dgs)) if third_party_usage > 0 else 0.0
        }

        synthetic_data = {
            'id': synthetic.id,
            'name': synthetic.name,
            'dt_id': synthetic.dt_id,
            'usage': usage,
            'tagged_dgs': tagged_dgs
        }

        # Process each DG the synthetic belongs to
        for dg in processed_dgs:
            logger.debug(f"Processing DG {dg.name} for synthetic {synthetic.name}")
            
            dg_index = next((i for i, dg_report in enumerate(report['dgs']) if dg_report['name'] == dg.name), None)
            if dg_index is None:
                logger.warning(f"DG {dg.name} not found in report, skipping")
                continue

            matching_is = next((is_obj for is_obj in dg.information_systems if synthetic in is_obj.synthetics), None)
            if matching_is:
                logger.debug(f"Found matching IS {matching_is.name} for synthetic {synthetic.name} in DG {dg.name}")
                is_index = next((i for i, is_report in enumerate(report['dgs'][dg_index]['data']['information_systems']) 
                               if is_report['name'] == matching_is.name), None)
                
                if is_index is None:
                    logger.debug(f"Creating new IS report structure for {matching_is.name}")
                    report['dgs'][dg_index]['data']['information_systems'].append(
                        self._create_is_report_structure(matching_is))
                    is_index = -1
                
                report['dgs'][dg_index]['data']['information_systems'][is_index]['data']['entities']['synthetics'].append(synthetic_data)
                logger.debug(f"Successfully added synthetic {synthetic.name} to IS {matching_is.name}")
            else:
                logger.debug(f"No matching IS found for synthetic {synthetic.name} in DG {dg.name}, adding to unassigned entities")
                report['dgs'][dg_index]['data']['unassigned_entities']['entities']['synthetics'].append(synthetic_data)
                logger.debug(f"Added synthetic {synthetic.name} to unassigned entities for DG {dg.name}")

    def _calculate_totals(self, report: Dict) -> Dict:
        """
        Calculates totals at all levels.
        """
        # Initialize report totals
        report['totals']['usage'] = {usage_type: 0.0 for usage_type in self.usage_types}
        report['totals']['entities'] = {entity_type: 0 for entity_type in self.entity_types}
        report['totals']['managed_hosts'] = 0

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
                    for usage_type, value in app['usage'].items():
                        is_totals[usage_type] += value
                        dg_totals[usage_type] += value
                is_entity_totals['applications'] = len(apps)
                dg_entity_totals['applications'] += len(apps)
                        
                # Sum synthetic usage and count
                synthetics = is_system['data']['entities'].get('synthetics', [])
                for synthetic in synthetics:
                    for usage_type, value in synthetic['usage'].items():
                        is_totals[usage_type] += value
                        dg_totals[usage_type] += value
                is_entity_totals['synthetics'] = len(synthetics)
                dg_entity_totals['synthetics'] += len(synthetics)
                        
                is_system['data']['totals']['usage'] = is_totals
                is_system['data']['totals']['entities'] = is_entity_totals
            
            # Add unassigned entities to DG totals
            unassigned = dg['data']['unassigned_entities']['entities']
            
            # Sum unassigned host usage and count
            unassigned_hosts = unassigned.get('hosts', [])
            for host in unassigned_hosts:
                for usage_type, value in host['usage'].items():
                    dg_totals[usage_type] += value
                if host.get('managed', False):
                    dg_managed_hosts += 1
            dg_entity_totals['hosts'] += len(unassigned_hosts)
                    
            # Sum unassigned application usage and count
            unassigned_apps = unassigned.get('applications', [])
            for app in unassigned_apps:
                for usage_type, value in app['usage'].items():
                    dg_totals[usage_type] += value
            dg_entity_totals['applications'] += len(unassigned_apps)
                    
            # Sum unassigned synthetic usage and count
            unassigned_synthetics = unassigned.get('synthetics', [])
            for synthetic in unassigned_synthetics:
                for usage_type, value in synthetic['usage'].items():
                    dg_totals[usage_type] += value
            dg_entity_totals['synthetics'] += len(unassigned_synthetics)
            
            dg['data']['totals']['usage'] = dg_totals
            dg['data']['totals']['entities'] = dg_entity_totals
            dg['data']['totals']['managed_hosts'] = dg_managed_hosts
            
            # Add DG totals to report totals
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