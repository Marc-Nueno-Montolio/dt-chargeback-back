from typing import List, Dict
from collections import defaultdict
import logging
from sqlalchemy.orm import Session
from ..models import DG, IS, Host, Application, Synthetic
from . import usage as usage_svc
from ..dynatrace import (
    query_unassigned_host_full_stack_usage,
    query_unassigned_host_infra_usage,
    query_unassigned_real_user_monitoring_usage,
    query_unassigned_real_user_monitoring_with_sr_usage,
    query_unassigned_browser_monitor_usage,
    query_unassigned_http_monitor_usage,
    query_unassigned_3rd_party_monitor_usage
)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ChargebackReport:
    """
    Generates chargeback reports for Dynatrace usage across DGs and Information Systems.
    Calculates usage metrics for hosts, applications and synthetic monitors.
    """

    def __init__(self, db: Session):
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
        # Track assigned entities to prevent duplicates
        self.assigned_entities = {
            'hosts': set(),
            'applications': set(), 
            'synthetics': set()
        }

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

            # Initialize report structure
            report = {
                'dgs': [],
                'totals': {
                    'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                    'entities': {'hosts': 0, 'applications': 0, 'synthetics': 0},
                    'managed_hosts': 0
                },
                'unassigned_totals': {
                    'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                    'entities': {'hosts': 0, 'applications': 0, 'synthetics': 0},
                    'managed_hosts': 0
                }
            }

            # Process DIGIT and DIGIT C first
            logger.debug("Processing DIGIT/DIGIT C DGs first")
            priority_dgs = ['DIGIT', 'DIGIT C']
            other_dgs = [dg for dg in dg_names if dg not in priority_dgs]
            
            for dg_name in priority_dgs:
                if dg_name in dg_names:
                    dg = self.db.query(DG).filter(DG.name == dg_name).first()
                    if dg:
                        dg_data = await self._process_dg(dg, usage_data)
                        report['dgs'].append({
                            'name': dg_name,
                            'data': dg_data
                        })
                        self._update_report_totals(report, dg_data)

            # Then process remaining DGs
            for dg_name in other_dgs:
                dg = self.db.query(DG).filter(DG.name == dg_name).first()
                if not dg:
                    logger.warning(f"DG {dg_name} not found in database")
                    continue

                dg_data = await self._process_dg(dg, usage_data)
                report['dgs'].append({
                    'name': dg_name,
                    'data': dg_data
                })
                self._update_report_totals(report, dg_data)

            # Handle entities not assigned to any DG
            logger.info("Processing unassigned entities")
            unassigned_dg = await self._process_unassigned_dg(usage_data, dg_names)
            report['dgs'].append({
                'name': 'Unassigned',
                'data': unassigned_dg
            })
            self._update_unassigned_totals(report, unassigned_dg)

            logger.info("Report generation completed successfully")
            return report

        except Exception as e:
            logger.error(f"Error generating chargeback report: {str(e)}", exc_info=True)
            raise

    def _update_report_totals(self, report: Dict, dg_data: Dict):
        """Helper method to update report totals"""
        for usage_type in self.usage_types:
            report['totals']['usage'][usage_type] += dg_data['totals']['usage'][usage_type]
        for entity_type in ['hosts', 'applications', 'synthetics']:
            report['totals']['entities'][entity_type] += dg_data['totals']['entities'][entity_type]
        report['totals']['managed_hosts'] += dg_data['totals']['managed_hosts']

    def _update_unassigned_totals(self, report: Dict, dg_data: Dict):
        """Helper method to update unassigned totals"""
        for usage_type in self.usage_types:
            report['unassigned_totals']['usage'][usage_type] += dg_data['totals']['usage'][usage_type]
        for entity_type in ['hosts', 'applications', 'synthetics']:
            report['unassigned_totals']['entities'][entity_type] += dg_data['totals']['entities'][entity_type]
        report['unassigned_totals']['managed_hosts'] += dg_data['totals']['managed_hosts']

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
            logger.debug(f"Collecting {usage_type} usage data")
            data = collector(dgs=dgs)
            usage_data[usage_type] = {item['dt_id']: item['value'] for item in data}
        
        return usage_data

    async def _process_dg(self, dg: DG, usage_data: Dict) -> Dict:
        """
        Processes a single DG, calculating usage for all its entities.
        Skips entities that are already assigned to DIGIT or DIGIT C.
        """
        logger.info(f"Processing DG: {dg.name}")
        
        # Initialize DG report structure
        dg_report = {
            'information_systems': [],
            'unassigned_entities': {
                'hosts': [], 
                'applications': [], 
                'synthetics': [],
                'usage': {usage_type: 0.0 for usage_type in self.usage_types}
            },
            'totals': {
                'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                'entities': {'hosts': 0, 'applications': 0, 'synthetics': 0},
                'managed_hosts': 0
            }
        }

        # Process each Information System in the DG
        for is_ in dg.information_systems:
            logger.debug(f"Processing IS: {is_.name} in DG: {dg.name}")
            is_report = await self._process_information_system(is_, usage_data)
            dg_report['information_systems'].append({
                'name': is_.name,
                'data': is_report
            })
            
            # Update totals
            for usage_type in self.usage_types:
                dg_report['totals']['usage'][usage_type] += is_report['usage'][usage_type]
            for entity_type in ['hosts', 'applications', 'synthetics']:
                dg_report['totals']['entities'][entity_type] += len(is_report['entities'][entity_type])
            dg_report['totals']['managed_hosts'] += is_report['managed_hosts']

        # Process unassigned entities
        for entity_type in ['hosts', 'applications', 'synthetics']:
            entities = getattr(dg, entity_type)
            for entity in entities:
                if entity.dt_id not in self.assigned_entities[entity_type]:
                    processor = getattr(self, f'_process_{entity_type[:-1]}')
                    entity_data = processor(entity, usage_data)
                    if entity_data:
                        dg_report['unassigned_entities'][entity_type].append(entity_data)
                        self.assigned_entities[entity_type].add(entity.dt_id)
                        for usage_type, value in entity_data['usage'].items():
                            dg_report['unassigned_entities']['usage'][usage_type] += value
                            dg_report['totals']['usage'][usage_type] += value
                        dg_report['totals']['entities'][entity_type] += 1

        return dict(dg_report)

    async def _process_unassigned_dg(self, usage_data: Dict, dg_names: List[str]) -> Dict:
        """
        Processes entities that aren't assigned to any DG.
        Only includes entities that haven't been assigned to any other DG.
        """
        logger.info("Processing unassigned entities")
        
        # Initialize unassigned report structure
        unassigned_report = {
            'information_systems': [],
            'unassigned_entities': {
                'hosts': [],
                'applications': [],
                'synthetics': [],
                'usage': {usage_type: 0.0 for usage_type in self.usage_types}
            },
            'totals': {
                'usage': {usage_type: 0.0 for usage_type in self.usage_types},
                'entities': {'hosts': 0, 'applications': 0, 'synthetics': 0},
                'managed_hosts': 0
            }
        }

        # Query unassigned entities directly from Dynatrace
        unassigned_fullstack = query_unassigned_host_full_stack_usage()
        unassigned_infra = query_unassigned_host_infra_usage()
        unassigned_rum = query_unassigned_real_user_monitoring_usage()
        unassigned_rum_sr = query_unassigned_real_user_monitoring_with_sr_usage()
        unassigned_browser = query_unassigned_browser_monitor_usage()
        unassigned_http = query_unassigned_http_monitor_usage()
        unassigned_3rd_party = query_unassigned_3rd_party_monitor_usage()

        # Process unassigned hosts
        for host in unassigned_fullstack + unassigned_infra:
            if host['dt_id'] not in self.assigned_entities['hosts']:
                # Query host from database
                db_host = self.db.query(Host).filter(Host.dt_id == host['dt_id']).first()
                host_data = {
                    'id': db_host.id if db_host else None,
                    'name': db_host.name if db_host else 'Unknown',
                    'dt_id': host['dt_id'],
                    'usage': {
                        'fullstack': next((h['value'] for h in unassigned_fullstack if h['dt_id'] == host['dt_id']), 0.0),
                        'infra': next((h['value'] for h in unassigned_infra if h['dt_id'] == host['dt_id']), 0.0)
                    }
                }
                unassigned_report['unassigned_entities']['hosts'].append(host_data)
                self.assigned_entities['hosts'].add(host['dt_id'])
                for usage_type, value in host_data['usage'].items():
                    unassigned_report['unassigned_entities']['usage'][usage_type] += value
                    unassigned_report['totals']['usage'][usage_type] += value
                unassigned_report['totals']['entities']['hosts'] += 1

        # Process unassigned applications
        for app in unassigned_rum + unassigned_rum_sr:
            if app['dt_id'] not in self.assigned_entities['applications']:
                # Query application from database
                db_app = self.db.query(Application).filter(Application.dt_id == app['dt_id']).first()
                app_data = {
                    'id': db_app.id if db_app else None,
                    'name': db_app.name if db_app else 'Unknown',
                    'dt_id': app['dt_id'],
                    'usage': {
                        'rum': next((a['value'] for a in unassigned_rum if a['dt_id'] == app['dt_id']), 0.0),
                        'rum_with_sr': next((a['value'] for a in unassigned_rum_sr if a['dt_id'] == app['dt_id']), 0.0)
                    }
                }
                unassigned_report['unassigned_entities']['applications'].append(app_data)
                self.assigned_entities['applications'].add(app['dt_id'])
                for usage_type, value in app_data['usage'].items():
                    unassigned_report['unassigned_entities']['usage'][usage_type] += value
                    unassigned_report['totals']['usage'][usage_type] += value
                unassigned_report['totals']['entities']['applications'] += 1

        # Process unassigned synthetics
        for synthetic in unassigned_browser + unassigned_http + unassigned_3rd_party:
            if synthetic['dt_id'] not in self.assigned_entities['synthetics']:
                # Query synthetic from database
                db_synthetic = self.db.query(Synthetic).filter(Synthetic.dt_id == synthetic['dt_id']).first()
                synthetic_data = {
                    'id': db_synthetic.id if db_synthetic else None,
                    'name': db_synthetic.name if db_synthetic else 'Unknown',
                    'dt_id': synthetic['dt_id'],
                    'usage': {
                        'browser_monitor': next((s['value'] for s in unassigned_browser if s['dt_id'] == synthetic['dt_id']), 0.0),
                        'http_monitor': next((s['value'] for s in unassigned_http if s['dt_id'] == synthetic['dt_id']), 0.0),
                        '3rd_party_monitor': next((s['value'] for s in unassigned_3rd_party if s['dt_id'] == synthetic['dt_id']), 0.0)
                    }
                }
                unassigned_report['unassigned_entities']['synthetics'].append(synthetic_data)
                self.assigned_entities['synthetics'].add(synthetic['dt_id'])
                for usage_type, value in synthetic_data['usage'].items():
                    unassigned_report['unassigned_entities']['usage'][usage_type] += value
                    unassigned_report['totals']['usage'][usage_type] += value
                unassigned_report['totals']['entities']['synthetics'] += 1

        return unassigned_report

    async def _process_information_system(self, is_: IS, usage_data: Dict) -> Dict:
        """
        Processes a single Information System, calculating usage for all its entities.
        Only processes entities that haven't been assigned to other DGs.
        """
        logger.debug(f"Processing Information System: {is_.name}")
        
        # Initialize IS report structure
        is_report = {
            'id': is_.id,
            'entities': {'hosts': [], 'applications': [], 'synthetics': []},
            'usage': {usage_type: 0.0 for usage_type in self.usage_types},
            'managed_hosts': 0
        }

        # Process each entity type
        for entity_type in ['hosts', 'applications', 'synthetics']:
            entities = getattr(is_, entity_type)
            processor = getattr(self, f'_process_{entity_type[:-1]}')
            
            for entity in entities:
                if entity.dt_id not in self.assigned_entities[entity_type]:
                    entity_data = processor(entity, usage_data)
                    if entity_data:
                        is_report['entities'][entity_type].append(entity_data)
                        self.assigned_entities[entity_type].add(entity.dt_id)
                        for capability, usage in entity_data['usage'].items():
                            is_report['usage'][capability] += usage
                        if entity_type == 'hosts' and getattr(entity, 'managed', False):
                            is_report['managed_hosts'] += 1

        return dict(is_report)

    def _process_host(self, host: Host, usage_data: Dict) -> Dict:
        """
        Processes a single host entity.
        A host can only be either fullstack or infrastructure monitored, not both.
        """
        fullstack_usage = usage_data['fullstack'].get(host.dt_id, 0.0)
        infra_usage = usage_data['infra'].get(host.dt_id, 0.0)
        
        # Only include usage if it's greater than 0
        usage = {
            'fullstack': fullstack_usage if fullstack_usage > 0 else 0.0,
            'infra': infra_usage if infra_usage > 0 and fullstack_usage == 0 else 0.0
        }
        
        return {
            'id': host.id,
            'name': host.name,
            'dt_id': host.dt_id,
            'usage': usage
        }

    def _process_application(self, app: Application, usage_data: Dict) -> Dict:
        """
        Processes a single application entity.
        Handles RUM and Session Replay usage.
        """
        usage = {usage_type: usage_data[usage_type].get(app.dt_id, 0.0) 
                for usage_type in ['rum', 'rum_with_sr']}
        return {
            'id': app.id,
            'name': app.name,
            'dt_id': app.dt_id,
            'usage': usage
        }

    def _process_synthetic(self, synthetic: Synthetic, usage_data: Dict) -> Dict:
        """
        Processes a single synthetic monitor entity.
        Handles browser, HTTP, and third-party monitor usage.
        """
        usage = {usage_type: usage_data[usage_type].get(synthetic.dt_id, 0.0) 
                for usage_type in ['browser_monitor', 'http_monitor', '3rd_party_monitor']}
        return {
            'id': synthetic.id,
            'name': synthetic.name,
            'dt_id': synthetic.dt_id,
            'usage': usage
        }

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
        return usage_to_entity.get(usage_type)