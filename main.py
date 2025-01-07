import click
import json
import csv
import asyncio
from database import engine, get_db
from models import DG, Host, Application, Synthetic
from chargeback import ChargebackReport
from settings import SQLALCHEMY_DATABASE_URL
from topology import refresh_dgs_task, refresh_hosts_task, refresh_applications_task, refresh_synthetics_task

# Create the database tables
from models import Base
Base.metadata.create_all(bind=engine)
from export import export_data

@click.group()
def cli():
    """ EC DPS Chargeback CLI 1.0 """
    pass


# Entities Group
@cli.group()
def get():
    """List DT entities in local DB"""
    pass

@get.command()
@click.option('--dg', type=str)
@click.option('--output', type=click.Path(), help="Output file path")
@click.option('--format', type=click.Choice(['json', 'csv']), default='json', help="Output format")
def dgs(dg, output, format):
    """List DGs"""
    db = next(get_db())
    query = db.query(DG)
    if dg:
        query = query.filter(DG.name == dg)
    results = query.all()
    data = [{"id": result.id, "name": result.name, "last_updated": result.last_updated} for result in results]
    
    if dg and results:
        dg_details = results[0]
        click.echo(f"DG Details: ID={dg_details.id}, Name={dg_details.name}, Last Updated={dg_details.last_updated}")
    else:
        for result in results:
            click.echo(f"DG: {result.name}")
    
    if output:
        export_data(data, output, format)

@get.command()
@click.option('--dg', type=str)
@click.option('--output', type=click.Path(), help="Output file path")
@click.option('--format', type=click.Choice(['json', 'csv']), default='json', help="Output format")
def hosts(dg, output, format):
    """List hosts"""
    db = next(get_db())
    query = db.query(Host)
    if dg:
        query = query.join(Host.dgs).filter(DG.name == dg)
    results = query.all()
    data = [{"id": result.id, "name": result.name, "dt_id": result.dt_id, "last_updated": result.last_updated} for result in results]
    
    for result in results:
        click.echo(f"Host: {result.name}")
    
    if output:
        export_data(data, output, format)

@get.command()
@click.option('--dg', type=str)
@click.option('--output', type=click.Path(), help="Output file path")
@click.option('--format', type=click.Choice(['json', 'csv']), default='json', help="Output format")
def applications(dg, output, format):
    """List applications"""
    db = next(get_db())
    query = db.query(Application)
    if dg:
        query = query.join(Application.dgs).filter(DG.name == dg)
    results = query.all()
    data = [{"id": result.id, "name": result.name, "dt_id": result.dt_id, "last_updated": result.last_updated} for result in results]
    
    for result in results:
        click.echo(f"Application: {result.name}")
    
    if output:
        export_data(data, output, format)

@get.command()
@click.option('--dg', type=str)
@click.option('--output', type=click.Path(), help="Output file path")
@click.option('--format', type=click.Choice(['json', 'csv']), default='json', help="Output format")
def synthetics(dg, output, format):
    """List synthetics"""
    db = next(get_db())
    query = db.query(Synthetic)
    if dg:
        query = query.join(Synthetic.dgs).filter(DG.name == dg)
    results = query.all()
    data = [{"id": result.id, "name": result.name, "dt_id": result.dt_id, "last_updated": result.last_updated} for result in results]
    
    for result in results:
        click.echo(f"Synthetic: {result.name}")
    
    if output:
        export_data(data, output, format)

# DB Operations Group
@cli.group()
def db():
    """Perform local DB operations"""
    pass

@db.command()
def init_db():
    """Initialize the database"""
    from models import Base
    Base.metadata.create_all(bind=engine)
    click.echo("Database initialized")

@db.command()
@click.option('--refresh', multiple=True, default=['dgs', 'hosts', 'synthetics', 'applications'])
def refresh_topology(refresh):
    """Refresh the topology for specified components"""
    tasks = {
        'dgs': refresh_dgs_task,
        'hosts': refresh_hosts_task,
        'applications': refresh_applications_task,
        'synthetics': refresh_synthetics_task
    }
    

    for component in refresh:
        if component in tasks:
            click.echo(f"Refreshing {component}...")
            tasks[component]()
            
            click.echo(f"{component.capitalize()} refresh completed")

# Chargeback Group
@cli.group()
def chargeback():
    """Chargeback Tools"""
    pass


@chargeback.command()
@click.option('--refresh-topology', is_flag=True, help="Refresh topology database before generating the report (will take more time) (default: False)", default=False)
@click.option('--dg', multiple=True, help="List of DG names to generate the report for, if empty all DGS will be processed")
@click.option('--from_date', default="-30d", help="Start date for the report (default: -30d) (Not implemented yet)")
@click.option('--to_date', default="now", help="End date for the report (default: now) (Not Implemented yet)")
@click.option('--process-unassigned', is_flag=True, default=False, help="Include entities not assigned to any DG (default: False)")
@click.option('--include-non-charged-entities-in-dg', is_flag=True, default=False, help="Include non-charged entities in DG (default: False)")
@click.option('--output', default="output.xlsx", help="Output file name (default: output.xlsx)")
def generate(refresh_topology, dg, from_date, to_date, process_unassigned, include_non_charged_entities_in_dg, output):
    """Generate chargeback report"""
    if refresh_topology:
        click.echo("Refreshing topology database...")
        refresh_dgs_task()
        refresh_hosts_task()
        refresh_applications_task()
        refresh_synthetics_task()
        click.echo("Topology database refreshed")
    dgs = list(dg) # Convert multiple option to list
    db = next(get_db())
    if not dgs:
        dgs = [dg.name for dg in db.query(DG).all()]
    
    report_generator = ChargebackReport(db,  process_unassigned=process_unassigned)
    report = asyncio.run(report_generator.generate_report(
        dg_names=dgs,
    ))

    # Export report to the specified format
    if '.json' in output:
        with open(output, 'w') as f:
            json.dump(report, f, indent=2)

    elif '.xlsx' in output:
        from export import ChargebackExcelExporter
        exporter = ChargebackExcelExporter()
        exporter.export_to_excel(report, output)
    
    click.echo(f"Chargeback report generated and saved to {output}")

@chargeback.command()
@click.argument('report_path', type=click.Path(exists=True))
@click.argument('output_path', type=click.Path())
def convert_to_excel(report_path, output_path):
    """
    Generate Excel report from chargeback JSON report
    
    Args:
        report_path: Path to the JSON chargeback report
        output_path: Path where the Excel file should be saved
    """
    try:
        # Load the JSON report
        with open(report_path, 'r') as f:
            report = json.load(f)
            
        # Create Excel export
        from export import ChargebackExcelExporter
        exporter = ChargebackExcelExporter()
        exporter.export_to_excel(report, output_path)
        
        click.echo(f"Excel report successfully generated at: {output_path}")
    except Exception as e:
        click.echo(f"Error generating Excel report: {str(e)}", err=True)

if __name__ == "__main__":
    cli()