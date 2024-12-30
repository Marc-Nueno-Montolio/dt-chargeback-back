import click
from app import models
from app.models import DG
from app.database import engine, get_db
from app.routes import topology
from sqlalchemy.orm import Session
from app.services import usage as usage_svc
import json
import asyncio

from app.dynatrace import query_host_infra_usage
from app.services.chargeback import ChargebackReport
# Create the database tables
models.Base.metadata.create_all(bind=engine)

@click.group()
def cli():
    """EC DPS Chargeback CLI"""
    click.echo("Welcome to the EC DPS Chargeback CLI")

@cli.command()
def test_dt_api():
    data = query_host_infra_usage(dg='OP')
    print(data)

@cli.command()
def retrieve_usage(dgs=['OP']):
    """Retrieve usage data for the given DG(s)"""
    results = dict()
    # First retrieve Fullstack
    data = usage_svc.retrieve_hosts_fullstack_usage(dgs=dgs)
    results['fullstack'] = data

    # Then retrieve Infra
    data = usage_svc.retrieve_hosts_infra_usage(dgs=dgs)
    results['infra'] = data

    # Then retrieve RUM
    data = usage_svc.retrieve_real_user_monitoring_usage(dgs=dgs)
    results['rum'] = data

    # Then retrieve RUM with SR
    data = usage_svc.retrieve_real_user_monitoring_with_sr_usage(dgs=dgs)
    results['rum_with_sr'] = data

    # Then retrieve Browser Monitor
    data = usage_svc.retrieve_browser_monitor_usage(dgs=dgs)
    results['browser_monitor'] = data

    # Then retrieve HTTP Monitor
    data = usage_svc.retrieve_http_monitor_usage(dgs=dgs)
    results['http_monitor'] = data

    # Then retrieve 3rd Party Monitor   
    data = usage_svc.retrieve_3rd_party_monitor_usage(dgs=dgs)
    results['3rd_party_monitor'] = data

    # save results to json file
    with open('usage.json', 'w') as f:
        json.dump(results, f)
    
@cli.command()
def list_dgs():
    db = next(get_db())
    dgs = db.query(DG).all()
    for dg in dgs:
        print(dg.name)

@cli.command()
@click.argument('dgs', nargs=-1)
def generate_chargeback(dgs):
    """Generate chargeback report for specified DGs"""
    if not dgs:
        click.echo("Please specify at least one DG")
        return

    db = next(get_db())
    report_generator = ChargebackReport(db)
    report = asyncio.run(report_generator.generate_report(list(dgs)))
    
    # Save report to file
    with open('chargeback_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    click.echo(f"Chargeback report generated for DGs: {', '.join(dgs)}")

if __name__ == "__main__":
    cli()
