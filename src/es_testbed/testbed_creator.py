"""Create a sample index on a cluster with multiple docs for PII redaction testing"""
# pylint: disable=unused-argument, redefined-builtin

import click
from es_client.defaults import OPTION_DEFAULTS
from es_client.helpers import config as esconfig
from es_client.helpers.logging import configure_logging
from es_testbed.helpers import es_api
from es_testbed.exceptions import TestbedException
from es_testbed.version import __version__

@click.command()
@click.option('--name', type=str, help='The index name', required=True)
@click.option('--start_num', type=int, help='Start numbers from this value', default=0)
@click.option('--count', type=int, help='Make this many documents', default=10)
@click.option('--match/--no-match', help='Make matchable documents or not', default=True)
@click.pass_context
def hot(ctx, name, start_num, count, match):
    """
    Add an index to the hot tier
    """
    client = ctx.obj['client']
    es_api.delete_index(client, name)
    es_api.fill_index(client, name, count, start_num, match=match)

@click.command()
@click.option('--name', type=str, help='The index name', required=True)
@click.option('--start_num', type=int, help='Start numbers from this value', default=0)
@click.option('--count', type=int, help='Make this many documents', default=10)
@click.option('--match/--no-match', help='Make matchable documents or not', default=True)
@click.option('--repo', type=str, help='The snapshot repository', required=True)
@click.option('--snap', type=str, help='The snapshot name', required=True)
@click.pass_context
def cold(ctx, name, start_num, count, match, repo, snap):
    """
    Add an index to the cold tier
    """
    client = ctx.obj['client']
    es_api.delete_index(client, name)
    es_api.delete_index(client, f'restored-{name}')
    es_api.delete_snapshot(client, repo, snap)
    es_api.fill_index(client, name, count, start_num, match=match)
    es_api.do_snap(client, repo, snap, name)
    es_api.fix_aliases(client, name, f'restored-{name}')

@click.command()
@click.option('--name', type=str, help='The index name', required=True)
@click.option('--start_num', type=int, help='Start numbers from this value', default=0)
@click.option('--count', type=int, help='Make this many documents', default=10)
@click.option('--match/--no-match', help='Make matchable documents or not', default=True)
@click.option('--repo', type=str, help='The snapshot repository', required=True)
@click.option('--snap', type=str, help='The snapshot name', required=True)
@click.pass_context
def frozen(ctx, name, start_num, count, match, repo, snap):
    """
    Add an index to the frozen tier
    """
    client = ctx.obj['client']
    es_api.delete_index(client, name)
    es_api.delete_index(client, f'partial-{name}')
    es_api.delete_snapshot(client, repo, snap)
    es_api.fill_index(client, name, count, start_num, match=match)
    es_api.do_snap(client, repo, snap, name, tier='frozen')
    es_api.fix_aliases(client, name, f'partial-{name}')

@click.command()
@click.option('--name', type=str, help='The index name', required=True)
@click.pass_context
def show_index(ctx, name):
    """
    Show index settings
    """
    client = ctx.obj['client']
    click.echo(client.indices.get_settings(index=name))

@click.command()
@click.option('--repo', type=str, help='The snapshot repository', required=True)
@click.option('--snap', type=str, help='The snapshot name', required=True)
@click.pass_context
def show_snapshot(ctx, repo, snap):
    """
    Show snapshot contents
    """
    client = ctx.obj['client']
    click.echo(client.snapshot.get(repository=repo, snapshot=snap))

@click.command()
@click.option('--name', type=str, help='The index name', required=True)
@click.pass_context
def delete_index(ctx, name):
    """
    Purge index
    """
    client = ctx.obj['client']
    es_api.delete_index(client, name)

@click.command()
@click.option('--repo', type=str, help='The snapshot repository', required=True)
@click.option('--snap', type=str, help='The snapshot name', required=True)
@click.pass_context
def delete_snapshot(ctx, repo, snap):
    """
    Purge snapshot
    """
    client = ctx.obj['client']
    es_api.delete_snapshot(client, repo, snap)

# pylint: disable=unused-argument, redefined-builtin, too-many-arguments, too-many-locals, line-too-long
@click.group(context_settings=esconfig.context_settings())
@esconfig.options_from_dict(OPTION_DEFAULTS)
@click.version_option(__version__, '-v', '--version', prog_name="testbed_creator")
@click.pass_context
def cli(
    ctx, config, hosts, cloud_id, api_token, id, api_key, username, password, bearer_auth,
    opaque_id, request_timeout, http_compress, verify_certs, ca_certs, client_cert, client_key,
    ssl_assert_hostname, ssl_assert_fingerprint, ssl_version, master_only, skip_version_test,
    loglevel, logfile, logformat, blacklist
):
    """
    Testbed Creator

    Create indices and snapshots for testing elastic-pii-redacter
    """
    esconfig.get_config(ctx)
    configure_logging(ctx)
    esconfig.generate_configdict(ctx)
    try:
        ctx.obj['client'] = esconfig.get_client(configdict=ctx.obj['configdict'])
    except Exception as exc:
        raise TestbedException('Unable to establish connection to Elasticsearch!') from exc

# Add the subcommands
cli.add_command(hot)
cli.add_command(cold)
cli.add_command(frozen)
cli.add_command(show_index)
cli.add_command(show_snapshot)
cli.add_command(delete_index)
cli.add_command(delete_snapshot)

if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    cli()
