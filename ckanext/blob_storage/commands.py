import click
import sys
from .cli import MigrateResourcesCommand

@click.group(name='blob-storage', short_help='Blob storage commands')
def blob_storage():
    """Commands for managing blob storage."""
    pass

@blob_storage.command('migrate')
@click.option('--from-bucket', is_flag=True, help='Migrate resources from a bucket')
@click.argument('bucket_url', required=False)
def migrate(from_bucket, bucket_url):
    """Migrate resources to blob storage.
    
    If --from-bucket is specified, resources will be migrated from the specified bucket URL.
    Otherwise, resources will be migrated from the CKAN upload directory.
    """
    # Create an instance of the existing command class
    command = MigrateResourcesCommand(None)
    
    # Set up the args as they would come from command line
    args = []
    if from_bucket:
        args.append('--from-bucket')
        if bucket_url:
            args.append(bucket_url)
    
    # Set the args and run the command
    command.args = args
    command.command()

def get_commands():
    return [blob_storage] 