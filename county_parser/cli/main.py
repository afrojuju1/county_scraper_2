"""Main CLI interface for county data parsing."""

import click
from pathlib import Path
from rich.console import Console
from rich.table import Table
from datetime import datetime

from ..models import Config
from ..parsers import RealAccountsParser, OwnersParser, HarrisCountyNormalizer
from ..parsers.travis_parser import TravisCountyNormalizer
from ..parsers.dallas_parser import DallasCountyNormalizer
from ..utils.data_validator import DataQualityValidator
from ..services import MongoDBService


@click.group()
@click.option('--config-file', type=click.Path(), help='Path to configuration file')
@click.pass_context
def cli(ctx, config_file):
    """County Property Data Parser - Parse and clean large county property files."""
    
    # Load configuration
    if config_file:
        # TODO: Load from custom config file
        config = Config()
    else:
        config = Config.from_env()
    
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['console'] = Console()


@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.option('--format', 'output_format', type=click.Choice(['csv', 'parquet', 'json']), 
              default='parquet', help='Output format')
@click.option('--chunk-size', type=int, default=10000, help='Chunk size for processing')
@click.option('--use-chunks', is_flag=True, help='Process file in chunks')
@click.pass_context
def parse_real_accounts(ctx, input_file, output, output_format, chunk_size, use_chunks):
    """Parse real account data file."""
    
    config = ctx.obj['config']
    console = ctx.obj['console']
    
    # Override config with CLI options
    config.parsing.chunk_size = chunk_size
    config.parsing.output_format = output_format
    config.real_accounts_file = Path(input_file).name
    config.data_dir = Path(input_file).parent
    
    parser = RealAccountsParser(config)
    
    # Set default output path if not provided
    if not output:
        input_path = Path(input_file)
        output = config.output_dir / f"parsed_{input_path.stem}.{output_format}"
    else:
        output = Path(output)
    
    try:
        if use_chunks:
            console.print("[yellow]Processing in chunks...[/yellow]")
            chunks = list(parser.parse_in_chunks(output))
            console.print(f"[green]Processed {len(chunks)} chunks successfully![/green]")
        else:
            df = parser.parse_file(output)
            
            # Display summary
            stats = parser.get_summary_stats(df)
            console.print("\n[bold]Summary Statistics:[/bold]")
            console.print(f"Total records: {stats['total_records']:,}")
            console.print(f"Unique accounts: {stats['unique_accounts']:,}")
            console.print(f"Average appraised value: ${stats['value_stats']['avg_total_appraised_value']:,.2f}")
            
            console.print(f"\n[green]Successfully parsed and saved to {output}[/green]")
            
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.option('--format', 'output_format', type=click.Choice(['csv', 'parquet', 'json']), 
              default='parquet', help='Output format')
@click.option('--chunk-size', type=int, default=10000, help='Chunk size for processing')
@click.option('--use-chunks', is_flag=True, help='Process file in chunks')
@click.pass_context
def parse_owners(ctx, input_file, output, output_format, chunk_size, use_chunks):
    """Parse owners data file."""
    
    config = ctx.obj['config']
    console = ctx.obj['console']
    
    # Override config with CLI options
    config.parsing.chunk_size = chunk_size
    config.parsing.output_format = output_format
    config.owners_file = Path(input_file).name
    config.data_dir = Path(input_file).parent
    
    parser = OwnersParser(config)
    
    # Set default output path if not provided
    if not output:
        input_path = Path(input_file)
        output = config.output_dir / f"parsed_{input_path.stem}.{output_format}"
    else:
        output = Path(output)
    
    try:
        if use_chunks:
            console.print("[yellow]Processing in chunks...[/yellow]")
            chunks = list(parser.parse_in_chunks(output))
            console.print(f"[green]Processed {len(chunks)} chunks successfully![/green]")
        else:
            df = parser.parse_file(output)
            
            # Display summary
            stats = parser.get_summary_stats(df)
            console.print("\n[bold]Summary Statistics:[/bold]")
            console.print(f"Total records: {stats['total_records']:,}")
            console.print(f"Unique accounts: {stats['unique_accounts']:,}")
            console.print(f"Unique owners: {stats['unique_owners']:,}")
            console.print(f"Properties with multiple owners: {stats['multiple_owners']:,}")
            
            console.print(f"\n[green]Successfully parsed and saved to {output}[/green]")
            
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--real-accounts', type=click.Path(exists=True), required=True,
              help='Path to parsed real accounts file')
@click.option('--owners', type=click.Path(exists=True), required=True,
              help='Path to parsed owners file')
@click.option('--output', '-o', type=click.Path(), required=True, help='Output file path')
@click.option('--format', 'output_format', type=click.Choice(['csv', 'parquet', 'json']), 
              default='parquet', help='Output format')
@click.pass_context
def join_data(ctx, real_accounts, owners, output, output_format):
    """Join real accounts and owners data."""
    
    console = ctx.obj['console']
    
    try:
        import polars as pl
        
        console.print("[yellow]Loading data files...[/yellow]")
        
        # Load the files based on their format
        if Path(real_accounts).suffix == '.parquet':
            accounts_df = pl.read_parquet(real_accounts)
        else:
            accounts_df = pl.read_csv(real_accounts)
            
        if Path(owners).suffix == '.parquet':
            owners_df = pl.read_parquet(owners)
        else:
            owners_df = pl.read_csv(owners)
        
        console.print(f"Loaded {len(accounts_df):,} account records")
        console.print(f"Loaded {len(owners_df):,} owner records")
        
        # Join the data
        console.print("[yellow]Joining data...[/yellow]")
        joined_df = accounts_df.join(owners_df, on="acct", how="left")
        
        console.print(f"Joined dataset has {len(joined_df):,} records")
        
        # Save the result
        output_path = Path(output)
        if output_format == 'parquet':
            joined_df.write_parquet(output_path.with_suffix('.parquet'))
        elif output_format == 'csv':
            joined_df.write_csv(output_path.with_suffix('.csv'))
        elif output_format == 'json':
            joined_df.write_json(output_path.with_suffix('.json'))
        
        console.print(f"[green]Successfully joined and saved to {output_path}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--format', 'output_format', type=click.Choice(['json', 'csv', 'mongodb']),
              default='mongodb', help='Output format: json/csv file or direct mongodb save')
@click.option('--output', '-o', type=click.Path(), help='Output file path (not needed for mongodb)')
@click.option('--include-related/--basic-only', default=True,
              help='Include related data (owners, deeds, permits) or basic property info only')
@click.option('--sample-size', type=int, help='Process only first N records for testing')
@click.option('--batch-id', help='Custom batch ID for MongoDB storage')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def normalize_all(ctx, output_format, output, include_related, sample_size, batch_id, mongo_uri, database):
    """Normalize and combine all county data files into a single dataset."""
    
    config = ctx.obj['config']
    console = ctx.obj['console']
    
    if output_format == 'mongodb':
        # Direct MongoDB save
        try:
            console.print(f"[yellow]🏘️  Starting normalization and MongoDB save...[/yellow]")
            if sample_size:
                console.print(f"[yellow]Processing sample of {sample_size:,} records[/yellow]")
            else:
                console.print(f"[yellow]Processing ALL county data files...[/yellow]")
                
            normalizer = HarrisCountyNormalizer(config)
            
            # Get normalized data directly
            console.print("[blue]📊 Loading and normalizing data...[/blue]")
            
            # Load all data files
            real_accounts_df = normalizer._load_real_accounts(sample_size=sample_size)
            owners_df = normalizer._load_owners()
            deeds_df = normalizer._load_deeds()  
            permits_df = normalizer._load_permits()
            tieback_df = normalizer._load_parcel_tieback()
            neighborhood_df = normalizer._load_neighborhood_codes()
            mineral_df = normalizer._load_mineral_rights()
            
            # Create normalized data directly
            normalized_data = normalizer._create_json_normalized_data(
                real_accounts_df, owners_df, deeds_df, permits_df, 
                tieback_df, neighborhood_df, mineral_df
            )
            
            # Save directly to MongoDB
            from ..services import MongoDBService
            mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
            if not mongodb.connect():
                raise click.ClickException("Failed to connect to MongoDB")
            
            try:
                mongo_result = mongodb.save_properties(
                    normalized_data, 
                    batch_id=batch_id,
                    source_files=['real_acct.txt', 'owners.txt', 'deeds.txt', 'permits.txt', 'parcel_tieback.txt']
                )
                
                console.print(f"\n[bold green]🎉 Successfully saved to MongoDB![/bold green]")
                console.print(f"📊 Batch ID: {mongo_result['batch_id']}")
                console.print(f"💾 Properties: {mongo_result['saved_count']:,}")
                console.print(f"📅 Timestamp: {mongo_result['timestamp']}")
                
                # Show collection stats
                stats = mongodb.get_collection_stats()
                console.print(f"\n[blue]📈 Database Status:[/blue]")
                console.print(f"   Total properties: {stats['properties_count']:,}")
                console.print(f"   Processing logs: {stats['logs_count']:,}")
                
                console.print("\n[bold]Included Data:[/bold]")
                console.print(f"✅ Owners: {len(owners_df):,} records")
                console.print(f"✅ Deeds: {len(deeds_df):,} records") 
                console.print(f"✅ Permits: {len(permits_df):,} records")
                console.print(f"✅ Parcel Relationships: {len(tieback_df):,} records")
                console.print(f"✅ Mineral Rights: {len(mineral_df):,} records")
                
            finally:
                mongodb.disconnect()
                
        except Exception as e:
            console.print(f"[red]Error during MongoDB save: {e}[/red]")
            raise click.ClickException(str(e))
    else:
        # Traditional file save
        if not output:
            raise click.ClickException("Output path required for JSON/CSV formats")
            
        config.parsing.output_format = output_format
        normalizer = HarrisCountyNormalizer(config)
        output_path = Path(output)
        
        try:
            console.print(f"[yellow]Starting normalization to {output_format.upper()} format...[/yellow]")
            if sample_size:
                console.print(f"[yellow]Processing sample of {sample_size:,} records[/yellow]")
                
            results = normalizer.normalize_all_files(
                output_path=output_path,
                format=output_format,
                include_all_fields=include_related,
                sample_size=sample_size,
                use_chunking=True
            )
            
            console.print("\n[bold green]🎉 Normalization Complete![/bold green]")
            console.print(f"Total properties processed: {results['total_properties']:,}")
            console.print(f"Output format: {results['format'].upper()}")
            console.print(f"Saved to: {results['output_path']}")
            
            console.print("\n[bold]Included Data:[/bold]")
            for data_type, included in results['included_data'].items():
                status = "✅" if included else "❌"
                console.print(f"{status} {data_type.replace('_', ' ').title()}")
                
        except Exception as e:
            console.print(f"[red]Error during normalization: {e}[/red]")
            raise click.ClickException(str(e))


@cli.command()
@click.option('--check-integrity', is_flag=True, help='Run comprehensive data integrity checks')
@click.pass_context  
def diagnose(ctx, check_integrity):
    """Diagnose data quality issues and parsing problems."""
    
    config = ctx.obj['config']
    console = ctx.obj['console']
    
    console.print("[bold]🔍 Data Quality Diagnosis[/bold]")
    
    validator = DataQualityValidator()
    validation_results = []
    
    # Check each file for structural issues
    files_to_check = [
        ("real_acct.txt", config.real_accounts_file),
        ("owners.txt", config.owners_file), 
        ("deeds.txt", config.deeds_file),
        ("permits.txt", config.permits_file),
        ("parcel_tieback.txt", config.parcel_tieback_file)
    ]
    
    for display_name, filename in files_to_check:
        file_path = config.get_file_path(filename)
        
        if file_path.exists():
            console.print(f"\n🔍 Analyzing {display_name}...")
            
            # Check for embedded newlines and structural issues
            structure_issues = validator.detect_embedded_newlines(file_path)
            
            # Try to load a small sample using the correct specialized parsers
            try:
                normalizer = HarrisCountyNormalizer(config)
                
                # Use the appropriate specialized loader for each file type
                if display_name == "real_acct.txt":
                    sample_df = normalizer._load_real_accounts(sample_size=1000)
                elif display_name == "owners.txt":
                    sample_df = normalizer._load_owners()
                elif display_name == "deeds.txt":
                    sample_df = normalizer._load_deeds()  
                elif display_name == "permits.txt":
                    sample_df = normalizer._load_permits()
                elif display_name == "parcel_tieback.txt":
                    sample_df = normalizer._load_parcel_tieback()
                else:
                    # Fallback to robust loading for other files
                    sample_df = normalizer._robust_csv_load(file_path, display_name, sample_size=1000)
                
                # Get expected column count from header
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    header = f.readline().strip()
                    expected_cols = len(header.split('\t'))
                    
                # Limit sample size for validation if we got full file
                if len(sample_df) > 1000:
                    sample_df = sample_df.head(1000)
                    
                # Validate row integrity
                integrity_results = validator.validate_row_integrity(sample_df, expected_cols, display_name)
                
                # Combine results
                combined_results = {
                    **structure_issues,
                    **integrity_results,
                    "parsing_successful": True
                }
                validation_results.append(combined_results)
                
                # Report issues
                if structure_issues["issues_found"] > 0:
                    console.print(f"⚠️  Found {structure_issues['issues_found']} structural issues")
                elif integrity_results["data_integrity_score"] < 0.95:
                    console.print(f"⚠️  Data integrity: {integrity_results['data_integrity_score']*100:.1f}%")
                else:
                    console.print("✅ Good data quality detected")
                    
            except Exception as e:
                console.print(f"❌ Failed to load sample: {str(e)[:50]}...")
                validation_results.append({
                    **structure_issues,
                    "total_rows": 0,
                    "data_integrity_score": 0.0,
                    "parsing_successful": False
                })
        else:
            console.print(f"❌ {display_name} not found at {file_path}")
    
    # Generate quality report
    if validation_results:
        validator.generate_quality_report(validation_results)

@cli.command()
@click.option('--input-file', '-i', type=click.Path(exists=True), required=True,
              help='Path to the JSON file containing normalized county data')
@click.option('--batch-id', help='Custom batch ID for this import (optional)')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.option('--dry-run', is_flag=True, help='Show what would be imported without actually saving')
@click.pass_context
def save_to_mongodb(ctx, input_file, batch_id, mongo_uri, database, dry_run):
    """Save normalized county data to MongoDB."""
    console = ctx.obj['console']
    
    try:
        # Load the data
        console.print(f"[yellow]📂 Loading data from {input_file}...[/yellow]")
        
        import json
        with open(input_file, 'r') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            raise click.ClickException("Input file must contain a JSON array of property records")
        
        console.print(f"[green]✅ Loaded {len(data):,} property records[/green]")
        
        if dry_run:
            console.print("[yellow]🔍 DRY RUN MODE - No data will be saved[/yellow]")
            
            # Show sample structure
            if data:
                sample = data[0]
                console.print(f"\n📋 Sample property structure:")
                console.print(f"   Account ID: {sample.get('account_id', 'N/A')}")
                console.print(f"   Fields: {len(sample)} top-level fields")
                
                if 'owners' in sample:
                    console.print(f"   Owners: {len(sample['owners'])} records")
                if 'permits' in sample:
                    console.print(f"   Permits: {len(sample['permits'])} records")
                if 'deeds' in sample:
                    console.print(f"   Deeds: {len(sample['deeds'])} records")
            
            console.print(f"\n[green]✅ Would save {len(data):,} properties to MongoDB[/green]")
            return
        
        # Initialize MongoDB service
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            # Determine source files from metadata if available
            source_files = []
            if data and 'metadata' in data[0]:
                source_files = data[0]['metadata'].get('source_files', [])
            
            # Save to MongoDB
            result = mongodb.save_properties(
                data, 
                batch_id=batch_id,
                source_files=source_files or ['normalized_data.json']
            )
            
            console.print(f"\n[bold green]🎉 Successfully saved to MongoDB![/bold green]")
            console.print(f"📊 Batch ID: {result['batch_id']}")
            console.print(f"💾 Saved: {result['saved_count']:,} properties")
            console.print(f"📅 Timestamp: {result['timestamp']}")
            
            # Show collection stats
            stats = mongodb.get_collection_stats()
            console.print(f"\n[blue]📈 Collection Statistics:[/blue]")
            console.print(f"   Total properties in database: {stats['properties_count']:,}")
            console.print(f"   Processing logs: {stats['logs_count']:,}")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]❌ Error saving to MongoDB: {e}[/red]")
        raise click.ClickException(str(e))

@cli.command()
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.option('--limit', default=10, help='Number of recent properties to show')
@click.pass_context  
def mongodb_status(ctx, mongo_uri, database, limit):
    """Show MongoDB collection status and recent data."""
    console = ctx.obj['console']
    
    try:
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            stats = mongodb.get_collection_stats()
            
            console.print("[bold]🗄️  MongoDB Status[/bold]")
            console.print(f"Database: {stats['database_name']}")
            console.print(f"Properties: {stats['properties_count']:,}")
            console.print(f"Processing logs: {stats['logs_count']:,}")
            
            if stats['latest_batch']:
                latest = stats['latest_batch']
                console.print(f"\n[blue]📊 Latest Batch:[/blue]")
                console.print(f"   Batch ID: {latest['batch_id']}")
                console.print(f"   Status: {latest['status']}")
                console.print(f"   Properties: {latest.get('processed_properties', 0):,}")
                console.print(f"   Timestamp: {latest['timestamp']}")
            
            if stats['sample_structure']:
                console.print(f"\n[blue]📋 Property Document Structure:[/blue]")
                for field in stats['sample_structure'][:10]:
                    if field != '_id':
                        console.print(f"   • {field}")
                if len(stats['sample_structure']) > 10:
                    console.print(f"   • ... and {len(stats['sample_structure']) - 10} more fields")
            
            # Show recent properties
            if stats['properties_count'] > 0:
                console.print(f"\n[blue]🏠 Recent Properties (last {limit}):[/blue]")
                recent = mongodb.query_properties({}, limit=limit)
                
                for prop in recent[:5]:  # Show first 5
                    account_id = prop.get('account_id', 'N/A')
                    address = prop.get('property_address', {}).get('full_address', 'N/A')
                    value = prop.get('valuation', {}).get('total_market_value', 'N/A')
                    console.print(f"   • {account_id}: {address} (${value})")
        
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]❌ Error checking MongoDB status: {e}[/red]")
        raise click.ClickException(str(e))

@cli.command()
@click.option('--output', '-o', type=click.Path(), required=True, 
              help='Output path for the backup file')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def backup_mongodb(ctx, output, mongo_uri, database):
    """Create a backup of MongoDB data to JSON file."""
    console = ctx.obj['console']
    
    try:
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            from pathlib import Path
            backup_path = Path(output)
            
            # Create backup
            mongodb.create_backup(backup_path)
            
            # Show file info
            file_size = backup_path.stat().st_size / (1024 * 1024)
            console.print(f"[green]📦 Backup created successfully![/green]")
            console.print(f"File: {backup_path}")
            console.print(f"Size: {file_size:.1f} MB")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]❌ Error creating backup: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def clean_mongodb(ctx, mongo_uri, database):
    """Clean/reset MongoDB collections (properties and processing_logs)."""
    console = ctx.obj['console']
    
    try:
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            # Drop collections
            mongodb.properties_collection.drop()
            mongodb.logs_collection.drop()
            
            # Recreate with indexes
            mongodb.database.create_collection('properties')
            mongodb.database.create_collection('processing_logs')
            
            # Recreate indexes
            mongodb.properties_collection.create_index([("account_id", 1)], unique=True)
            mongodb.logs_collection.create_index([("timestamp", -1)])
            mongodb.logs_collection.create_index([("batch_id", 1)], unique=True)
            
            console.print("[bold green]✅ Successfully cleaned MongoDB collections![/bold green]")
            console.print("   - Properties collection: reset")
            console.print("   - Processing logs collection: reset")
            console.print("   - Indexes: recreated")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error cleaning MongoDB: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.pass_context
def travis_diagnose(ctx):
    """Diagnose Travis County data files and structure."""
    console = ctx.obj['console']
    
    try:
        # Create Travis County config
        from ..models.config import Config
        travis_config = Config()
        
        console.print("[bold blue]🏛️ Travis County Data Diagnostics[/bold blue]")
        console.print(f"Data directory: {travis_config.data_dir}")
        
        # Initialize Travis normalizer
        normalizer = TravisCountyNormalizer(travis_config)
        
        # Run diagnostics
        results = normalizer.diagnose_files()
        
        # Display results
        table = Table(title="Travis County File Analysis")
        table.add_column("File Type", style="bold")
        table.add_column("Status")
        table.add_column("Size (MB)")
        table.add_column("Records")
        table.add_column("Format")
        
        for file_type, info in results.items():
            status = "✅" if info['status'] == 'available' else "❌"
            size_mb = str(info.get('size_mb', 'N/A'))
            records = f"{info.get('line_count', 0):,}" if info.get('line_count') else 'N/A'
            format_type = info.get('format', 'unknown')
            
            table.add_row(
                file_type.replace('_', ' ').title(),
                f"{status} {info['status']}",
                size_mb,
                records,
                format_type
            )
        
        console.print(table)
        
        # Show sample data for main files
        console.print("\n[bold]Sample Records:[/bold]")
        
        main_files = ['properties', 'property_entities', 'improvements']
        for file_type in main_files:
            if file_type in results and results[file_type]['status'] == 'available':
                info = results[file_type]
                console.print(f"\n[bold cyan]{file_type.replace('_', ' ').title()}:[/bold cyan]")
                console.print(f"Line Length: {info.get('line_length', 'N/A')} chars")
                
                if 'sample_lines' in info and info['sample_lines']:
                    console.print("Sample:")
                    for i, line in enumerate(info['sample_lines'][:2]):
                        console.print(f"  {i+1}: {line[:100]}{'...' if len(line) > 100 else ''}")
        
    except Exception as e:
        console.print(f"[red]Error during Travis diagnostics: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--file-type', type=click.Choice([
    'properties', 'property_entities', 'improvements', 'improvement_attributes',
    'land_details', 'improvement_info', 'agents', 'subdivisions'
]), default='properties', help='File type to analyze in detail')
@click.pass_context
def travis_analyze(ctx, file_type):
    """Analyze a specific Travis County file in detail."""
    console = ctx.obj['console']
    
    try:
        from ..models.config import Config
        travis_config = Config()
        normalizer = TravisCountyNormalizer(travis_config)
        
        console.print(f"[bold blue]🔍 Analyzing Travis County {file_type.replace('_', ' ').title()}[/bold blue]")
        
        info = normalizer.get_file_info(file_type)
        
        if 'error' in info:
            console.print(f"[red]Error: {info['error']}[/red]")
            return
        
        # Display detailed info
        console.print(f"File: {info['path']}")
        console.print(f"Size: {info['size_mb']} MB")
        
        if 'line_lengths' in info:
            lengths = info['line_lengths']
            console.print(f"Line lengths: min={lengths['min']}, max={lengths['max']}, avg={lengths['avg']}")
        
        if 'sample_records' in info:
            console.print("\n[bold]Sample Records (first 3):[/bold]")
            for i, record in enumerate(info['sample_records']):
                console.print(f"\n[cyan]Record {i+1}:[/cyan]")
                console.print(f"Length: {len(record)} chars")
                console.print(f"Content: {record}")
        
        console.print(f"\n[yellow]Analysis: {info.get('analysis', 'No analysis available')}[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error analyzing {file_type}: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', type=int, default=100, help='Number of records to process')
@click.option('--output-file', '-o', type=click.Path(), help='Output JSON file path (optional)')
@click.pass_context
def travis_normalize_sample(ctx, sample_size, output_file):
    """Load and normalize a sample of Travis County data."""
    console = ctx.obj['console']
    
    try:
        from ..models.config import Config
        travis_config = Config()
        normalizer = TravisCountyNormalizer(travis_config)
        
        console.print(f"[bold blue]🏛️ Travis County Sample Normalization[/bold blue]")
        console.print(f"Sample size: {sample_size:,} properties")
        
        # Load and normalize sample
        normalized_records = normalizer.load_and_normalize_sample(sample_size)
        
        if not normalized_records:
            console.print("[red]No records were normalized[/red]")
            return
        
        # Show sample record structure
        sample_record = normalized_records[0]
        console.print("\n[bold cyan]Sample Record Structure:[/bold cyan]")
        
        def print_dict_structure(d, indent=0):
            for key, value in d.items():
                if isinstance(value, dict):
                    console.print("  " * indent + f"• {key}:")
                    print_dict_structure(value, indent + 1)
                elif isinstance(value, list):
                    console.print("  " * indent + f"• {key}: [{len(value)} items]")
                    if value and isinstance(value[0], dict):
                        console.print("  " * (indent + 1) + f"  Sample item keys: {list(value[0].keys())}")
                else:
                    console.print("  " * indent + f"• {key}: {type(value).__name__}")
        
        print_dict_structure(sample_record)
        
        # Save to file if requested
        if output_file:
            output_path = Path(output_file)
            normalizer.save_sample_output(normalized_records, output_path)
        
        # Compare with Harris model
        comparison = normalizer.compare_with_harris_model(normalized_records)
        
        console.print("\n[bold green]📊 Model Comparison Analysis:[/bold green]")
        
        if comparison['common_fields']:
            console.print(f"✅ Common fields: {len(comparison['common_fields'])}")
            console.print(f"   {', '.join(comparison['common_fields'])}")
        
        if comparison['travis_unique_fields']:
            console.print(f"\n🆕 Travis-specific fields: {len(comparison['travis_unique_fields'])}")
            console.print(f"   {', '.join(comparison['travis_unique_fields'])}")
        
        if comparison['harris_missing_fields']:
            console.print(f"\n❓ Missing from Travis: {len(comparison['harris_missing_fields'])}")
            console.print(f"   {', '.join(comparison['harris_missing_fields'])}")
        
        if comparison['recommendations']:
            console.print("\n[bold yellow]💡 Recommendations:[/bold yellow]")
            for rec in comparison['recommendations']:
                console.print(f"   • {rec}")
        
        # Data coverage summary
        high_coverage = [
            field for field, info in comparison['data_coverage'].items() 
            if info['coverage_percentage'] >= 80
        ]
        if high_coverage:
            console.print(f"\n[green]📈 High data coverage fields (≥80%):[/green]")
            console.print(f"   {', '.join(high_coverage)}")
        
        console.print(f"\n[bold green]✅ Successfully processed {len(normalized_records):,} Travis County records![/bold green]")
        
    except Exception as e:
        console.print(f"[red]Error during Travis normalization: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', type=int, default=50, help='Number of records to compare')
@click.pass_context  
def compare_counties(ctx, sample_size):
    """Compare data structures between Harris and Travis counties."""
    console = ctx.obj['console']
    
    try:
        console.print("[bold blue]🔄 Comparing Harris vs Travis County Data Models[/bold blue]")
        
        # Load Travis sample
        from ..models.config import Config
        travis_config = Config()
        travis_normalizer = TravisCountyNormalizer(travis_config)
        
        console.print(f"Loading {sample_size} Travis County records...")
        travis_records = travis_normalizer.load_and_normalize_sample(sample_size)
        
        # Load Harris sample (if available)
        harris_config = Config()
        harris_normalizer = HarrisCountyNormalizer(harris_config)
        
        console.print(f"Loading {sample_size} Harris County records...")
        try:
            # Try to load Harris sample
            real_accounts_df = harris_normalizer._load_real_accounts(sample_size=sample_size)
            owners_df = harris_normalizer._load_owners()
            deeds_df = harris_normalizer._load_deeds()
            permits_df = harris_normalizer._load_permits()
            tieback_df = harris_normalizer._load_parcel_tieback()
            neighborhood_df = harris_normalizer._load_neighborhood_codes()
            mineral_df = harris_normalizer._load_mineral_rights()
            
            harris_records = harris_normalizer._create_json_normalized_data(
                real_accounts_df, owners_df, deeds_df, permits_df,
                tieback_df, neighborhood_df, mineral_df
            )[:sample_size]  # Limit to sample size
            
        except Exception as e:
            console.print(f"[yellow]⚠️ Could not load Harris County data: {e}[/yellow]")
            harris_records = []
        
        # Create comparison table
        from rich.table import Table
        
        comparison_table = Table(title="County Data Model Comparison")
        comparison_table.add_column("Field", style="bold")
        comparison_table.add_column("Travis County", style="cyan")
        comparison_table.add_column("Harris County", style="green")
        comparison_table.add_column("Coverage", style="yellow")
        
        # Analyze Travis structure
        if travis_records:
            travis_sample = travis_records[0]
            travis_fields = set(travis_sample.keys())
            
            # Get Harris structure
            harris_fields = set()
            if harris_records:
                harris_sample = harris_records[0]
                harris_fields = set(harris_sample.keys())
            
            all_fields = travis_fields.union(harris_fields)
            
            for field in sorted(all_fields):
                travis_has = "✅" if field in travis_fields else "❌"
                harris_has = "✅" if field in harris_fields else "❌"
                
                # Calculate coverage for Travis
                if travis_records and field in travis_fields:
                    coverage = sum(1 for r in travis_records if r.get(field) is not None)
                    coverage_pct = f"{(coverage/len(travis_records)*100):.1f}%"
                else:
                    coverage_pct = "N/A"
                
                comparison_table.add_row(field, travis_has, harris_has, coverage_pct)
            
            console.print(comparison_table)
        
        # Summary statistics
        console.print(f"\n[bold]Summary:[/bold]")
        console.print(f"Travis records loaded: {len(travis_records):,}")
        console.print(f"Harris records loaded: {len(harris_records):,}")
        
        if travis_records and harris_records:
            common_fields = travis_fields.intersection(harris_fields)
            travis_unique = travis_fields - harris_fields  
            harris_unique = harris_fields - travis_fields
            
            console.print(f"Common fields: {len(common_fields)}")
            console.print(f"Travis-only fields: {len(travis_unique)}")
            console.print(f"Harris-only fields: {len(harris_unique)}")
            
            if travis_unique:
                console.print(f"\n[cyan]Travis-specific: {', '.join(sorted(travis_unique))}[/cyan]")
            if harris_unique:
                console.print(f"\n[green]Harris-specific: {', '.join(sorted(harris_unique))}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error during comparison: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', type=int, default=10000, help='Number of records to process')
@click.option('--batch-id', help='Custom batch ID for MongoDB storage')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def travis_normalize_mongodb(ctx, sample_size, batch_id, mongo_uri, database):
    """Load and normalize Travis County data directly to MongoDB."""
    console = ctx.obj['console']
    
    try:
        from ..models.config import Config
        travis_config = Config()
        normalizer = TravisCountyNormalizer(travis_config)
        
        console.print(f"[bold blue]🏛️ Travis County → MongoDB Pipeline[/bold blue]")
        console.print(f"Processing {sample_size:,} properties directly to MongoDB")
        
        # Load and normalize Travis data
        console.print("[blue]📊 Loading and normalizing Travis County data...[/blue]")
        normalized_records = normalizer.load_and_normalize_sample(sample_size)
        
        if not normalized_records:
            console.print("[red]No records were normalized[/red]")
            return
        
        console.print(f"[green]✅ Successfully normalized {len(normalized_records):,} records[/green]")
        
        # Save directly to MongoDB
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            result = mongodb.save_properties(
                normalized_records, 
                batch_id=batch_id or f"travis_batch_{len(normalized_records)}",
                source_files=['PROP.TXT', 'PROP_ENT.TXT']
            )
            
            console.print(f"\n[bold green]🎉 Successfully saved to MongoDB![/bold green]")
            console.print(f"📊 Batch ID: {result['batch_id']}")
            console.print(f"💾 Properties: {result['saved_count']:,}")
            console.print(f"📅 Timestamp: {result['timestamp']}")
            
            # Show collection stats
            stats = mongodb.get_collection_stats()
            console.print(f"\n[blue]📈 Database Status:[/blue]")
            console.print(f"   Total properties: {stats['properties_count']:,}")
            console.print(f"   Processing logs: {stats['logs_count']:,}")
            
            # Show data summary
            console.print(f"\n[bold cyan]📋 Travis County Data Summary:[/bold cyan]")
            console.print(f"   • Account IDs: 12-digit format")
            console.print(f"   • Tax Entities: {sum(len(r.get('tax_entities', [])) for r in normalized_records[:10]):,} (sample)")
            console.print(f"   • Complete Owners: {len([r for r in normalized_records[:100] if r.get('owners')])}/100 (sample)")
            console.print(f"   • Legal Descriptions: {len([r for r in normalized_records[:100] if r.get('property_details', {}).get('legal_description')])}/100 (sample)")
            
            # Data coverage analysis
            sample_records = normalized_records[:100]  # Analyze first 100 for speed
            high_coverage_fields = []
            
            for field in ['mailing_address', 'property_details', 'valuation', 'tax_entities']:
                coverage = sum(1 for r in sample_records if r.get(field) and 
                              (not isinstance(r[field], dict) or any(v for v in r[field].values() if v is not None)))
                if coverage >= 80:  # 80% coverage threshold
                    high_coverage_fields.append(f"{field} ({coverage}%)")
            
            if high_coverage_fields:
                console.print(f"\n[green]📊 High Coverage Fields:[/green]")
                console.print(f"   {', '.join(high_coverage_fields)}")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error during Travis MongoDB processing: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.pass_context
def dallas_diagnose(ctx):
    """Diagnose Dallas County data files structure and quality."""
    from ..models.config import Config
    dallas_config = Config()
    console = ctx.obj['console']
    
    try:
        normalizer = DallasCountyNormalizer(dallas_config)
        console.print("[bold blue]🏛️ Dallas County Data Diagnosis[/bold blue]\n")
        
        diagnostics = normalizer.diagnose_files()
        
        for file_name, info in diagnostics.items():
            if info['status'] == 'ok':
                console.print(f"[green]✅ {file_name.upper()}[/green]")
                console.print(f"   📁 Size: {info['size_mb']:.1f} MB")
                console.print(f"   📊 Est. Records: {info['estimated_rows']:,}")
                console.print(f"   🔍 Columns: {len(info['columns'])}")
                console.print(f"   📋 Fields: {', '.join(info['columns'][:8])}{'...' if len(info['columns']) > 8 else ''}")
            elif info['status'] == 'missing':
                console.print(f"[red]❌ {file_name.upper()} - Missing[/red]")
                console.print(f"   📁 Expected: {info['path']}")
            else:
                console.print(f"[yellow]⚠️ {file_name.upper()} - Error[/yellow]")
                console.print(f"   ❗ {info['error']}")
            console.print()
        
        # Summary
        total_files = len(diagnostics)
        ok_files = sum(1 for info in diagnostics.values() if info['status'] == 'ok')
        
        console.print(f"[bold]📈 Summary: {ok_files}/{total_files} files ready[/bold]")
        
        if ok_files == total_files:
            console.print("[green]🎉 All Dallas County files are ready for processing![/green]")
        elif ok_files >= 4:  # Account info + appraisal are minimum
            console.print("[yellow]⚠️ Some files missing but core files available[/yellow]")
        else:
            console.print("[red]❌ Missing critical files for processing[/red]")
    
    except Exception as e:
        console.print(f"[red]Error during Dallas diagnosis: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', type=int, default=100, help='Number of records to analyze')
@click.pass_context
def dallas_analyze(ctx, sample_size):
    """Analyze Dallas County data structure in detail."""
    from ..models.config import Config
    dallas_config = Config()
    console = ctx.obj['console']
    
    try:
        console.print(f"[bold blue]🔍 Dallas County Deep Analysis ({sample_size:,} records)[/bold blue]\n")
        
        normalizer = DallasCountyNormalizer(dallas_config)
        records = normalizer.load_and_normalize_sample(sample_size)
        
        if not records:
            console.print("[red]No records were processed[/red]")
            return
        
        # Analyze the structure
        console.print(f"[bold green]📋 Dallas County Analysis Results:[/bold green]")
        console.print(f"   Total Records: {len(records):,}")
        
        # Sample record structure
        sample_record = records[0]
        console.print(f"\n[bold cyan]🏗️ Unified Structure Preview:[/bold cyan]")
        console.print(f"   Account ID Format: {sample_record['account_id']} (17-digit)")
        console.print(f"   County: {sample_record['county']}")
        console.print(f"   Tax Entities: {len(sample_record.get('tax_entities', []))}")
        console.print(f"   Owner Info: {'✅' if sample_record.get('owners') else '❌'}")
        console.print(f"   Property Address: {'✅' if sample_record.get('property_address', {}).get('street_address') else '❌'}")
        console.print(f"   Valuation Data: {'✅' if sample_record.get('valuation', {}).get('total_value') else '❌'}")
        
        # Tax entities analysis
        if records[0].get('tax_entities'):
            console.print(f"\n[bold yellow]🏛️ Sample Tax Entities:[/bold yellow]")
            for entity in records[0]['tax_entities'][:3]:
                console.print(f"   • {entity['entity_name']} ({entity['entity_type']}): ${entity['taxable_value']:,}")
        
        # Property details coverage
        property_types = {}
        building_years = []
        
        for record in records[:50]:  # Sample analysis
            prop_details = record.get('property_details', {})
            division = prop_details.get('division_code', 'Unknown')
            property_types[division] = property_types.get(division, 0) + 1
            
            year_built = prop_details.get('year_built')
            if year_built and year_built > 0:
                building_years.append(year_built)
        
        console.print(f"\n[bold magenta]🏠 Property Type Distribution (sample):[/bold magenta]")
        for prop_type, count in property_types.items():
            console.print(f"   {prop_type}: {count} properties")
        
        if building_years:
            avg_year = sum(building_years) / len(building_years)
            console.print(f"\n[bold green]📅 Average Building Year: {avg_year:.0f}[/bold green]")
    
    except Exception as e:
        console.print(f"[red]Error during Dallas analysis: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', type=int, default=1000, help='Number of records to process')
@click.option('--output-file', type=click.Path(), help='Optional JSON output file')
@click.pass_context
def dallas_normalize_sample(ctx, sample_size, output_file):
    """Load and normalize Dallas County data sample."""
    from ..models.config import Config
    dallas_config = Config()
    console = ctx.obj['console']
    
    try:
        normalizer = DallasCountyNormalizer(dallas_config)
        
        console.print(f"[bold blue]🏛️ Dallas County Sample Normalization[/bold blue]")
        console.print(f"Processing {sample_size:,} properties")
        
        normalized_records = normalizer.load_and_normalize_sample(sample_size)
        
        if not normalized_records:
            console.print("[red]No records were normalized[/red]")
            return
        
        # Save to file if requested
        if output_file:
            from pathlib import Path
            normalizer.save_sample_output(normalized_records, Path(output_file))
        
        # Display results
        console.print(f"\n[bold green]🎉 Dallas County Normalization Complete![/bold green]")
        console.print(f"   Records Processed: {len(normalized_records):,}")
        console.print(f"   Account ID Format: 17-digit Dallas format")
        console.print(f"   Unified JSON Structure: ✅")
        
        # Show sample data
        sample = normalized_records[0]
        console.print(f"\n[bold cyan]📋 Sample Record:[/bold cyan]")
        console.print(f"   Account: {sample['account_id']}")
        console.print(f"   Owner: {sample['mailing_address']['name'][:50]}...")
        console.print(f"   Address: {sample['property_address']['street_address']}")
        console.print(f"   Value: ${sample['valuation']['total_value']:,}")
        console.print(f"   Tax Entities: {len(sample['tax_entities'])}")
        
        if output_file:
            console.print(f"\n[green]💾 Saved to: {output_file}[/green]")
    
    except Exception as e:
        console.print(f"[red]Error during Dallas normalization: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', type=int, default=10000, help='Number of records to process')
@click.option('--batch-id', help='Custom batch ID for MongoDB storage')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def dallas_normalize_mongodb(ctx, sample_size, batch_id, mongo_uri, database):
    """Load and normalize Dallas County data directly to MongoDB."""
    console = ctx.obj['console']
    
    try:
        from ..models.config import Config
        dallas_config = Config()
        normalizer = DallasCountyNormalizer(dallas_config)
        
        console.print(f"[bold blue]🏛️ Dallas County → MongoDB Pipeline[/bold blue]")
        console.print(f"Processing {sample_size:,} properties directly to MongoDB")
        
        # Load and normalize Dallas data
        console.print("[blue]📊 Loading and normalizing Dallas County data...[/blue]")
        normalized_records = normalizer.load_and_normalize_sample(sample_size)
        
        if not normalized_records:
            console.print("[red]No records were normalized[/red]")
            return
        
        console.print(f"[green]✅ Successfully normalized {len(normalized_records):,} records[/green]")
        
        # Save directly to MongoDB
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            result = mongodb.save_properties(
                normalized_records, 
                batch_id=batch_id or f"dallas_batch_{len(normalized_records)}",
                source_files=['ACCOUNT_INFO.CSV', 'ACCOUNT_APPRL_YEAR.CSV', 'MULTI_OWNER.CSV']
            )
            
            console.print(f"\n[bold green]🎉 Successfully saved to MongoDB![/bold green]")
            console.print(f"📊 Batch ID: {result['batch_id']}")
            console.print(f"💾 Properties: {result['saved_count']:,}")
            console.print(f"📅 Timestamp: {result['timestamp']}")
            
            # Show collection stats
            stats = mongodb.get_collection_stats()
            console.print(f"\n[blue]📈 Database Status:[/blue]")
            console.print(f"   Total properties: {stats['properties_count']:,}")
            console.print(f"   Processing logs: {stats['logs_count']:,}")
            
            # Show data summary
            console.print(f"\n[bold cyan]📋 Dallas County Data Summary:[/bold cyan]")
            console.print(f"   • Account IDs: 17-digit format")
            console.print(f"   • Tax Entities: {sum(len(r.get('tax_entities', [])) for r in normalized_records[:10]):,} (sample)")
            console.print(f"   • Multiple Owners: {len([r for r in normalized_records[:100] if len(r.get('owners', [])) > 1])}/100 (sample)")
            console.print(f"   • Building Details: {len([r for r in normalized_records[:100] if r.get('property_details', {}).get('year_built')])}/100 (sample)")
            console.print(f"   • Land Information: {len([r for r in normalized_records[:100] if r.get('property_details', {}).get('zoning')])}/100 (sample)")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error during Dallas MongoDB processing: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', type=int, default=100, help='Number of random properties to display')
@click.option('--county', help='Filter by specific county (harris, travis, dallas)')
@click.option('--format', 'output_format', type=click.Choice(['table', 'detailed', 'stats']), 
              default='table', help='Output format')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def view_properties(ctx, sample_size, county, output_format, mongo_uri, database):
    """View random sample of properties from the unified database."""
    console = ctx.obj['console']
    
    try:
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            console.print(f"[bold blue]🏠 Property Database Viewer[/bold blue]")
            console.print(f"Sampling {sample_size:,} random properties{f' from {county.title()} County' if county else ' from all counties'}")
            
            # Build query filter
            query = {}
            if county:
                query['county'] = county.lower()
            
            # Get random sample using MongoDB aggregation
            pipeline = [
                {'$match': query},
                {'$sample': {'size': sample_size}}
            ]
            
            properties = list(mongodb.properties_collection.aggregate(pipeline))
            
            if not properties:
                console.print("[red]No properties found matching criteria[/red]")
                return
            
            console.print(f"[green]✅ Found {len(properties):,} properties[/green]\n")
            
            if output_format == 'stats':
                _display_property_stats(console, properties)
            elif output_format == 'detailed':
                _display_detailed_properties(console, properties)
            else:  # table format
                _display_property_table(console, properties)
                
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error viewing properties: {e}[/red]")
        raise click.ClickException(str(e))


def _display_property_table(console: Console, properties: list):
    """Display properties in a table format."""
    from rich.table import Table
    
    table = Table(title="🏠 Multi-County Property Sample", show_header=True, header_style="bold blue")
    table.add_column("County", style="cyan", width=8)
    table.add_column("Account ID", style="yellow", width=18)
    table.add_column("Owner", style="green", width=25)
    table.add_column("Address", style="magenta", width=30)
    table.add_column("Market Value", justify="right", style="white", width=12)
    table.add_column("Tax Entities", justify="center", style="blue", width=10)
    
    for prop in properties[:50]:  # Limit to 50 for readability
        county = prop.get('county', 'unknown').title()
        account_id = prop.get('account_id', 'N/A')
        
        # Get owner name
        owner_name = "Unknown Owner"
        if prop.get('mailing_address', {}).get('name'):
            owner_name = prop['mailing_address']['name'][:22] + "..." if len(prop['mailing_address']['name']) > 22 else prop['mailing_address']['name']
        
        # Get property address
        address = "No Address"
        if prop.get('property_address', {}).get('street_address'):
            street = prop['property_address']['street_address']
            city = prop['property_address'].get('city', '')
            if city:
                address = f"{street}, {city}"[:28] + "..." if len(f"{street}, {city}") > 28 else f"{street}, {city}"
            else:
                address = street[:28] + "..." if len(street) > 28 else street
        
        # Get market value
        market_value = 0
        valuation = prop.get('valuation', {})
        if valuation.get('market_value'):
            market_value = int(valuation['market_value'])
        elif valuation.get('total_value'):
            market_value = int(valuation['total_value'])
        
        value_str = f"${market_value:,}" if market_value > 0 else "$0"
        
        # Get tax entities count
        tax_entities_count = len(prop.get('tax_entities', []))
        
        table.add_row(
            county,
            account_id,
            owner_name,
            address,
            value_str,
            str(tax_entities_count) if tax_entities_count > 0 else "-"
        )
    
    console.print(table)
    
    if len(properties) > 50:
        console.print(f"\n[yellow]📝 Showing first 50 of {len(properties):,} properties[/yellow]")


def _display_detailed_properties(console: Console, properties: list):
    """Display properties in detailed format."""
    for i, prop in enumerate(properties[:10], 1):  # Show first 10 in detail
        console.print(f"\n[bold cyan]🏠 Property {i}: {prop.get('account_id', 'Unknown')}[/bold cyan]")
        console.print(f"[blue]County:[/blue] {prop.get('county', 'unknown').title()}")
        
        # Owner info
        mailing = prop.get('mailing_address', {})
        if mailing.get('name'):
            console.print(f"[green]Owner:[/green] {mailing['name']}")
            if mailing.get('city') and mailing.get('state'):
                console.print(f"[green]Mailing:[/green] {mailing.get('city', '')}, {mailing.get('state', '')}")
        
        # Property address
        prop_addr = prop.get('property_address', {})
        if prop_addr.get('street_address'):
            address_parts = [prop_addr.get('street_address', '')]
            if prop_addr.get('city'):
                address_parts.append(prop_addr['city'])
            if prop_addr.get('zip_code'):
                address_parts.append(prop_addr['zip_code'])
            console.print(f"[magenta]Address:[/magenta] {', '.join(address_parts)}")
        
        # Valuation
        valuation = prop.get('valuation', {})
        if valuation:
            market_val = valuation.get('market_value') or valuation.get('total_value', 0)
            if market_val:
                console.print(f"[yellow]Value:[/yellow] ${int(market_val):,}")
            
            land_val = valuation.get('land_value', 0)
            if land_val:
                console.print(f"[yellow]Land:[/yellow] ${int(land_val):,}")
        
        # Tax entities
        tax_entities = prop.get('tax_entities', [])
        if tax_entities:
            console.print(f"[blue]Tax Entities ({len(tax_entities)}):[/blue]")
            for entity in tax_entities[:3]:  # Show first 3
                name = entity.get('entity_name', 'Unknown')
                value = entity.get('taxable_value', 0)
                console.print(f"  • {name}: ${int(value):,}" if value else f"  • {name}")
        
        # Property details
        prop_details = prop.get('property_details', {})
        interesting_details = []
        if prop_details.get('year_built'):
            interesting_details.append(f"Built: {prop_details['year_built']}")
        if prop_details.get('living_area_sf'):
            interesting_details.append(f"Living Area: {prop_details['living_area_sf']:,} sq ft")
        if prop_details.get('num_bedrooms'):
            interesting_details.append(f"Bedrooms: {prop_details['num_bedrooms']}")
        
        if interesting_details:
            console.print(f"[white]Details:[/white] {', '.join(interesting_details)}")


def _display_property_stats(console: Console, properties: list):
    """Display statistical summary of properties."""
    from collections import Counter
    
    console.print(f"[bold green]📊 Property Statistics ({len(properties):,} properties)[/bold green]\n")
    
    # County distribution
    counties = Counter(prop.get('county', 'unknown') for prop in properties)
    console.print("[bold blue]🏛️ County Distribution:[/bold blue]")
    for county, count in counties.most_common():
        percentage = (count / len(properties)) * 100
        console.print(f"  {county.title()}: {count:,} ({percentage:.1f}%)")
    
    # Value statistics
    values = []
    for prop in properties:
        valuation = prop.get('valuation', {})
        val = valuation.get('market_value') or valuation.get('total_value', 0)
        if val and val > 0:
            values.append(int(val))
    
    if values:
        console.print(f"\n[bold yellow]💰 Property Values:[/bold yellow]")
        console.print(f"  Properties with values: {len(values):,}/{len(properties):,}")
        console.print(f"  Average: ${sum(values) // len(values):,}")
        console.print(f"  Median: ${sorted(values)[len(values)//2]:,}")
        console.print(f"  Range: ${min(values):,} - ${max(values):,}")
    
    # Tax entities statistics
    tax_entity_counts = [len(prop.get('tax_entities', [])) for prop in properties]
    if tax_entity_counts:
        avg_entities = sum(tax_entity_counts) / len(tax_entity_counts)
        console.print(f"\n[bold blue]🏛️ Tax Entities:[/bold blue]")
        console.print(f"  Average per property: {avg_entities:.1f}")
        console.print(f"  Range: {min(tax_entity_counts)} - {max(tax_entity_counts)}")
    
    # Property types (if available)
    prop_types = []
    years_built = []
    
    for prop in properties:
        details = prop.get('property_details', {})
        
        # Property type from division code or other indicators
        if details.get('division_code'):
            prop_types.append(details['division_code'])
        elif details.get('property_type'):
            prop_types.append(details['property_type'])
        
        # Year built
        if details.get('year_built') and details['year_built'] > 0:
            years_built.append(details['year_built'])
    
    if prop_types:
        type_counts = Counter(prop_types)
        console.print(f"\n[bold cyan]🏠 Property Types:[/bold cyan]")
        for prop_type, count in type_counts.most_common(5):
            console.print(f"  {prop_type}: {count:,}")
    
    if years_built:
        avg_year = sum(years_built) / len(years_built)
        console.print(f"\n[bold green]📅 Building Ages:[/bold green]")
        console.print(f"  Average year built: {avg_year:.0f}")
        console.print(f"  Oldest: {min(years_built)}")
        console.print(f"  Newest: {max(years_built)}")


@cli.command()
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def fix_harris_county(ctx, mongo_uri, database):
    """Fix existing Harris County records to set county field properly."""
    console = ctx.obj['console']
    
    try:
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            console.print("[blue]🔧 Fixing Harris County records...[/blue]")
            
            # Count records without county field (these are the old Harris County records)
            count_missing = mongodb.properties_collection.count_documents({'county': {'$exists': False}})
            console.print(f"Found {count_missing:,} records without county field (Harris County)")
            
            if count_missing == 0:
                console.print("[green]✅ No records need fixing - all have county field set![/green]")
                return
            
            # Update all records without a county field to 'harris'
            result = mongodb.properties_collection.update_many(
                {'county': {'$exists': False}},  # Records without county field
                {'$set': {'county': 'harris'}}   # Set county to harris
            )
            
            console.print(f"[green]✅ Updated {result.modified_count:,} Harris County records[/green]")
            
            # Verify the fix by checking updated distribution
            pipeline = [{'$group': {'_id': '$county', 'count': {'$sum': 1}}}]
            county_counts = list(mongodb.properties_collection.aggregate(pipeline))
            
            console.print(f"\n[bold blue]🏛️ Updated County Distribution:[/bold blue]")
            total_count = 0
            for county_data in sorted(county_counts, key=lambda x: x['_id']):
                count = county_data['count']
                total_count += count
                console.print(f"  {county_data['_id'].title()}: {count:,} properties")
            
            console.print(f"\n[bold green]🎉 All {total_count:,} properties now have county field set![/bold green]")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error fixing Harris County records: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def fix_harris_addresses(ctx, mongo_uri, database):
    """Fix Harris County records to add street_address field from full_address."""
    console = ctx.obj['console']
    
    try:
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            console.print("[blue]🔧 Fixing Harris County address fields...[/blue]")
            
            # Count Harris County records that need the street_address field added
            count_missing = mongodb.properties_collection.count_documents({
                'county': 'harris',
                'property_address.street_address': {'$exists': False}
            })
            console.print(f"Found {count_missing:,} Harris County records missing street_address field")
            
            if count_missing == 0:
                console.print("[green]✅ No records need fixing - all have street_address field![/green]")
                return
            
            # Get Harris County records that need fixing
            records_to_fix = list(mongodb.properties_collection.find(
                {
                    'county': 'harris',
                    'property_address.street_address': {'$exists': False}
                },
                {'_id': 1, 'property_address.full_address': 1}
            ))
            
            # Update each record to copy full_address to street_address
            updated_count = 0
            for record in records_to_fix:
                full_address = record.get('property_address', {}).get('full_address')
                if full_address:
                    mongodb.properties_collection.update_one(
                        {'_id': record['_id']},
                        {'$set': {'property_address.street_address': full_address}}
                    )
                    updated_count += 1
            
            class UpdateResult:
                def __init__(self, modified_count):
                    self.modified_count = modified_count
            
            result = UpdateResult(updated_count)
            
            console.print(f"[green]✅ Updated {result.modified_count:,} Harris County address records[/green]")
            
            # Test that addresses are now visible
            sample_properties = list(mongodb.properties_collection.find(
                {'county': 'harris', 'property_address.street_address': {'$ne': None}}, 
                {'property_address': 1, 'account_id': 1}
            ).limit(3))
            
            if sample_properties:
                console.print(f"\n[bold blue]✅ Sample Fixed Addresses:[/bold blue]")
                for prop in sample_properties:
                    addr = prop.get('property_address', {})
                    street = addr.get('street_address', 'N/A')
                    city = addr.get('city', 'N/A') 
                    console.print(f"  {prop['account_id']}: {street}, {city}")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error fixing Harris County addresses: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def fix_harris_market_values(ctx, mongo_uri, database):
    """Fix Harris County records to add market_value field from total_market_value."""
    console = ctx.obj['console']
    
    try:
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            console.print("[blue]🔧 Fixing Harris County market value fields...[/blue]")
            
            # Count Harris County records that need the market_value field added
            count_missing = mongodb.properties_collection.count_documents({
                'county': 'harris',
                'valuation.market_value': {'$exists': False}
            })
            console.print(f"Found {count_missing:,} Harris County records missing market_value field")
            
            if count_missing == 0:
                console.print("[green]✅ No records need fixing - all have market_value field![/green]")
                return
            
            # Get Harris County records that need fixing
            records_to_fix = list(mongodb.properties_collection.find(
                {
                    'county': 'harris',
                    'valuation.market_value': {'$exists': False}
                },
                {'_id': 1, 'valuation.total_market_value': 1}
            ))
            
            # Update each record to copy total_market_value to market_value
            updated_count = 0
            for record in records_to_fix:
                total_market_val = record.get('valuation', {}).get('total_market_value')
                if total_market_val is not None:
                    mongodb.properties_collection.update_one(
                        {'_id': record['_id']},
                        {'$set': {'valuation.market_value': total_market_val}}
                    )
                    updated_count += 1
            
            console.print(f"[green]✅ Updated {updated_count:,} Harris County market value records[/green]")
            
            # Test that market values are now visible
            sample_properties = list(mongodb.properties_collection.find(
                {'county': 'harris', 'valuation.market_value': {'$gt': 0}}, 
                {'valuation.market_value': 1, 'account_id': 1}
            ).limit(5))
            
            if sample_properties:
                console.print(f"\n[bold blue]✅ Sample Fixed Market Values:[/bold blue]")
                for prop in sample_properties:
                    market_val = prop.get('valuation', {}).get('market_value', 0)
                    console.print(f"  {prop['account_id']}: ${market_val:,}")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error fixing Harris County market values: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.pass_context
def info(ctx):
    """Display information about data files and configuration."""
    
    config = ctx.obj['config']
    console = ctx.obj['console']
    
    # Create info table
    table = Table(title="County Data Parser Configuration")
    table.add_column("Setting", style="bold")
    table.add_column("Value")
    
    table.add_row("Data Directory", str(config.data_dir))
    table.add_row("Output Directory", str(config.output_dir))
    table.add_row("Chunk Size", str(config.parsing.chunk_size))
    table.add_row("Max Workers", str(config.parsing.max_workers))
    table.add_row("Output Format", config.parsing.output_format)
    
    console.print(table)
    
    # File information
    console.print("\n[bold]File Information:[/bold]")
    
    parsers = [
        ("Real Accounts", RealAccountsParser(config)),
        ("Owners", OwnersParser(config))
    ]
    
    for name, parser in parsers:
        info = parser.get_file_info()
        if "error" not in info:
            console.print(f"✅ {name}: {info['file_size_mb']} MB ({info['file_path']})")
        else:
            console.print(f"❌ {name}: File not found ({parser.get_file_path()})")


@cli.command()
@click.option('--sample-size', default=5000, help='Number of random properties to sample and load')
@click.option('--batch-id', help='Custom batch ID for tracking')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.option('--force-reload', is_flag=True, help='Force reload even if data exists')
@click.pass_context
def load_harris_sample_for_frontend(ctx, sample_size, batch_id, mongo_uri, database, force_reload):
    """Load a random sample of Harris County properties into MongoDB for frontend review."""
    console = ctx.obj['console']
    
    try:
        from ..models.config import Config
        harris_config = Config()
        harris_config.county_type = "harris"  # Ensure Harris County mode
        
        console.print(f"[bold blue]🏛️ Harris County Frontend Sample Loader[/bold blue]")
        console.print(f"🎯 Target: {sample_size:,} random properties for frontend review")
        
        # Check if we already have Harris County data
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            # Check existing Harris County data
            existing_harris_count = mongodb.properties_collection.count_documents({'county': 'harris'})
            
            if existing_harris_count > 0 and not force_reload:
                console.print(f"[yellow]⚠️ Found {existing_harris_count:,} existing Harris County properties[/yellow]")
                console.print("[yellow]Use --force-reload to replace existing data[/yellow]")
                
                # Show sample of existing data
                sample_existing = list(mongodb.properties_collection.find(
                    {'county': 'harris'}, 
                    {'account_id': 1, 'property_address': 1, 'valuation': 1}
                ).limit(5))
                
                if sample_existing:
                    console.print(f"\n[blue]📋 Sample of existing data:[/blue]")
                    for prop in sample_existing:
                        addr = prop.get('property_address', {})
                        street = addr.get('street_address', 'N/A')
                        city = addr.get('city', 'N/A')
                        value = prop.get('valuation', {}).get('market_value', 'N/A')
                        console.print(f"  {prop['account_id']}: {street}, {city} - ${value:,}" if isinstance(value, (int, float)) else f"  {prop['account_id']}: {street}, {city} - {value}")
                
                return
            
            # Initialize Harris normalizer
            normalizer = HarrisCountyNormalizer(harris_config)
            
            # Load and normalize Harris data
            console.print(f"\n[blue]📊 Loading and normalizing {sample_size:,} Harris County properties...[/blue]")
            normalized_records = normalizer.load_and_normalize_sample(sample_size)
            
            if not normalized_records:
                console.print("[red]No records were normalized[/red]")
                return
            
            console.print(f"[green]✅ Successfully normalized {len(normalized_records):,} records[/green]")
            
            # Save to MongoDB
            batch_id = batch_id or f"harris_frontend_{len(normalized_records)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            console.print(f"\n[blue]💾 Saving to MongoDB with batch ID: {batch_id}[/blue]")
            
            result = mongodb.save_properties(
                normalized_records, 
                batch_id=batch_id,
                source_files=['real_acct.txt', 'owners.txt', 'deeds.txt']
            )
            
            console.print(f"\n[bold green]🎉 Successfully loaded Harris County sample for frontend![/bold green]")
            console.print(f"📊 Batch ID: {result['batch_id']}")
            console.print(f"💾 Properties: {result['saved_count']:,}")
            console.print(f"📅 Timestamp: {result['timestamp']}")
            
            # Show collection stats
            stats = mongodb.get_collection_stats()
            console.print(f"\n[blue]📈 Database Status:[/blue]")
            console.print(f"   Total properties: {stats['properties_count']:,}")
            console.print(f"   Harris County: {mongodb.properties_collection.count_documents({'county': 'harris'}):,}")
            console.print(f"   Processing logs: {stats['logs_count']:,}")
            
            # Data quality analysis for frontend
            console.print(f"\n[bold cyan]📋 Frontend Data Quality Report:[/bold cyan]")
            
            # Sample analysis for key fields
            sample_records = normalized_records[:100]  # Analyze first 100 for speed
            
            # Address coverage
            address_coverage = sum(1 for r in sample_records if r.get('property_address', {}).get('street_address'))
            console.print(f"   📍 Property Addresses: {address_coverage}/{len(sample_records)} ({address_coverage/len(sample_records)*100:.1f}%)")
            
            # Owner coverage
            owner_coverage = sum(1 for r in sample_records if r.get('mailing_address', {}).get('name'))
            console.print(f"   👤 Owner Names: {owner_coverage}/{len(sample_records)} ({owner_coverage/len(sample_records)*100:.1f}%)")
            
            # Valuation coverage
            valuation_coverage = sum(1 for r in sample_records if r.get('valuation', {}).get('market_value'))
            console.print(f"   💰 Market Values: {valuation_coverage}/{len(sample_records)} ({valuation_coverage/len(sample_records)*100:.1f}%)")
            
            # Tax entities coverage
            tax_entities_coverage = sum(1 for r in sample_records if r.get('tax_entities'))
            console.print(f"   🏛️ Tax Entities: {tax_entities_coverage}/{len(sample_records)} ({tax_entities_coverage/len(sample_records)*100:.1f}%)")
            
            # Improvements coverage
            improvements_coverage = sum(1 for r in sample_records if r.get('improvements'))
            console.print(f"   🏗️ Improvements: {improvements_coverage}/{len(sample_records)} ({improvements_coverage/len(sample_records)*100:.1f}%)")
            
            # Sample data preview for frontend
            console.print(f"\n[bold blue]🔍 Frontend Data Preview:[/bold blue]")
            preview_records = normalized_records[:3]
            
            for i, record in enumerate(preview_records, 1):
                console.print(f"\n  [cyan]Record {i}:[/cyan]")
                console.print(f"    Account ID: {record.get('account_id', 'N/A')}")
                console.print(f"    Owner: {record.get('mailing_address', {}).get('name', 'N/A')}")
                console.print(f"    Address: {record.get('property_address', {}).get('street_address', 'N/A')}, {record.get('property_address', {}).get('city', 'N/A')}")
                console.print(f"    Market Value: ${record.get('valuation', {}).get('market_value', 'N/A'):,}" if isinstance(record.get('valuation', {}).get('market_value'), (int, float)) else f"    Market Value: {record.get('valuation', {}).get('market_value', 'N/A')}")
                console.print(f"    Tax Entities: {len(record.get('tax_entities', []))}")
                console.print(f"    Improvements: {len(record.get('improvements', []))}")
            
            # Frontend access instructions
            console.print(f"\n[bold green]🚀 Frontend Access:[/bold green]")
            console.print(f"   🌐 Web App: http://localhost:5000")
            console.print(f"   📊 API Endpoint: /api/properties?county=harris&limit={min(sample_size, 1000)}")
            console.print(f"   📈 Stats: /api/stats")
            console.print(f"   🔍 Filter by county: 'harris' in the frontend dropdown")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error during Harris County frontend sample loading: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', default=5000, help='Number of random properties to sample and load')
@click.option('--batch-id', help='Custom batch ID for tracking')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.option('--force-reload', is_flag=True, help='Force reload even if data exists')
@click.pass_context
def load_travis_sample_for_frontend(ctx, sample_size, batch_id, mongo_uri, database, force_reload):
    """Load a random sample of Travis County properties into MongoDB for frontend review."""
    console = ctx.obj['console']
    
    try:
        from ..models.config import Config
        travis_config = Config()
        travis_config.county_type = "travis"  # Ensure Travis County mode
        
        console.print(f"[bold blue]🏛️ Travis County Frontend Sample Loader[/bold blue]")
        console.print(f"🎯 Target: {sample_size:,} random properties for frontend review")
        
        # Check if we already have Travis County data
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            # Check existing Travis County data
            existing_travis_count = mongodb.properties_collection.count_documents({'county': 'travis'})
            
            if existing_travis_count > 0 and not force_reload:
                console.print(f"[yellow]⚠️ Found {existing_travis_count:,} existing Travis County properties[/yellow]")
                console.print("[yellow]Use --force-reload to replace existing data[/yellow]")
                
                # Show sample of existing data
                sample_existing = list(mongodb.properties_collection.find(
                    {'county': 'travis'}, 
                    {'account_id': 1, 'property_address': 1, 'valuation': 1}
                ).limit(5))
                
                if sample_existing:
                    console.print(f"\n[blue]📋 Sample of existing data:[/blue]")
                    for prop in sample_existing:
                        addr = prop.get('property_address', {})
                        street = addr.get('street_address', 'N/A')
                        city = addr.get('city', 'N/A')
                        value = prop.get('valuation', {}).get('market_value', 'N/A')
                        console.print(f"  {prop['account_id']}: {street}, {city} - ${value:,}" if isinstance(value, (int, float)) else f"  {prop['account_id']}: {street}, {city} - {value}")
                
                return
            
            # Initialize Travis normalizer
            normalizer = TravisCountyNormalizer(travis_config)
            
            # Load and normalize Travis data
            console.print(f"\n[blue]📊 Loading and normalizing {sample_size:,} Travis County properties...[/blue]")
            normalized_records = normalizer.load_and_normalize_sample(sample_size)
            
            if not normalized_records:
                console.print("[red]No records were normalized[/red]")
                return
            
            console.print(f"[green]✅ Successfully normalized {len(normalized_records):,} records[/green]")
            
            # Save to MongoDB
            batch_id = batch_id or f"travis_frontend_{len(normalized_records)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            console.print(f"\n[blue]💾 Saving to MongoDB with batch ID: {batch_id}[/blue]")
            
            result = mongodb.save_properties(
                normalized_records, 
                batch_id=batch_id,
                source_files=['PROP.TXT', 'PROP_ENT.TXT', 'IMP_DET.TXT', 'LAND_DET.TXT', 'AGENT.TXT']
            )
            
            console.print(f"\n[bold green]🎉 Successfully loaded Travis County sample for frontend![/bold green]")
            console.print(f"📊 Batch ID: {result['batch_id']}")
            console.print(f"💾 Properties: {result['saved_count']:,}")
            console.print(f"📅 Timestamp: {result['timestamp']}")
            
            # Show collection stats
            stats = mongodb.get_collection_stats()
            console.print(f"\n[blue]📈 Database Status:[/blue]")
            console.print(f"   Total properties: {stats['properties_count']:,}")
            console.print(f"   Travis County: {mongodb.properties_collection.count_documents({'county': 'travis'}):,}")
            console.print(f"   Processing logs: {stats['logs_count']:,}")
            
            # Data quality analysis for frontend
            console.print(f"\n[bold cyan]📋 Frontend Data Quality Report:[/bold cyan]")
            
            # Sample analysis for key fields
            sample_records = normalized_records[:100]  # Analyze first 100 for speed
            
            # Address coverage
            address_coverage = sum(1 for r in sample_records if r.get('property_address', {}).get('street_address'))
            console.print(f"   📍 Property Addresses: {address_coverage}/{len(sample_records)} ({address_coverage/len(sample_records)*100:.1f}%)")
            
            # Owner coverage
            owner_coverage = sum(1 for r in sample_records if r.get('mailing_address', {}).get('name'))
            console.print(f"   👤 Owner Names: {owner_coverage}/{len(sample_records)} ({owner_coverage/len(sample_records)*100:.1f}%)")
            
            # Valuation coverage
            valuation_coverage = sum(1 for r in sample_records if r.get('valuation', {}).get('market_value'))
            console.print(f"   💰 Market Values: {valuation_coverage}/{len(sample_records)} ({valuation_coverage/len(sample_records)*100:.1f}%)")
            
            # Tax entities coverage
            tax_entities_coverage = sum(1 for r in sample_records if r.get('tax_entities'))
            console.print(f"   🏛️ Tax Entities: {tax_entities_coverage}/{len(sample_records)} ({tax_entities_coverage/len(sample_records)*100:.1f}%)")
            
            # Improvements coverage
            improvements_coverage = sum(1 for r in sample_records if r.get('improvements'))
            console.print(f"   🏗️ Improvements: {improvements_coverage}/{len(sample_records)} ({improvements_coverage/len(sample_records)*100:.1f}%)")
            
            # Sample data preview for frontend
            console.print(f"\n[bold blue]🔍 Frontend Data Preview:[/bold blue]")
            preview_records = normalized_records[:3]
            
            for i, record in enumerate(preview_records, 1):
                console.print(f"\n  [cyan]Record {i}:[/cyan]")
                console.print(f"    Account ID: {record.get('account_id', 'N/A')}")
                console.print(f"    Owner: {record.get('mailing_address', {}).get('name', 'N/A')}")
                console.print(f"    Address: {record.get('property_address', {}).get('street_address', 'N/A')}, {record.get('property_address', {}).get('city', 'N/A')}")
                console.print(f"    Market Value: ${record.get('valuation', {}).get('market_value', 'N/A'):,}" if isinstance(record.get('valuation', {}).get('market_value'), (int, float)) else f"    Market Value: {record.get('valuation', {}).get('market_value', 'N/A')}")
                console.print(f"    Tax Entities: {len(record.get('tax_entities', []))}")
                console.print(f"    Improvements: {len(record.get('improvements', []))}")
            
            # Frontend access instructions
            console.print(f"\n[bold green]🚀 Frontend Access:[/bold green]")
            console.print(f"   🌐 Web App: http://localhost:5000")
            console.print(f"   📊 API Endpoint: /api/properties?county=travis&limit={min(sample_size, 1000)}")
            console.print(f"   📈 Stats: /api/stats")
            console.print(f"   🔍 Filter by county: 'travis' in the frontend dropdown")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error during Travis County frontend sample loading: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--sample-size', default=5000, help='Number of random properties to sample and load')
@click.option('--batch-id', help='Custom batch ID for tracking')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.option('--force-reload', is_flag=True, help='Force reload even if data exists')
@click.pass_context
def load_dallas_sample_for_frontend(ctx, sample_size, batch_id, mongo_uri, database, force_reload):
    """Load a random sample of Dallas County properties into MongoDB for frontend review."""
    console = ctx.obj['console']
    
    try:
        from ..models.config import Config
        dallas_config = Config()
        dallas_config.county_type = "dallas"  # Ensure Dallas County mode
        
        console.print(f"[bold blue]🏛️ Dallas County Frontend Sample Loader[/bold blue]")
        console.print(f"🎯 Target: {sample_size:,} random properties for frontend review")
        
        # Check if we already have Dallas County data
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            # Check existing Dallas County data
            existing_dallas_count = mongodb.properties_collection.count_documents({'county': 'dallas'})
            
            if existing_dallas_count > 0 and not force_reload:
                console.print(f"[yellow]⚠️ Found {existing_dallas_count:,} existing Dallas County properties[/yellow]")
                console.print("[yellow]Use --force-reload to replace existing data[/yellow]")
                
                # Show sample of existing data
                sample_existing = list(mongodb.properties_collection.find(
                    {'county': 'dallas'}, 
                    {'account_id': 1, 'property_address': 1, 'valuation': 1}
                ).limit(5))
                
                if sample_existing:
                    console.print(f"\n[blue]📋 Sample of existing data:[/blue]")
                    for prop in sample_existing:
                        addr = prop.get('property_address', {})
                        street = addr.get('street_address', 'N/A')
                        city = addr.get('city', 'N/A')
                        value = prop.get('valuation', {}).get('market_value', 'N/A')
                        console.print(f"  {prop['account_id']}: {street}, {city} - ${value:,}" if isinstance(value, (int, float)) else f"  {prop['account_id']}: {street}, {city} - {value}")
                
                return
            
            # Initialize Dallas normalizer
            normalizer = DallasCountyNormalizer(dallas_config)
            
            # Load and normalize Dallas data
            console.print(f"\n[blue]📊 Loading and normalizing {sample_size:,} Dallas County properties...[/blue]")
            normalized_records = normalizer.load_and_normalize_sample(sample_size)
            
            if not normalized_records:
                console.print("[red]No records were normalized[/red]")
                return
            
            console.print(f"[green]✅ Successfully normalized {len(normalized_records):,} records[/green]")
            
            # Save to MongoDB
            batch_id = batch_id or f"dallas_frontend_{len(normalized_records)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            console.print(f"\n[blue]💾 Saving to MongoDB with batch ID: {batch_id}[/blue]")
            
            result = mongodb.save_properties(
                normalized_records, 
                batch_id=batch_id,
                source_files=['ACCOUNT_INFO.CSV', 'ACCOUNT_APPRL_YEAR.CSV', 'MULTI_OWNER.CSV']
            )
            
            console.print(f"\n[bold green]🎉 Successfully loaded Dallas County sample for frontend![/bold green]")
            console.print(f"📊 Batch ID: {result['batch_id']}")
            console.print(f"💾 Properties: {result['saved_count']:,}")
            console.print(f"📅 Timestamp: {result['timestamp']}")
            
            # Show collection stats
            stats = mongodb.get_collection_stats()
            console.print(f"\n[blue]📈 Database Status:[/blue]")
            console.print(f"   Total properties: {stats['properties_count']:,}")
            console.print(f"   Dallas County: {mongodb.properties_collection.count_documents({'county': 'dallas'}):,}")
            console.print(f"   Processing logs: {stats['logs_count']:,}")
            
            # Data quality analysis for frontend
            console.print(f"\n[bold cyan]📋 Frontend Data Quality Report:[/bold cyan]")
            
            # Sample analysis for key fields
            sample_records = normalized_records[:100]  # Analyze first 100 for speed
            
            # Address coverage
            address_coverage = sum(1 for r in sample_records if r.get('property_address', {}).get('street_address'))
            console.print(f"   📍 Property Addresses: {address_coverage}/{len(sample_records)} ({address_coverage/len(sample_records)*100:.1f}%)")
            
            # Owner coverage
            owner_coverage = sum(1 for r in sample_records if r.get('mailing_address', {}).get('name'))
            console.print(f"   👤 Owner Names: {owner_coverage}/{len(sample_records)} ({owner_coverage/len(sample_records)*100:.1f}%)")
            
            # Valuation coverage
            valuation_coverage = sum(1 for r in sample_records if r.get('valuation', {}).get('market_value'))
            console.print(f"   💰 Market Values: {valuation_coverage}/{len(sample_records)} ({valuation_coverage/len(sample_records)*100:.1f}%)")
            
            # Tax entities coverage
            tax_entities_coverage = sum(1 for r in sample_records if r.get('tax_entities'))
            console.print(f"   🏛️ Tax Entities: {tax_entities_coverage}/{len(sample_records)} ({tax_entities_coverage/len(sample_records)*100:.1f}%)")
            
            # Improvements coverage
            improvements_coverage = sum(1 for r in sample_records if r.get('improvements'))
            console.print(f"   🏗️ Improvements: {improvements_coverage}/{len(sample_records)} ({improvements_coverage/len(sample_records)*100:.1f}%)")
            
            # Sample data preview for frontend
            console.print(f"\n[bold blue]🔍 Frontend Data Preview:[/bold blue]")
            preview_records = normalized_records[:3]
            
            for i, record in enumerate(preview_records, 1):
                console.print(f"\n  [cyan]Record {i}:[/cyan]")
                console.print(f"    Account ID: {record.get('account_id', 'N/A')}")
                console.print(f"    Owner: {record.get('mailing_address', {}).get('name', 'N/A')}")
                console.print(f"    Address: {record.get('property_address', {}).get('street_address', 'N/A')}, {record.get('property_address', {}).get('city', 'N/A')}")
                console.print(f"    Market Value: ${record.get('valuation', {}).get('market_value', 'N/A'):,}" if isinstance(record.get('valuation', {}).get('market_value'), (int, float)) else f"    Market Value: {record.get('valuation', {}).get('market_value', 'N/A')}")
                console.print(f"    Tax Entities: {len(record.get('tax_entities', []))}")
                console.print(f"    Improvements: {len(record.get('improvements', []))}")
            
            # Frontend access instructions
            console.print(f"\n[bold green]🚀 Frontend Access:[/bold green]")
            console.print(f"   🌐 Web App: http://localhost:5000")
            console.print(f"   📊 API Endpoint: /api/properties?county=dallas&limit={min(sample_size, 1000)}")
            console.print(f"   📈 Stats: /api/stats")
            console.print(f"   🔍 Filter by county: 'dallas' in the frontend dropdown")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]Error during Dallas County frontend sample loading: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--travis-size', default=1000, help='Number of Travis County properties to load (default: 1000)')
@click.option('--dallas-size', default=1000, help='Number of Dallas County properties to load (default: 1000)')
@click.option('--harris-size', default=1000, help='Number of Harris County properties to load (default: 1000)')
@click.option('--batch-id', help='Custom batch ID for tracking all counties')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.option('--parallel', is_flag=True, help='Process counties in parallel (future enhancement)')
@click.option('--force-reload', is_flag=True, help='Force reload even if data exists')
@click.pass_context
def load_all_counties_for_frontend(ctx, travis_size, dallas_size, harris_size, batch_id, mongo_uri, database, parallel, force_reload):
    """Load samples from all counties into MongoDB for frontend review. Default: 1000 properties from each county."""
    console = ctx.obj['console']
    
    console.print(f"[bold blue]🏛️ Multi-County Frontend Sample Loader[/bold blue]")
    console.print(f"🎯 Target: {travis_size:,} Travis + {dallas_size:,} Dallas + {harris_size:,} Harris = {travis_size + dallas_size + harris_size:,} total properties")
    
    if parallel:
        console.print("[yellow]⚠️  Parallel processing not yet implemented - processing sequentially[/yellow]")
    
    # Generate unified batch ID
    batch_id = batch_id or f"multi_county_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    console.print(f"📊 Unified Batch ID: {batch_id}")
    
    # Track results for all counties
    results = {}
    total_loaded = 0
    
    try:
        # Test MongoDB connection first
        from ..services import MongoDBService
        mongodb = MongoDBService(mongo_uri=mongo_uri, database=database)
        
        if not mongodb.connect():
            raise click.ClickException("Failed to connect to MongoDB")
        
        try:
            # Process Travis County
            if travis_size > 0:
                console.print(f"\n[bold cyan]🏛️ Processing Travis County ({travis_size:,} properties)[/bold cyan]")
                try:
                    from ..models.config import Config
                    travis_config = Config()
                    travis_config.county_type = "travis"
                    
                    normalizer = TravisCountyNormalizer(travis_config)
                    normalized_records = normalizer.load_and_normalize_sample(travis_size)
                    
                    if normalized_records:
                        result = mongodb.save_properties(
                            normalized_records,
                            batch_id=f"{batch_id}_travis",
                            source_files=['PROP.TXT', 'PROP_ENT.TXT', 'IMP_DET.TXT', 'LAND_DET.TXT', 'AGENT.TXT']
                        )
                        results['travis'] = result
                        total_loaded += result['saved_count']
                        console.print(f"[green]✅ Travis County: {result['saved_count']:,} properties loaded[/green]")
                    else:
                        results['travis'] = {'error': 'No records normalized'}
                        console.print("[red]❌ Travis County: No records normalized[/red]")
                        
                except Exception as e:
                    results['travis'] = {'error': str(e)}
                    console.print(f"[red]❌ Travis County failed: {e}[/red]")
            
            # Process Dallas County
            if dallas_size > 0:
                console.print(f"\n[bold cyan]🏛️ Processing Dallas County ({dallas_size:,} properties)[/bold cyan]")
                try:
                    from ..models.config import Config
                    dallas_config = Config()
                    dallas_config.county_type = "dallas"
                    
                    normalizer = DallasCountyNormalizer(dallas_config)
                    normalized_records = normalizer.load_and_normalize_sample(dallas_size)
                    
                    if normalized_records:
                        result = mongodb.save_properties(
                            normalized_records,
                            batch_id=f"{batch_id}_dallas",
                            source_files=['ACCOUNT_INFO.CSV', 'ACCOUNT_APPRL_YEAR.CSV', 'MULTI_OWNER.CSV']
                        )
                        results['dallas'] = result
                        total_loaded += result['saved_count']
                        console.print(f"[green]✅ Dallas County: {result['saved_count']:,} properties loaded[/green]")
                    else:
                        results['dallas'] = {'error': 'No records normalized'}
                        console.print("[red]❌ Dallas County: No records normalized[/red]")
                        
                except Exception as e:
                    results['dallas'] = {'error': str(e)}
                    console.print(f"[red]❌ Dallas County failed: {e}[/red]")
            
            # Process Harris County
            if harris_size > 0:
                console.print(f"\n[bold cyan]🏛️ Processing Harris County ({harris_size:,} properties)[/bold cyan]")
                try:
                    from ..models.config import Config
                    harris_config = Config()
                    harris_config.county_type = "harris"
                    
                    normalizer = HarrisCountyNormalizer(harris_config)
                    normalized_records = normalizer.load_and_normalize_sample(harris_size)
                    
                    if normalized_records:
                        result = mongodb.save_properties(
                            normalized_records,
                            batch_id=f"{batch_id}_harris",
                            source_files=['real_acct.txt', 'owners.txt', 'deeds.txt']
                        )
                        results['harris'] = result
                        total_loaded += result['saved_count']
                        console.print(f"[bold green]✅ Harris County: {result['saved_count']:,} properties loaded[/bold green]")
                    else:
                        results['harris'] = {'error': 'No records normalized'}
                        console.print("[red]❌ Harris County: No records normalized[/red]")
                        
                except Exception as e:
                    results['harris'] = {'error': str(e)}
                    console.print(f"[red]❌ Harris County failed: {e}[/red]")
            
            # Summary report
            console.print(f"\n" + "=" * 60)
            console.print(f"[bold green]🎉 Multi-County Loading Complete![/bold green]")
            console.print(f"📊 Total Properties Loaded: {total_loaded:,}")
            console.print(f"📅 Batch ID: {batch_id}")
            
            # County-by-county summary
            console.print(f"\n[bold cyan]📋 County Summary:[/bold cyan]")
            for county, result in results.items():
                if 'error' in result:
                    console.print(f"   {county.title()}: ❌ {result['error']}")
                else:
                    console.print(f"   {county.title()}: ✅ {result['saved_count']:,} properties")
            
            # Database status
            stats = mongodb.get_collection_stats()
            console.print(f"\n[blue]📈 Database Status:[/blue]")
            console.print(f"   Total properties: {stats['properties_count']:,}")
            console.print(f"   Travis County: {mongodb.properties_collection.count_documents({'county': 'travis'}):,}")
            console.print(f"   Dallas County: {mongodb.properties_collection.count_documents({'county': 'dallas'}):,}")
            console.print(f"   Harris County: {mongodb.properties_collection.count_documents({'county': 'harris'}):,}")
            
            # Frontend access instructions
            console.print(f"\n[bold green]🚀 Frontend Access:[/bold green]")
            console.print(f"   🌐 Web App: http://localhost:5000")
            console.print(f"   📊 API Endpoint: /api/properties?limit=1000")
            console.print(f"   📈 Stats: /api/stats")
            console.print(f"   🔍 Filter by county in the frontend dropdown")
            
            # Future scalability notes
            if parallel:
                console.print(f"\n[bold yellow]💡 Future Enhancement:[/bold yellow]")
                console.print(f"   Parallel processing will be implemented for faster loading")
                console.print(f"   Estimated time savings: 60-70% with parallel processing")
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        console.print(f"[red]❌ Multi-county loading failed: {e}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--force-reload', is_flag=True, help='Force reload even if data exists')
@click.option('--mongo-uri', help='MongoDB connection URI (overrides environment)')
@click.option('--database', help='MongoDB database name (overrides environment)')
@click.pass_context
def load_frontend_data(ctx, force_reload, mongo_uri, database):
    """Load 1000 properties from each county into MongoDB for frontend review (default command)."""
    # Call the multi-county loader with default values
    ctx.invoke(load_all_counties_for_frontend, 
               travis_size=1000, 
               dallas_size=1000, 
               harris_size=1000,
               force_reload=force_reload,
               mongo_uri=mongo_uri,
               database=database)


if __name__ == "__main__":
    cli()
