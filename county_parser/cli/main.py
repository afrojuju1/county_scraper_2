"""Main CLI interface for county data parsing."""

import click
from pathlib import Path
from rich.console import Console
from rich.table import Table

from ..models import Config
from ..parsers import RealAccountsParser, OwnersParser, CountyDataNormalizer
from ..parsers.travis_normalizer import TravisCountyNormalizer
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
                
            normalizer = CountyDataNormalizer(config)
            
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
        normalizer = CountyDataNormalizer(config)
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
                normalizer = CountyDataNormalizer(config)
                
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
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def clean_mongodb(ctx, mongo_uri, database, confirm):
    """Clean/reset MongoDB collections (properties and processing_logs)."""
    console = ctx.obj['console']
    
    if not confirm:
        console.print("[yellow]⚠️  This will DELETE ALL data in the MongoDB collections![/yellow]")
        console.print("   - Properties collection")
        console.print("   - Processing logs collection")
        
        if not click.confirm("Are you sure you want to continue?"):
            console.print("[blue]Operation cancelled.[/blue]")
            return
    
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
        harris_normalizer = CountyDataNormalizer(harris_config)
        
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


if __name__ == "__main__":
    cli()
