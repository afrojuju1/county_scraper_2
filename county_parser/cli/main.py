"""Main CLI interface for county data parsing."""

import click
from pathlib import Path
from rich.console import Console
from rich.table import Table

from ..models import Config
from ..parsers import RealAccountsParser, OwnersParser, CountyDataNormalizer
from ..utils.data_validator import DataQualityValidator


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
@click.option('--format', 'output_format', type=click.Choice(['json', 'csv']), 
              default='json', help='Output format for normalized data')
@click.option('--output', '-o', type=click.Path(), required=True, help='Output file path')
@click.option('--include-related/--basic-only', default=True, 
              help='Include related data (owners, deeds, permits) or basic property info only')
@click.option('--sample-size', type=int, help='Process only first N records for testing')
@click.pass_context
def normalize_all(ctx, output_format, output, include_related, sample_size):
    """Normalize and combine all county data files into a single dataset."""
    
    config = ctx.obj['config']
    console = ctx.obj['console']
    
    # Override config with CLI options
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
        
        # Display results summary
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
