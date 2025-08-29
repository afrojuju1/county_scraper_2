"""Data normalization engine for combining all county files."""

import polars as pl
from pathlib import Path
from typing import Dict, Any, List, Optional
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from ..models import Config
from .base import BaseParser


class CountyDataNormalizer(BaseParser):
    """Normalize and combine all county data files into a single dataset."""
    
    def __init__(self, config: Config):
        super().__init__(config)
        self.console = Console()
        
    def get_schema(self) -> Dict[str, Any]:
        """Return combined schema (not used for normalizer)."""
        return {}
        
    def get_file_path(self) -> Path:
        """Return data directory path."""
        return self.config.data_dir
    
    def preprocess_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Not used for normalizer."""
        return df
    
    def normalize_all_files(self, output_path: Optional[Path] = None, 
                          format: str = "json", include_all_fields: bool = True,
                          sample_size: Optional[int] = None, 
                          use_chunking: bool = True) -> Dict[str, Any]:
        """
        Normalize all county files into a single dataset keyed by account number.
        
        Args:
            output_path: Where to save the normalized data
            format: Output format ("json" or "csv")  
            include_all_fields: Include all related data (owners, deeds, permits)
            sample_size: Limit processing to first N records for testing
            use_chunking: Process large files in chunks for memory efficiency
        
        Returns:
            Dictionary with normalization results
        """
        
        self.console.print("[bold green]🏘️  Starting County Data Normalization...[/bold green]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=self.console
        ) as progress:
            
            # Step 1: Load primary property data
            load_task = progress.add_task("Loading primary property data...", total=7)
            
            real_accounts_df = self._load_real_accounts(sample_size, use_chunking)
            progress.update(load_task, advance=1, description=f"✅ Loaded {len(real_accounts_df):,} property records")
            
            # Step 2: Load related data files
            owners_df = self._load_owners() if include_all_fields else None
            progress.update(load_task, advance=1, description="✅ Loaded owner data")
            
            deeds_df = self._load_deeds() if include_all_fields else None
            progress.update(load_task, advance=1, description="✅ Loaded deed records")
            
            permits_df = self._load_permits() if include_all_fields else None
            progress.update(load_task, advance=1, description="✅ Loaded permit data")
            
            tieback_df = self._load_parcel_tieback() if include_all_fields else None
            progress.update(load_task, advance=1, description="✅ Loaded parcel relationships")
            
            neighborhood_df = self._load_neighborhood_codes()
            progress.update(load_task, advance=1, description="✅ Loaded neighborhood codes")
            
            mineral_df = self._load_mineral_rights() if include_all_fields else None
            progress.update(load_task, advance=1, description="✅ Loaded mineral rights")
            
            # Step 3: Normalize and combine
            normalize_task = progress.add_task("Normalizing data...", total=1)
            
            if format.lower() == "json":
                normalized_data = self._create_json_normalized_data(
                    real_accounts_df, owners_df, deeds_df, permits_df, 
                    tieback_df, neighborhood_df, mineral_df
                )
            else:
                normalized_data = self._create_csv_normalized_data(
                    real_accounts_df, owners_df, deeds_df, permits_df,
                    tieback_df, neighborhood_df, mineral_df
                )
            
            progress.update(normalize_task, advance=1, description="✅ Data normalized")
            
            # Step 4: Save results
            if output_path:
                save_task = progress.add_task("Saving normalized data...", total=1)
                self._save_normalized_data(normalized_data, output_path, format)
                progress.update(save_task, advance=1, description=f"✅ Saved to {output_path}")
        
        return {
            "format": format,
            "total_properties": len(real_accounts_df),
            "output_path": str(output_path) if output_path else None,
            "included_data": {
                "owners": owners_df is not None and len(owners_df) > 0,
                "deeds": deeds_df is not None and len(deeds_df) > 0,
                "permits": permits_df is not None and len(permits_df) > 0,
                "parcel_relationships": tieback_df is not None and len(tieback_df) > 0,
                "mineral_rights": mineral_df is not None and len(mineral_df) > 0
            }
        }
    
    def _load_real_accounts(self, sample_size: Optional[int] = None, use_chunking: bool = True) -> pl.DataFrame:
        """Load and clean real accounts data."""
        file_path = self.config.get_file_path(self.config.real_accounts_file)
        
        # Detect delimiter first
        delimiter = self._detect_delimiter(file_path)
        self.console.print(f"📖 Reading file: {file_path.name} (delimiter: '{delimiter}')")
        
        try:
            read_kwargs = {
                "has_header": True,
                "separator": delimiter,
                "null_values": ["", "NULL", "null", "N/A", "n/a"],
                "truncate_ragged_lines": True,
                "ignore_errors": True,
                "infer_schema_length": 0  # Don't infer schema, read all as strings
            }
            
            if sample_size:
                read_kwargs["n_rows"] = sample_size
                
            df = pl.read_csv(file_path, **read_kwargs)
            
        except Exception as e:
            self.console.print(f"[yellow]Polars parsing failed, trying pandas fallback: {str(e)[:100]}...[/yellow]")
            # Fallback: use pandas for messy CSV files
            try:
                import pandas as pd
                
                # Use pandas for robust CSV reading
                pandas_df = pd.read_csv(
                    file_path,
                    sep="\t",
                    nrows=sample_size if sample_size else None,
                    dtype=str,  # Read all as strings
                    na_values=["", "NULL", "null", "N/A", "n/a"],
                    keep_default_na=False,  # Don't convert strings to NaN
                    encoding='utf-8',
                    on_bad_lines='skip'  # Skip problematic lines
                )
                
                # Convert to polars
                df = pl.from_pandas(pandas_df)
                self.console.print(f"✅ Successfully read {len(df):,} rows using pandas fallback")
                
            except Exception as e2:
                self.console.print(f"[red]Both polars and pandas failed: {e2}[/red]")
                raise
    
        # Clean up the dataframe after loading (only if we have one)
        if df is not None:
            df = self._clean_dataframe(df)
        
        return df
    
    def _detect_delimiter(self, file_path: Path) -> str:
        """Detect the delimiter used in the file."""
        
        # Read first few lines to detect delimiter
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            sample = f.read(5000)  # Read first 5KB
            
        # Count common delimiters in first few lines
        lines = sample.split('\n')[:5]  # Check first 5 lines
        delimiters = {',': 0, '\t': 0, '|': 0, ';': 0}
        
        for line in lines:
            if len(line.strip()) > 0:  # Skip empty lines
                for delim in delimiters:
                    delimiters[delim] += line.count(delim)
        
        # Return most common delimiter (default to tab if tie)
        best_delim = max(delimiters, key=delimiters.get)
        return best_delim if delimiters[best_delim] > 0 else '\t'
        
        # Clean up the dataframe after loading (only if we have one)
        if df is not None:
            df = self._clean_dataframe(df)
        
        return df
    
    def _clean_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean up the loaded dataframe."""
        
        # Clean up quoted fields
        string_columns = [col for col, dtype in df.schema.items() if dtype == pl.Utf8]
        for col in string_columns:
            df = df.with_columns(
                pl.col(col).str.strip_chars('"').alias(col)
            )
        
        return df
    
    def _load_owners(self) -> pl.DataFrame:
        """Load owners data.""" 
        file_path = self.config.get_file_path(self.config.owners_file)
        
        try:
            return pl.read_csv(file_path, separator="\t", has_header=True, ignore_errors=True)
        except Exception as e:
            self.console.print(f"[yellow]Using pandas fallback for owners.txt: {str(e)[:50]}...[/yellow]")
            import pandas as pd
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    pandas_df = pd.read_csv(
                        file_path, 
                        sep="\t", 
                        dtype=str, 
                        on_bad_lines='skip',
                        encoding=encoding,
                        encoding_errors='ignore'
                    )
                    self.console.print(f"✅ Successfully loaded with {encoding} encoding")
                    return pl.from_pandas(pandas_df)
                except Exception:
                    continue
            raise Exception("Could not load file with any encoding")
    
    def _load_deeds(self) -> pl.DataFrame:
        """Load deeds data.""" 
        file_path = self.config.get_file_path(self.config.deeds_file)
        return pl.read_csv(file_path, separator="\t", has_header=True)
    
    def _load_permits(self) -> pl.DataFrame:
        """Load permits data."""
        file_path = self.config.get_file_path(self.config.permits_file)
        return pl.read_csv(file_path, separator="\t", has_header=True)
    
    def _load_parcel_tieback(self) -> pl.DataFrame:
        """Load parcel tieback relationships."""
        file_path = self.config.get_file_path(self.config.parcel_tieback_file)
        return pl.read_csv(file_path, separator="\t", has_header=True)
    
    def _load_neighborhood_codes(self) -> pl.DataFrame:
        """Load neighborhood code lookup."""
        file_path = self.config.get_file_path("real_neighborhood_code.txt")
        return pl.read_csv(file_path, separator="\t", has_header=True)
    
    def _load_mineral_rights(self) -> pl.DataFrame:
        """Load mineral rights data."""
        file_path = self.config.get_file_path("real_mnrl.txt")
        return pl.read_csv(file_path, separator="\t", has_header=True)
    
    def _create_json_normalized_data(self, real_accounts_df, owners_df, deeds_df, 
                                   permits_df, tieback_df, neighborhood_df, mineral_df) -> List[Dict]:
        """Create JSON-structured normalized data."""
        
        normalized_records = []
        
        # Create efficient lookups for related data
        owners_dict = {}
        if owners_df is not None:
            for row in owners_df.iter_rows(named=True):
                acct = row["acct"]
                if acct not in owners_dict:
                    owners_dict[acct] = []
                owners_dict[acct].append({
                    "line_number": row["ln_num"],
                    "name": row["name"],
                    "aka": row.get("aka"),
                    "ownership_percentage": row.get("pct_own")
                })
        
        deeds_dict = {}
        if deeds_df is not None:
            for row in deeds_df.iter_rows(named=True):
                acct = row["acct"]
                if acct not in deeds_dict:
                    deeds_dict[acct] = []
                deeds_dict[acct].append({
                    "date_of_sale": row.get("dos"),
                    "clerk_year": row.get("clerk_yr"),
                    "clerk_id": row.get("clerk_id"),
                    "deed_id": row.get("deed_id")
                })
        
        permits_dict = {}
        if permits_df is not None:
            for row in permits_df.iter_rows(named=True):
                acct = row["acct"]
                if acct not in permits_dict:
                    permits_dict[acct] = []
                permits_dict[acct].append({
                    "permit_id": row.get("id"),
                    "agency_id": row.get("agency_id"),
                    "status": row.get("status"),
                    "description": row.get("dscr"),
                    "permit_type": row.get("permit_type"),
                    "permit_type_description": row.get("permit_tp_descr"),
                    "issue_date": row.get("issue_date"),
                    "year": row.get("yr")
                })
        
        mineral_dict = {}
        if mineral_df is not None:
            for row in mineral_df.iter_rows(named=True):
                acct = row["acct"]
                mineral_dict[acct] = {
                    "dor_code": row.get("dor_cd"),
                    "rail_lease_num": row.get("Rail_leasenum"),
                    "interest_type": row.get("Type_Interest"),
                    "interest_percent": row.get("Interest_Percent")
                }
        
        # Create neighborhood lookup
        neighborhood_lookup = {}
        if neighborhood_df is not None:
            for row in neighborhood_df.iter_rows(named=True):
                neighborhood_lookup[row["cd"]] = {
                    "group_code": row.get("grp_cd"),
                    "description": row.get("dscr")
                }
        
        self.console.print(f"Processing {len(real_accounts_df):,} property records...")
        
        for row in real_accounts_df.iter_rows(named=True):
            account_id = row["acct"]
            
            # Base property record
            property_record = {
                "account_id": account_id,
                "year": row.get("yr"),
                "property_address": {
                    "street_number": row.get("str_num"),
                    "street_prefix": row.get("str_pfx"),
                    "street_name": row.get("str"),
                    "street_suffix": row.get("str_sfx"),
                    "street_direction": row.get("str_sfx_dir"),
                    "unit": row.get("str_unit"),
                    "full_address": row.get("site_addr_1"),
                    "city": row.get("site_addr_2"),
                    "zip_code": row.get("site_addr_3")
                },
                "mailing_address": {
                    "name": row.get("mailto"),
                    "address_1": row.get("mail_addr_1"),
                    "address_2": row.get("mail_addr_2"),
                    "city": row.get("mail_city"),
                    "state": row.get("mail_state"),
                    "zip": row.get("mail_zip"),
                    "country": row.get("mail_country")
                },
                "property_details": {
                    "year_improved": row.get("yr_impr"),
                    "building_area": row.get("bld_ar"),
                    "land_area": row.get("land_ar"),
                    "acreage": row.get("acreage"),
                    "school_district": row.get("school_dist"),
                    "market_area_1": row.get("Market_Area_1"),
                    "market_area_1_description": row.get("Market_Area_1_Dscr"),
                    "market_area_2": row.get("Market_Area_2"),
                    "market_area_2_description": row.get("Market_Area_2_Dscr")
                },
                "valuation": {
                    "land_value": row.get("land_val"),
                    "building_value": row.get("bld_val"),
                    "extra_features_value": row.get("x_features_val"),
                    "agricultural_value": row.get("ag_val"),
                    "assessed_value": row.get("assessed_val"),
                    "total_appraised_value": row.get("tot_appr_val"),
                    "total_market_value": row.get("tot_mkt_val"),
                    "prior_values": {
                        "land": row.get("prior_land_val"),
                        "building": row.get("prior_bld_val"),
                        "extra_features": row.get("prior_x_features_val"),
                        "agricultural": row.get("prior_ag_val"),
                        "total_appraised": row.get("prior_tot_appr_val"),
                        "total_market": row.get("prior_tot_mkt_val")
                    }
                },
                "legal_status": {
                    "value_status": row.get("value_status"),
                    "noticed": row.get("noticed") == "Y",
                    "notice_date": row.get("notice_dt"),
                    "protested": row.get("protested") == "Y",
                    "new_owner_date": row.get("new_own_dt"),
                    "legal_description": [
                        row.get("lgl_1"), row.get("lgl_2"), 
                        row.get("lgl_3"), row.get("lgl_4")
                    ],
                    "jurisdictions": row.get("jurs")
                }
            }
            
            # Add related data arrays using efficient lookups
            if account_id in owners_dict:
                property_record["owners"] = owners_dict[account_id]
            
            if account_id in deeds_dict:
                property_record["deeds"] = deeds_dict[account_id]
            
            if account_id in permits_dict:
                property_record["permits"] = permits_dict[account_id]
            
            # Add neighborhood info
            market_area_1 = row.get("Market_Area_1")
            if market_area_1 and market_area_1 in neighborhood_lookup:
                property_record["neighborhood"] = neighborhood_lookup[market_area_1]
            
            # Add mineral rights if available
            if account_id in mineral_dict:
                property_record["mineral_rights"] = mineral_dict[account_id]
            
            normalized_records.append(property_record)
        
        return normalized_records
    
    def _create_csv_normalized_data(self, real_accounts_df, owners_df, deeds_df, 
                                  permits_df, tieback_df, neighborhood_df, mineral_df) -> pl.DataFrame:
        """Create flattened CSV-structured data with primary owner info."""
        
        # Start with real accounts as base
        base_df = real_accounts_df
        
        # Join with primary owner (line_number = 1)
        if owners_df is not None:
            primary_owners = owners_df.filter(pl.col("ln_num") == 1).select([
                "acct", "name", "aka", "pct_own"
            ]).rename({
                "name": "primary_owner_name",
                "aka": "primary_owner_aka", 
                "pct_own": "primary_ownership_pct"
            })
            
            base_df = base_df.join(primary_owners, on="acct", how="left")
        
        # Add neighborhood descriptions
        if neighborhood_df is not None:
            neighborhood_lookup = neighborhood_df.rename({
                "cd": "Market_Area_1",
                "dscr": "neighborhood_description",
                "grp_cd": "neighborhood_group"
            }).with_columns([
                pl.col("Market_Area_1").cast(pl.Utf8)  # Ensure string type for joining
            ])
            base_df = base_df.with_columns([
                pl.col("Market_Area_1").cast(pl.Utf8)  # Ensure string type for joining
            ]).join(neighborhood_lookup, on="Market_Area_1", how="left")
        
        # Add mineral rights flag
        if mineral_df is not None:
            mineral_accounts = mineral_df.select("acct").unique().with_columns(
                pl.lit(True).alias("has_mineral_rights")
            )
            base_df = base_df.join(mineral_accounts, on="acct", how="left").with_columns(
                pl.col("has_mineral_rights").fill_null(False)
            )
        
        # Add counts of related records
        if owners_df is not None:
            owner_counts = owners_df.group_by("acct").agg(
                pl.count().alias("total_owners")
            )
            base_df = base_df.join(owner_counts, on="acct", how="left")
        
        if deeds_df is not None:
            deed_counts = deeds_df.group_by("acct").agg(
                pl.count().alias("total_deeds")
            )
            base_df = base_df.join(deed_counts, on="acct", how="left")
        
        if permits_df is not None:
            permit_counts = permits_df.group_by("acct").agg(
                pl.count().alias("total_permits")
            )
            base_df = base_df.join(permit_counts, on="acct", how="left")
        
        return base_df
    
    def _save_normalized_data(self, data, output_path: Path, format: str):
        """Save normalized data in specified format."""
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format.lower() == "json":
            import json
            with open(output_path.with_suffix('.json'), 'w') as f:
                json.dump(data, f, indent=2, default=str)
        
        elif format.lower() == "csv":
            # data is a polars DataFrame for CSV
            data.write_csv(output_path.with_suffix('.csv'))
        
        else:
            raise ValueError(f"Unsupported format: {format}")
