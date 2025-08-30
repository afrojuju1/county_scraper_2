"""Harris County data normalization engine for combining all Harris County files."""

import polars as pl
from pathlib import Path
from typing import Dict, Any, List, Optional
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from ..models import Config
from .base import BaseParser


class HarrisCountyNormalizer(BaseParser):
    """Normalize and combine all Harris County data files into a single dataset."""
    
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
    
    def load_and_normalize_sample(self, sample_size: int) -> List[Dict[str, Any]]:
        """
        Load and normalize a sample of Harris County properties for frontend use.
        
        Args:
            sample_size: Number of properties to load
            
        Returns:
            List of normalized property records
        """
        try:
            # Load primary property data with sample size limit
            real_accounts_df = self._load_real_accounts(sample_size, use_chunking=True)
            
            if len(real_accounts_df) == 0:
                self.console.print("[red]No property records found[/red]")
                return []
            
            # Load related data files
            owners_df = self._load_owners()
            deeds_df = self._load_deeds()
            permits_df = self._load_permits()
            tieback_df = self._load_parcel_tieback()
            neighborhood_df = self._load_neighborhood_codes()
            mineral_df = self._load_mineral_rights()
            
            # Normalize to unified format
            normalized_data = self._create_json_normalized_data(
                real_accounts_df, owners_df, deeds_df, permits_df, 
                tieback_df, neighborhood_df, mineral_df
            )
            
            # Convert to list format for MongoDB
            if isinstance(normalized_data, dict):
                # Extract the properties list from the normalized data
                properties = normalized_data.get('properties', [])
                if isinstance(properties, dict):
                    # If it's a dict keyed by account_id, convert to list
                    properties = list(properties.values())
                return properties
            elif isinstance(normalized_data, list):
                return normalized_data
            else:
                self.console.print(f"[yellow]Unexpected normalized data format: {type(normalized_data)}[/yellow]")
                return []
                
        except Exception as e:
            self.console.print(f"[red]Error loading Harris County sample: {e}[/red]")
            return []
    
    def _load_real_accounts(self, sample_size: Optional[int] = None, use_chunking: bool = True) -> pl.DataFrame:
        """Load and clean real accounts data with specialized line-ending handling."""
        file_path = self.config.get_file_path(self.config.real_accounts_file)
        return self._load_real_accounts_specialized(file_path, sample_size)
    
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
        """Load owners data with specialized CRLF and encoding handling."""
        file_path = self.config.get_file_path(self.config.owners_file)
        return self._load_owners_specialized(file_path)
    
    def _load_deeds(self) -> pl.DataFrame:
        """Load deeds data.""" 
        file_path = self.config.get_file_path(self.config.deeds_file)
        return self._robust_csv_load(file_path, "deeds.txt")
    
    def _load_permits(self) -> pl.DataFrame:
        """Load permits data with special handling for unescaped quotes."""
        file_path = self.config.get_file_path(self.config.permits_file)
        return self._load_permits_specialized(file_path)
    
    def _load_parcel_tieback(self) -> pl.DataFrame:
        """Load parcel tieback relationships."""
        file_path = self.config.get_file_path(self.config.parcel_tieback_file)
        return self._robust_csv_load(file_path, "parcel_tieback.txt")
    
    def _load_neighborhood_codes(self) -> pl.DataFrame:
        """Load neighborhood code lookup."""
        file_path = self.config.get_file_path("real_neighborhood_code.txt")
        return self._robust_csv_load(file_path, "real_neighborhood_code.txt")
    
    def _load_mineral_rights(self) -> pl.DataFrame:
        """Load mineral rights data."""
        file_path = self.config.get_file_path("real_mnrl.txt")
        return self._robust_csv_load(file_path, "real_mnrl.txt")
    
    def _robust_csv_load(self, file_path: Path, filename: str, sample_size: Optional[int] = None) -> pl.DataFrame:
        """Robust CSV loading with comprehensive error handling and data quality validation."""
        
        original_file_size = file_path.stat().st_size / (1024 * 1024)  # Size in MB
        
        try:
            # First try polars with strict settings
            df = pl.read_csv(file_path, separator="\t", has_header=True, ignore_errors=True)
            self.console.print(f"✅ {filename}: {len(df):,} rows loaded with polars")
            return df
            
        except Exception as e:
            self.console.print(f"[yellow]Polars failed for {filename}, using robust pandas parsing...[/yellow]")
            import pandas as pd
            
            # Get expected row count from file
            with open(file_path, 'rb') as f:
                line_count = sum(1 for _ in f) - 1  # Subtract header
                
            self.console.print(f"📊 {filename}: {original_file_size:.1f}MB, ~{line_count:,} expected rows")
            
            best_df = None
            best_row_count = 0
            parsing_method = "none"
            
            # Advanced parsing strategies to handle embedded newlines and malformed data
            strategies = [
                {
                    "name": "Multi-line aware parsing",
                    "params": {
                        "sep": "\t", "dtype": str, "encoding": "utf-8",
                        "quoting": 1, "doublequote": True, "skipinitialspace": True,
                        "on_bad_lines": "warn", "encoding_errors": "replace",
                        "nrows": sample_size, "engine": "python"  # Python engine handles complex cases better
                    }
                },
                {
                    "name": "Fixed-width fallback", 
                    "params": {
                        "sep": "\t", "dtype": str, "encoding": "utf-8",
                        "quoting": 3, "on_bad_lines": "skip", "engine": "python",
                        "encoding_errors": "replace", "nrows": sample_size,
                        "skip_blank_lines": True, "comment": None
                    }
                },
                {
                    "name": "Robust with line cleaning",
                    "params": {
                        "sep": "\t", "dtype": str, "encoding": "latin-1", 
                        "quoting": 3, "on_bad_lines": "skip", "engine": "c",
                        "nrows": sample_size, "low_memory": False
                    }
                },
                {
                    "name": "Minimal parsing (last resort)",
                    "params": {
                        "sep": "\t", "dtype": str, "encoding": "cp1252",
                        "quoting": 3, "on_bad_lines": "skip", "engine": "python",
                        "nrows": sample_size, "error_bad_lines": False,
                        "warn_bad_lines": False  # Suppress warnings for this final attempt
                    }
                }
            ]
            
            for strategy in strategies:
                try:
                    df = pd.read_csv(file_path, **strategy["params"])
                    
                    # Check data quality
                    row_count = len(df)
                    completeness = (line_count - row_count) / line_count if line_count > 0 else 0
                    
                    if row_count > best_row_count:
                        best_df = df
                        best_row_count = row_count
                        parsing_method = strategy["name"]
                    
                    # Report quality metrics
                    quality_icon = "✅" if completeness < 0.05 else "⚠️" if completeness < 0.15 else "❌"
                    self.console.print(f"{quality_icon} {strategy['name']}: {row_count:,} rows ({completeness*100:.1f}% data loss)")
                    
                    # If we got >95% of expected rows, use this method
                    if completeness < 0.05:
                        break
                        
                except Exception as e:
                    self.console.print(f"❌ {strategy['name']}: Failed - {str(e)[:50]}...")
                    continue
            
            if best_df is None:
                raise Exception(f"All parsing strategies failed for {filename}")
            
            # Final data quality report
            final_completeness = (line_count - best_row_count) / line_count if line_count > 0 else 0
            quality_score = "🟢 Excellent" if final_completeness < 0.02 else "🟡 Good" if final_completeness < 0.10 else "🔴 Poor"
            
            self.console.print(f"📋 {filename} Quality Report:")
            self.console.print(f"   Method: {parsing_method}")
            self.console.print(f"   Rows: {best_row_count:,} of {line_count:,} expected ({final_completeness*100:.1f}% loss)")
            self.console.print(f"   Quality: {quality_score}")
            
            # Add metadata to track data quality (use setattr to avoid pandas warning)
            setattr(best_df, '_parsing_metadata', {
                "filename": filename,
                "method": parsing_method,
                "expected_rows": line_count,
                "actual_rows": best_row_count,
                "data_loss_pct": final_completeness * 100,
                "file_size_mb": original_file_size
            })
            
            return pl.from_pandas(best_df)
    
    def _load_permits_specialized(self, file_path: Path) -> pl.DataFrame:
        """Specialized permits loader that handles unescaped quotes correctly."""
        
        self.console.print(f"🔧 Loading permits.txt with specialized quote handling...")
        
        import pandas as pd
        
        try:
            # Strategy: Use QUOTE_NONE and handle the description field specially
            df = pd.read_csv(
                file_path,
                sep='\t',
                dtype=str,
                encoding='utf-8',
                quoting=3,  # QUOTE_NONE - ignore all quotes
                on_bad_lines='skip',
                encoding_errors='replace',
                engine='python'  # Python engine handles edge cases better
            )
            
            self.console.print(f"✅ Successfully loaded {len(df):,} permit records")
            
            # Clean up any residual quote issues in the description field
            if 'dscr' in df.columns:
                # Remove any stray quotes at the beginning or end of descriptions
                df['dscr'] = df['dscr'].str.strip().str.strip('"').str.strip()
                
            return pl.from_pandas(df)
            
        except Exception as e:
            self.console.print(f"❌ Specialized permits parser failed: {e}")
            
            # Final fallback: Read raw lines and parse manually
            self.console.print("🔧 Using manual line parsing as final fallback...")
            return self._parse_permits_manually(file_path)
    
    def _parse_permits_manually(self, file_path: Path) -> pl.DataFrame:
        """Manual line-by-line parsing for severely malformed permits data."""
        
        import pandas as pd
        
        records = []
        skipped_lines = 0
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Read header
            header_line = f.readline().strip()
            headers = header_line.split('\t')
            expected_cols = len(headers)
            
            self.console.print(f"📋 Expected {expected_cols} columns: {headers[:5]}...")
            
            # Process lines manually
            for line_num, line in enumerate(f, start=2):
                try:
                    # Split on tabs
                    fields = line.strip().split('\t')
                    
                    # Handle cases where we have the right number of fields
                    if len(fields) == expected_cols:
                        records.append(fields)
                    elif len(fields) == expected_cols - 1:
                        # Missing last field, add empty string
                        fields.append('')
                        records.append(fields)
                    else:
                        # Skip malformed lines
                        skipped_lines += 1
                        continue
                        
                except Exception:
                    skipped_lines += 1
                    continue
                    
                # Limit for testing
                if len(records) >= 100000:  # Process first 100k records
                    break
        
        if not records:
            raise Exception("No valid records found in permits file")
            
        # Create DataFrame
        df = pd.DataFrame(records, columns=headers)
        
        self.console.print(f"✅ Manual parsing: {len(df):,} records, {skipped_lines} skipped lines")
        
        return pl.from_pandas(df)
    
    def _load_real_accounts_specialized(self, file_path: Path, sample_size: Optional[int] = None) -> pl.DataFrame:
        """Specialized real accounts loader that handles mixed line endings and chunking issues."""
        
        self.console.print(f"🔧 Loading real_acct.txt with specialized line-ending handling...")
        
        import pandas as pd
        import tempfile
        import os
        
        # Pre-process the file to normalize line endings
        temp_file = None
        try:
            # Create a temporary file with normalized line endings
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_f:
                temp_file = temp_f.name
                
                self.console.print(f"🔄 Preprocessing file to normalize line endings...")
                
                line_count = 0
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        # Normalize line endings: remove \r and ensure single \n
                        clean_line = line.replace('\r\n', '\n').replace('\r', '\n').rstrip('\n')
                        temp_f.write(clean_line + '\n')
                        line_count += 1
                        
                        # Stop if we have enough for sampling
                        if sample_size and line_count > sample_size:
                            break
            
            self.console.print(f"✅ Preprocessed {line_count:,} lines")
            
            # Now use pandas to read the cleaned file
            try:
                df = pd.read_csv(
                    temp_file,
                    sep='\t',
                    dtype=str,
                    encoding='utf-8',
                    on_bad_lines='skip',
                    engine='python',  # Better handling of edge cases
                    nrows=sample_size
                )
                
                self.console.print(f"✅ Successfully loaded {len(df):,} real account records")
                
                return pl.from_pandas(df)
                
            except Exception as e:
                self.console.print(f"❌ Pandas failed on preprocessed file: {e}")
                
                # Final fallback: manual line parsing
                return self._parse_real_accounts_manually(temp_file, sample_size)
        
        finally:
            # Clean up temp file
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def _parse_real_accounts_manually(self, file_path: str, sample_size: Optional[int] = None) -> pl.DataFrame:
        """Manual parsing for real accounts when all else fails."""
        
        import pandas as pd
        
        self.console.print("🔧 Using manual parsing for real accounts...")
        
        records = []
        skipped_lines = 0
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Read header
            header_line = f.readline().strip()
            headers = header_line.split('\t')
            expected_cols = len(headers)
            
            self.console.print(f"📋 Expected {expected_cols} columns")
            
            # Process lines
            for line_num, line in enumerate(f, start=2):
                try:
                    fields = line.strip().split('\t')
                    
                    if len(fields) == expected_cols:
                        records.append(fields)
                    elif len(fields) == expected_cols - 1:
                        # Add empty last field
                        fields.append('')
                        records.append(fields)
                    elif len(fields) == expected_cols + 1 and fields[-1] == '':
                        # Remove empty last field
                        records.append(fields[:-1])
                    else:
                        skipped_lines += 1
                        continue
                        
                except Exception:
                    skipped_lines += 1
                    continue
                    
                # Stop if we have enough records
                if sample_size and len(records) >= sample_size:
                    break
        
        if not records:
            raise Exception("No valid records found in real accounts file")
        
        df = pd.DataFrame(records, columns=headers)
        
        self.console.print(f"✅ Manual parsing: {len(df):,} records, {skipped_lines} skipped lines")
        
        return pl.from_pandas(df)
    
    def _load_owners_specialized(self, file_path: Path) -> pl.DataFrame:
        """Specialized owners loader that handles CRLF line endings and encoding issues."""
        
        self.console.print(f"🔧 Loading owners.txt with specialized CRLF and encoding handling...")
        
        import pandas as pd
        
        # Try multiple encoding strategies for owners.txt  
        encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings_to_try:
            try:
                self.console.print(f"🔄 Trying encoding: {encoding}")
                
                df = pd.read_csv(
                    file_path,
                    sep='\t',
                    dtype=str,
                    encoding=encoding,
                    on_bad_lines='skip',
                    engine='python',  # Better for handling CRLF
                    lineterminator=None,  # Let pandas auto-detect CRLF vs LF
                    encoding_errors='replace'  # Replace problematic characters
                )
                
                self.console.print(f"✅ Successfully loaded {len(df):,} owner records with {encoding} encoding")
                
                # Clean up any encoding artifacts in name fields
                if 'name' in df.columns:
                    # Remove any residual encoding artifacts
                    df['name'] = df['name'].str.replace('�', ' ', regex=False)  # Replace replacement character
                    df['name'] = df['name'].str.strip()
                
                return pl.from_pandas(df)
                
            except UnicodeDecodeError as e:
                self.console.print(f"❌ {encoding} encoding failed: {str(e)[:50]}...")
                continue
            except Exception as e:
                self.console.print(f"❌ {encoding} parsing failed: {str(e)[:50]}...")
                continue
        
        # If all encodings fail, try binary mode preprocessing
        self.console.print("🔧 All encodings failed, trying binary preprocessing...")
        return self._preprocess_owners_binary(file_path)
    
    def _preprocess_owners_binary(self, file_path: Path) -> pl.DataFrame:
        """Preprocess owners.txt in binary mode to handle encoding issues."""
        
        import pandas as pd
        import tempfile
        import os
        
        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', encoding='utf-8') as temp_f:
                temp_file = temp_f.name
                
                self.console.print("🔄 Preprocessing owners.txt to fix encoding issues...")
                
                line_count = 0
                with open(file_path, 'rb') as f:
                    for line_bytes in f:
                        try:
                            # Try utf-8 first, then latin1 as fallback
                            try:
                                line = line_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                line = line_bytes.decode('latin1', errors='replace')
                            
                            # Normalize line endings and clean up
                            clean_line = line.replace('\r\n', '\n').replace('\r', '\n').strip()
                            
                            # Skip empty lines
                            if clean_line:
                                temp_f.write(clean_line + '\n')
                                line_count += 1
                                
                        except Exception:
                            # Skip completely problematic lines
                            continue
            
            self.console.print(f"✅ Preprocessed {line_count:,} lines")
            
            # Now read the cleaned file
            df = pd.read_csv(
                temp_file,
                sep='\t',
                dtype=str,
                encoding='utf-8',
                on_bad_lines='skip',
                engine='python'
            )
            
            self.console.print(f"✅ Final result: {len(df):,} owner records")
            
            return pl.from_pandas(df)
            
        finally:
            # Clean up temp file
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def _create_json_normalized_data(self, real_accounts_df, owners_df, deeds_df, 
                                   permits_df, tieback_df, neighborhood_df, mineral_df) -> List[Dict]:
        """Create JSON-structured normalized data."""
        
        normalized_records = []
        
        # Create efficient lookups for related data with normalized account IDs
        def normalize_account_id(acct_raw) -> str:
            """Normalize account ID to consistent 13-digit format with leading zeros."""
            acct = str(acct_raw).strip()
            
            # Remove any non-digit characters and pad with leading zeros to 13 digits
            digits_only = ''.join(c for c in acct if c.isdigit())
            normalized = digits_only.zfill(13)  # Pad to 13 digits with leading zeros
            
            return normalized
        
        owners_dict = {}
        if owners_df is not None:
            for row in owners_df.iter_rows(named=True):
                acct = normalize_account_id(row["acct"])
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
                acct = normalize_account_id(row["acct"])  # This will fix the missing leading zeros!
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
                acct = normalize_account_id(row["acct"])
                if acct not in permits_dict:
                    permits_dict[acct] = []
                permits_dict[acct].append({
                    "permit_id": row.get("id"),
                    "agency_id": row.get("agency_id"),
                    "status": row.get("status"),
                    "description": row.get("dscr"),
                    "dor_code": row.get("dor_cd"),
                    "permit_type": row.get("permit_type"),
                    "permit_type_description": row.get("permit_tp_descr"),
                    "property_type": row.get("property_tp"),
                    "issue_date": row.get("issue_date"),
                    "year": row.get("yr"),
                    "site_address": {
                        "site_number": row.get("site_num"),
                        "site_prefix": row.get("site_pfx"),
                        "site_street": row.get("site_str"),
                        "site_type": row.get("site_tp"),
                        "site_suffix": row.get("site_sfx"),
                        "site_apartment": row.get("site_apt")
                    }
                })
        
        mineral_dict = {}
        if mineral_df is not None:
            for row in mineral_df.iter_rows(named=True):
                acct = normalize_account_id(row["acct"])
                mineral_dict[acct] = {
                    "dor_code": row.get("dor_cd"),
                    "rail_lease_num": row.get("Rail_leasenum"),
                    "interest_type": row.get("Type_Interest"),
                    "interest_percent": row.get("Interest_Percent")
                }
        
        # Create parcel tieback lookup
        tieback_dict = {}
        if tieback_df is not None:
            for row in tieback_df.iter_rows(named=True):
                acct = normalize_account_id(row["acct"])
                if acct not in tieback_dict:
                    tieback_dict[acct] = []
                tieback_dict[acct].append({
                    "account_id": row.get("acct"),
                    "type": row.get("tp"),
                    "description": row.get("dscr"),
                    "related_account_id": row.get("related_acct"),
                    "percentage": row.get("pct")
                })
        
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
            account_id = normalize_account_id(row["acct"])  # Normalize for consistent joining
            
            # Base property record
            property_record = {
                "account_id": account_id,
                "county": "harris",
                "year": row.get("yr"),
                "property_address": {
                    "street_number": row.get("str_num"),
                    "street_prefix": row.get("str_pfx"),
                    "street_name": row.get("str"),
                    "street_suffix": row.get("str_sfx"),
                    "street_direction": row.get("str_sfx_dir"),
                    "unit": row.get("str_unit"),
                    "street_address": row.get("site_addr_1"),  # Fixed: changed from full_address to street_address
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
                    "year_annexed": row.get("yr_annexed"),
                    "building_area": row.get("bld_ar"),
                    "land_area": row.get("land_ar"),
                    "acreage": row.get("acreage"),
                    "school_district": row.get("school_dist"),
                    "state_class": row.get("state_class"),
                    "economic_area": row.get("econ_area"),
                    "economic_building_class": row.get("econ_bld_class"),
                    "center_code": row.get("center_code"),
                    "market_area_1": row.get("Market_Area_1"),
                    "market_area_1_description": row.get("Market_Area_1_Dscr"),
                    "market_area_2": row.get("Market_Area_2"),
                    "market_area_2_description": row.get("Market_Area_2_Dscr"),
                    "neighborhood_code": row.get("Neighborhood_Code"),
                    "neighborhood_group": row.get("Neighborhood_Grp")
                },
                "valuation": {
                    "land_value": row.get("land_val"),
                    "building_value": row.get("bld_val"),
                    "extra_features_value": row.get("x_features_val"),
                    "agricultural_value": row.get("ag_val"),
                    "assessed_value": row.get("assessed_val"),
                    "total_appraised_value": row.get("tot_appr_val"),
                    "market_value": row.get("tot_mkt_val"),  # Fixed: changed from total_market_value to market_value
                    "total_market_value": row.get("tot_mkt_val"),  # Keep both for compatibility
                    "new_construction_value": row.get("new_construction_val"),
                    "total_rcn_value": row.get("tot_rcn_val"),
                    "prior_values": {
                        "land": row.get("prior_land_val"),
                        "building": row.get("prior_bld_val"),
                        "extra_features": row.get("prior_x_features_val"),
                        "agricultural": row.get("prior_ag_val"),
                        "total_appraised": row.get("prior_tot_appr_val"),
                        "total_market": row.get("prior_tot_mkt_val")
                    },
                    "value_changes": {
                        "land_change": self._calculate_value_change(row.get("land_val"), row.get("prior_land_val")),
                        "building_change": self._calculate_value_change(row.get("bld_val"), row.get("prior_bld_val")),
                        "total_market_change": self._calculate_value_change(row.get("tot_mkt_val"), row.get("prior_tot_mkt_val"))
                    }
                },
                "legal_status": {
                    "value_status": row.get("value_status"),
                    "noticed": row.get("noticed") == "Y",
                    "notice_date": row.get("notice_dt"),
                    "protested": row.get("protested") == "Y",
                    "certified_date": row.get("certified_date"),
                    "revision_date": row.get("rev_dt"),
                    "revision_by": row.get("rev_by"),
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
            
            # Add parcel relationships if available
            if account_id in tieback_dict:
                property_record["parcel_relationships"] = tieback_dict[account_id]
            
            normalized_records.append(property_record)
        
        return normalized_records
    
    def _calculate_value_change(self, current_val, prior_val) -> dict:
        """Calculate percentage and dollar change between current and prior values."""
        try:
            current = float(current_val) if current_val else 0
            prior = float(prior_val) if prior_val else 0
            
            if prior == 0:
                return {"dollar_change": current, "percent_change": None}
            
            dollar_change = current - prior
            percent_change = (dollar_change / prior) * 100
            
            return {
                "dollar_change": round(dollar_change, 2),
                "percent_change": round(percent_change, 2)
            }
        except (ValueError, TypeError):
            return {"dollar_change": None, "percent_change": None}
    
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
