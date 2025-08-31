"""
Dallas County Appraisal District (DCAD) data normalizer.

Dallas CAD provides a normalized relational CSV structure with separate files for:
- ACCOUNT_INFO.CSV: Main property and owner information
- ACCOUNT_APPRL_YEAR.CSV: Property valuations and tax jurisdictions
- MULTI_OWNER.CSV: Additional owners with ownership percentages
- RES_DETAIL.CSV: Residential property building details
- LAND.CSV: Land characteristics and zoning information
- Various exemption files

Account IDs are 17-digit format (e.g., "38075500070120000")
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
import polars as pl
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from ..models.config import Config


def normalize_dallas_account_id(account_id: str) -> str:
    """Normalize Dallas account ID to consistent 17-digit format."""
    if not account_id:
        return "00000000000000000"
    
    # Remove any non-digit characters and pad to 17 digits
    clean_id = ''.join(c for c in str(account_id) if c.isdigit())
    return clean_id.zfill(17)


class DallasCountyNormalizer:
    """Normalizer for Dallas County Appraisal District data."""
    
    def __init__(self, config: Config):
        self.config = config
        self.console = Console()
        
        # Dallas CAD data directory 
        dallas_data_dir = Path.home() / "Downloads" / "DCAD2025_CURRENT"
        
        self.files = {
            'account_info': dallas_data_dir / 'ACCOUNT_INFO.CSV',
            'account_apprl': dallas_data_dir / 'ACCOUNT_APPRL_YEAR.CSV', 
            'multi_owner': dallas_data_dir / 'MULTI_OWNER.CSV',
            'res_detail': dallas_data_dir / 'RES_DETAIL.CSV',
            'land_detail': dallas_data_dir / 'LAND.CSV',
            'taxable_object': dallas_data_dir / 'TAXABLE_OBJECT.CSV',
            'exemptions': dallas_data_dir / 'ACCT_EXEMPT_VALUE.CSV'
        }
        
        # Verify files exist
        missing_files = [name for name, path in self.files.items() if not path.exists()]
        if missing_files:
            self.console.print(f"[red]Missing files: {missing_files}[/red]")
            self.console.print(f"[yellow]Expected location: {dallas_data_dir}[/yellow]")

    def diagnose_files(self) -> Dict[str, Any]:
        """Analyze Dallas CAD file structure and quality."""
        diagnostics = {}
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:
            
            for file_name, file_path in self.files.items():
                task = progress.add_task(f"Analyzing {file_name}...", total=None)
                
                if not file_path.exists():
                    diagnostics[file_name] = {"status": "missing", "path": str(file_path)}
                    continue
                
                try:
                    # Quick analysis with pandas
                    df = pd.read_csv(file_path, nrows=1000)  # Sample first 1000 rows
                    
                    diagnostics[file_name] = {
                        "status": "ok",
                        "path": str(file_path),
                        "size_mb": file_path.stat().st_size / (1024 * 1024),
                        "estimated_rows": self._estimate_rows(file_path),
                        "columns": list(df.columns),
                        "sample_data": df.head(2).to_dict('records')
                    }
                    
                except Exception as e:
                    diagnostics[file_name] = {
                        "status": "error", 
                        "path": str(file_path),
                        "error": str(e)
                    }
                    
                progress.remove_task(task)
        
        return diagnostics

    def _estimate_rows(self, file_path: Path) -> int:
        """Estimate number of rows in CSV file."""
        try:
            with open(file_path, 'r') as f:
                # Count first 1000 lines to estimate average line length
                lines = []
                for i, line in enumerate(f):
                    if i >= 1000:
                        break
                    lines.append(len(line))
                
                if not lines:
                    return 0
                
                avg_line_length = sum(lines) / len(lines)
                file_size = file_path.stat().st_size
                estimated_rows = int(file_size / avg_line_length) - 1  # Subtract header
                return max(0, estimated_rows)
                
        except Exception:
            return 0

    def load_and_normalize_sample(self, sample_size: int = 1000) -> List[Dict[str, Any]]:
        """Load and normalize a sample of Dallas CAD data."""
        self.console.print(f"[blue]🏛️ Loading Dallas County sample ({sample_size:,} properties)[/blue]")
        
        # Load core data files - only the sample we need
        account_info_df = self._load_csv_file(self.files['account_info'], sample_size)
        
        if account_info_df is None:
            self.console.print("[red]Failed to load core account information[/red]")
            return []
        
        self.console.print(f"✅ Loaded {len(account_info_df):,} account records")
        
        # Normalize account IDs for joining
        account_info_df['account_id_norm'] = account_info_df['ACCOUNT_NUM'].apply(normalize_dallas_account_id)
        
        # Get the account IDs we're working with for efficient filtering
        target_account_ids = set(account_info_df['account_id_norm'])
        self.console.print(f"[blue]📋 Filtering related files for {len(target_account_ids):,} account IDs...[/blue]")
        
        # Load related files with filtering to only get records we need
        account_apprl_df = self._load_and_filter_csv(self.files['account_apprl'], target_account_ids, 'ACCOUNT_NUM')
        multi_owner_df = self._load_and_filter_csv(self.files['multi_owner'], target_account_ids, 'ACCOUNT_NUM')  
        res_detail_df = self._load_and_filter_csv(self.files['res_detail'], target_account_ids, 'ACCOUNT_NUM')
        land_detail_df = self._load_and_filter_csv(self.files['land_detail'], target_account_ids, 'ACCOUNT_NUM')
        
        if account_apprl_df is not None:
            account_apprl_df['account_id_norm'] = account_apprl_df['ACCOUNT_NUM'].apply(normalize_dallas_account_id)
            self.console.print(f"✅ Loaded {len(account_apprl_df):,} appraisal records (filtered)")
        
        if multi_owner_df is not None:
            multi_owner_df['account_id_norm'] = multi_owner_df['ACCOUNT_NUM'].apply(normalize_dallas_account_id)
            self.console.print(f"✅ Loaded {len(multi_owner_df):,} multi-owner records (filtered)")
        
        if res_detail_df is not None:
            res_detail_df['account_id_norm'] = res_detail_df['ACCOUNT_NUM'].apply(normalize_dallas_account_id)
            self.console.print(f"✅ Loaded {len(res_detail_df):,} residential detail records (filtered)")
        
        if land_detail_df is not None:
            land_detail_df['account_id_norm'] = land_detail_df['ACCOUNT_NUM'].apply(normalize_dallas_account_id)
            self.console.print(f"✅ Loaded {len(land_detail_df):,} land detail records (filtered)")
        
        self.console.print(f"[blue]🔄 Normalizing to unified format...[/blue]")
        
        # Create unified records
        normalized_records = []
        
        for _, account_row in account_info_df.iterrows():
            account_id = account_row['account_id_norm']
            
            # Get related data
            apprl_data = None
            if account_apprl_df is not None:
                apprl_matches = account_apprl_df[account_apprl_df['account_id_norm'] == account_id]
                if len(apprl_matches) > 0:
                    apprl_data = apprl_matches.iloc[0]
            
            # Get additional owners
            additional_owners = []
            if multi_owner_df is not None:
                owner_matches = multi_owner_df[multi_owner_df['account_id_norm'] == account_id]
                additional_owners = owner_matches.to_dict('records')
            
            # Get residential details
            res_data = None
            if res_detail_df is not None:
                res_matches = res_detail_df[res_detail_df['account_id_norm'] == account_id]
                if len(res_matches) > 0:
                    res_data = res_matches.iloc[0]
            
            # Get land details
            land_data = None
            if land_detail_df is not None:
                land_matches = land_detail_df[land_detail_df['account_id_norm'] == account_id]
                if len(land_matches) > 0:
                    land_data = land_matches.iloc[0]
            
            # Create unified record
            unified_record = self._map_to_unified_model(
                account_row, apprl_data, additional_owners, res_data, land_data
            )
            
            normalized_records.append(unified_record)
        
        self.console.print(f"[green]🎉 Successfully normalized {len(normalized_records):,} records[/green]")
        return normalized_records

    def _load_csv_file(self, file_path: Path, sample_size: Optional[int] = None) -> Optional[pd.DataFrame]:
        """Load CSV file with error handling."""
        if not file_path.exists():
            return None
        
        try:
            if sample_size:
                df = pd.read_csv(file_path, nrows=sample_size, dtype=str)
            else:
                df = pd.read_csv(file_path, dtype=str)
            return df
        except Exception as e:
            self.console.print(f"[red]Error loading {file_path.name}: {e}[/red]")
            return None

    def _load_and_filter_csv(self, file_path: Path, target_account_ids: set, account_column: str) -> Optional[pd.DataFrame]:
        """Load CSV file and filter to only include target account IDs."""
        if not file_path.exists():
            return None
        
        try:
            # Read file in chunks to efficiently filter large files
            chunk_size = 50000
            filtered_chunks = []
            
            for chunk in pd.read_csv(file_path, dtype=str, chunksize=chunk_size):
                # Normalize account IDs in the chunk
                chunk['account_id_norm'] = chunk[account_column].apply(normalize_dallas_account_id)
                
                # Filter to only accounts we care about
                filtered_chunk = chunk[chunk['account_id_norm'].isin(target_account_ids)]
                
                if len(filtered_chunk) > 0:
                    filtered_chunks.append(filtered_chunk)
                
                # Stop early if we've found all our target accounts
                if len(filtered_chunks) > 0:
                    found_accounts = set()
                    for fc in filtered_chunks:
                        found_accounts.update(fc['account_id_norm'])
                    
                    # If we've found most of our target accounts, we can stop early
                    if len(found_accounts) >= len(target_account_ids) * 0.9:
                        break
            
            if filtered_chunks:
                result_df = pd.concat(filtered_chunks, ignore_index=True)
                return result_df
            else:
                return None
                
        except Exception as e:
            self.console.print(f"[red]Error loading and filtering {file_path.name}: {e}[/red]")
            return None

    def _map_to_unified_model(
        self, 
        account_row: pd.Series, 
        apprl_data: Optional[pd.Series],
        additional_owners: List[Dict[str, Any]],
        res_data: Optional[pd.Series],
        land_data: Optional[pd.Series]
    ) -> Dict[str, Any]:
        """Map Dallas CAD data to unified JSON model."""
        
        account_id = account_row['account_id_norm']
        
        # Extract owner information
        primary_owner = self._safe_str(account_row.get('OWNER_NAME1', ''))
        if account_row.get('OWNER_NAME2'):
            secondary = self._safe_str(account_row.get('OWNER_NAME2', ''))
            if secondary:
                primary_owner = f"{primary_owner} {secondary}".strip()
        
        # Build property address
        property_address = {
            "street_address": self._build_street_address(account_row),
            "city": self._safe_str(account_row.get('PROPERTY_CITY', '')),
            "state": "TX",
            "zip_code": self._safe_str(account_row.get('PROPERTY_ZIPCODE', ''))
        }
        
        # Build mailing address
        mailing_address = {
            "name": primary_owner,
            "address_line_1": self._safe_str(account_row.get('OWNER_ADDRESS_LINE1', '')),
            "address_line_2": self._safe_str(account_row.get('OWNER_ADDRESS_LINE2', '')),
            "city": self._safe_str(account_row.get('OWNER_CITY', '')),
            "state": self._safe_str(account_row.get('OWNER_STATE', '')),
            "zip_code": self._safe_str(account_row.get('OWNER_ZIPCODE', ''))
        }
        
        # Build property details
        property_details = {
            "division_code": self._safe_str(account_row.get('DIVISION_CD', '')),
            "business_name": self._safe_str(account_row.get('BIZ_NAME', '')),
            "neighborhood_code": self._safe_str(account_row.get('NBHD_CD', '')),
            "map_reference": self._safe_str(account_row.get('MAPSCO', '')),
            "gis_parcel_id": self._safe_str(account_row.get('GIS_PARCEL_ID', '')),
            "legal_description": self._build_legal_description(account_row)
        }
        
        # Add residential details if available
        if res_data is not None:
            property_details.update({
                "year_built": self._safe_int(res_data.get('YR_BUILT')),
                "building_class": self._safe_str(res_data.get('BLDG_CLASS_DESC', '')),
                "living_area_sf": self._safe_int(res_data.get('TOT_LIVING_AREA_SF')),
                "main_area_sf": self._safe_int(res_data.get('TOT_MAIN_SF')),
                "num_stories": self._safe_str(res_data.get('NUM_STORIES_DESC', '')),
                "num_bedrooms": self._safe_int(res_data.get('NUM_BEDROOMS')),
                "num_full_baths": self._safe_int(res_data.get('NUM_FULL_BATHS')),
                "num_half_baths": self._safe_int(res_data.get('NUM_HALF_BATHS'))
            })
        
        # Add land details if available  
        if land_data is not None:
            property_details.update({
                "zoning": self._safe_str(land_data.get('ZONING', '')),
                "land_area_sf": self._safe_float(land_data.get('AREA_SIZE')),
                "front_dimension": self._safe_float(land_data.get('FRONT_DIM')),
                "depth_dimension": self._safe_float(land_data.get('DEPTH_DIM'))
            })
        
        # Build valuation data
        valuation = {}
        if apprl_data is not None:
            valuation = {
                "land_value": self._safe_int(apprl_data.get('LAND_VAL', 0)),
                "improvement_value": self._safe_int(apprl_data.get('IMPR_VAL', 0)),
                "total_value": self._safe_int(apprl_data.get('TOT_VAL', 0)),
                "homestead_cap_value": self._safe_int(apprl_data.get('HMSTD_CAP_VAL', 0)),
                "market_value": self._safe_int(apprl_data.get('TOT_VAL', 0)),  # Use total value as market value
                "agricultural_value": self._safe_int(apprl_data.get('AG_USE_VAL', 0))
            }
        
        # Build tax entities from appraisal data
        tax_entities = []
        if apprl_data is not None:
            jurisdictions = [
                ('CITY', 'city_juris_desc', 'city_taxable_val'),
                ('COUNTY', 'county_juris_desc', 'county_taxable_val'),
                ('ISD', 'isd_juris_desc', 'isd_taxable_val'),
                ('HOSPITAL', 'hospital_juris_desc', 'hospital_taxable_val'),
                ('COLLEGE', 'college_juris_desc', 'college_taxable_val'),
                ('SPECIAL_DIST', 'special_dist_juris_desc', 'special_dist_taxable_val')
            ]
            
            for entity_type, desc_col, taxable_col in jurisdictions:
                entity_desc = self._safe_str(apprl_data.get(desc_col.upper(), ''))
                taxable_val = self._safe_int(apprl_data.get(taxable_col.upper(), 0))
                
                if entity_desc and entity_desc != 'UNASSIGNED':
                    tax_entities.append({
                        "entity_name": entity_desc,
                        "entity_type": entity_type,
                        "taxable_value": taxable_val,
                        "jurisdiction_id": None  # Dallas doesn't provide separate jurisdiction IDs
                    })
        
        # Build owners list
        owners = []
        if primary_owner:
            owners.append({
                "name": primary_owner,
                "owner_type": "primary", 
                "percentage": 100.0 - sum(float(owner.get('OWNERSHIP_PCT', 0)) for owner in additional_owners)
            })
        
        # Add additional owners
        for owner_data in additional_owners:
            if owner_data.get('OWNER_NAME'):
                owners.append({
                    "name": self._safe_str(owner_data['OWNER_NAME']),
                    "owner_type": "additional",
                    "percentage": self._safe_float(owner_data.get('OWNERSHIP_PCT', 0))
                })
        
        # Build unified record
        unified_record = {
            "account_id": account_id,
            "county": "dallas",
            "year": 2025,
            "property_address": property_address,
            "mailing_address": mailing_address,
            "property_details": property_details,
            "valuation": valuation,
            "legal_status": {
                "deed_transfer_date": self._safe_str(account_row.get('DEED_TXFR_DATE', '')),
                "appraisal_method": self._safe_str(apprl_data.get('APPRAISAL_METH_CD', '')) if apprl_data is not None else None
            },
            "tax_entities": tax_entities,
            "owners": owners,
            # Add improvements field for unified schema compatibility
            "improvements": self._build_improvements(account_row, res_data),
            # Add land details field for unified schema compatibility  
            "land_details": self._build_land_details(account_row, land_data),
            "metadata": {
                "data_source": "dallas_county_appraisal_district",
                "last_updated": None,
                "record_type": "property",
                "division_code": self._safe_str(account_row.get('DIVISION_CD', ''))
            }
        }
        
        return unified_record

    def _build_improvements(self, account_row: pd.Series, res_data: Optional[pd.Series]) -> List[Dict[str, Any]]:
        """Build improvements list for unified schema compatibility."""
        improvements = []
        
        if res_data is not None:
            # Main building improvement
            if res_data.get('TOT_LIVING_AREA_SF') or res_data.get('YR_BUILT'):
                improvements.append({
                    "improvement_id": f"{account_row.get('ACCOUNT_NUM', '')}_MAIN",
                    "improvement_type": "Main Building",
                    "improvement_class": self._safe_str(res_data.get('BLDG_CLASS_DESC', '')),
                    "year_built": self._safe_int(res_data.get('YR_BUILT')),
                    "square_footage": self._safe_int(res_data.get('TOT_LIVING_AREA_SF')),
                    "value": self._safe_int(res_data.get('IMPR_VAL', 0)),
                    "description": f"{self._safe_str(res_data.get('BLDG_CLASS_DESC', 'Building'))} - {self._safe_str(res_data.get('NUM_STORIES_DESC', ''))} stories"
                })
            
            # Additional building features
            if res_data.get('TOT_MAIN_SF') and res_data.get('TOT_MAIN_SF') != res_data.get('TOT_LIVING_AREA_SF'):
                improvements.append({
                    "improvement_id": f"{account_row.get('ACCOUNT_NUM', '')}_ADDITIONAL",
                    "improvement_type": "Additional Area",
                    "improvement_class": "Additional",
                    "square_footage": self._safe_int(res_data.get('TOT_MAIN_SF')),
                    "value": 0,  # Value included in main building
                    "description": "Additional building area beyond living space"
                })
        
        # Add any other improvements from property details
        if account_row.get('BLDG_ID'):
            improvements.append({
                "improvement_id": f"{account_row.get('ACCOUNT_NUM', '')}_BLDG_{account_row.get('BLDG_ID')}",
                "improvement_type": "Building Structure",
                "improvement_class": "Structure",
                "value": 0,
                "description": f"Building ID: {account_row.get('BLDG_ID')}"
            })
        
        return improvements

    def _build_land_details(self, account_row: pd.Series, land_data: Optional[pd.Series]) -> List[Dict[str, Any]]:
        """Build land details list for unified schema compatibility."""
        land_details = []
        
        # Main land record
        land_record = {
            "land_id": f"{account_row.get('ACCOUNT_NUM', '')}_LAND",
            "land_type": "LAND",
            "land_description": self._build_legal_description(account_row),
            "land_class": self._safe_str(account_row.get('DIVISION_CD', '')),
            "land_area": self._safe_float(account_row.get('LAND_AREA', 0)),
            "land_value": self._safe_int(account_row.get('LAND_VAL', 0))
        }
        
        if land_data is not None:
            land_record.update({
                "zoning": self._safe_str(land_data.get('ZONING', '')),
                "front_dimension": self._safe_float(land_data.get('FRONT_DIM', 0)),
                "depth_dimension": self._safe_float(land_data.get('DEPTH_DIM', 0))
            })
        
        land_details.append(land_record)
        
        # Add any additional land classifications
        if account_row.get('ACREAGE'):
            land_details.append({
                "land_id": f"{account_row.get('ACCOUNT_NUM', '')}_ACREAGE",
                "land_type": "ACREAGE",
                "land_description": f"Acreage: {account_row.get('ACREAGE')} acres",
                "land_class": "ACREAGE",
                "land_area": self._safe_float(account_row.get('ACREAGE', 0)) * 43560,  # Convert acres to sq ft
                "land_value": 0  # Value included in main land record
            })
        
        return land_details

    def _build_street_address(self, account_row: pd.Series) -> str:
        """Build full street address from components."""
        components = []
        
        street_num = self._safe_str(account_row.get('STREET_NUM', ''))
        if street_num:
            components.append(street_num)
        
        street_half = self._safe_str(account_row.get('STREET_HALF_NUM', ''))
        if street_half:
            components.append(street_half)
        
        street_name = self._safe_str(account_row.get('FULL_STREET_NAME', ''))
        if street_name:
            components.append(street_name)
        
        bldg_id = self._safe_str(account_row.get('BLDG_ID', ''))
        unit_id = self._safe_str(account_row.get('UNIT_ID', ''))
        
        if bldg_id:
            components.append(f"Bldg {bldg_id}")
        if unit_id:
            components.append(f"Unit {unit_id}")
        
        return ' '.join(components)

    def _build_legal_description(self, account_row: pd.Series) -> str:
        """Build legal description from components."""
        legal_parts = []
        
        for i in range(1, 6):  # LEGAL1 through LEGAL5
            legal_part = self._safe_str(account_row.get(f'LEGAL{i}', ''))
            if legal_part:
                legal_parts.append(legal_part)
        
        return ' '.join(legal_parts)

    def _safe_str(self, value) -> str:
        """Safely convert value to string."""
        if pd.isna(value) or value is None:
            return ''
        return str(value).strip()

    def _safe_int(self, value) -> int:
        """Safely convert value to integer."""
        if pd.isna(value) or value is None or value == '':
            return 0
        try:
            return int(float(str(value).replace(',', '').strip()))
        except (ValueError, TypeError):
            return 0

    def _safe_float(self, value) -> float:
        """Safely convert value to float."""
        if pd.isna(value) or value is None or value == '':
            return 0.0
        try:
            return float(str(value).replace(',', '').strip())
        except (ValueError, TypeError):
            return 0.0

    def save_sample_output(self, records: List[Dict[str, Any]], output_file: Path):
        """Save normalized records to JSON file."""
        import json
        from datetime import datetime
        
        output_data = {
            "created_at": datetime.utcnow().isoformat() + "Z",
            "county": "dallas",
            "record_count": len(records),
            "properties": records
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        self.console.print(f"[green]✅ Saved {len(records):,} records to {output_file}[/green]")

    def _map_division_to_property_type(self, division_code: str) -> str:
        """Map Dallas division code to human-readable property type."""
        if not division_code:
            return "Unknown"
        
        division_mapping = {
            'RES': 'Residential',
            'COM': 'Commercial',
            'IND': 'Industrial',
            'AG': 'Agricultural',
            'VAC': 'Vacant Land',
            'MIX': 'Mixed Use',
            'MULT': 'Multi-Family',
            'CONDO': 'Condominium',
            'MOBILE': 'Mobile Home',
            'RENTAL': 'Rental Property',
            'EXEMPT': 'Exempt Property',
            'GOV': 'Government',
            'CHURCH': 'Religious',
            'SCHOOL': 'Educational',
            'UTIL': 'Utility',
            'MINERAL': 'Mineral Rights',
            'TIMBER': 'Timber',
            'REC': 'Recreational',
            'HIST': 'Historical',
            'CONS': 'Conservation'
        }
        
        return division_mapping.get(division_code.upper(), f"Other ({division_code})")
