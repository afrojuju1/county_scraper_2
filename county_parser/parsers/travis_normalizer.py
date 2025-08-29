"""
Travis County Data Normalizer

Handles parsing and normalizing Travis County appraisal district data.
Travis County uses a different format than Harris County:
- Fixed-width fields instead of CSV/TSV
- Multiple related tables (PROP.TXT, PROP_ENT.TXT, IMP_DET.TXT, etc.)
- Different primary key system
"""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import json
from datetime import datetime

from ..models.config import Config
from .travis_field_specs import TravisFieldExtractor, map_to_unified_model


class TravisCountyNormalizer:
    """Normalizer for Travis County appraisal data."""
    
    def __init__(self, config: Config):
        self.config = config
        self.console = Console()
        
        # Travis County file paths (override data directory for Travis)
        travis_data_dir = Path(__file__).parent.parent.parent / "data" / "travis_2025"
        self.files = {
            'properties': travis_data_dir / 'PROP.TXT',
            'property_entities': travis_data_dir / 'PROP_ENT.TXT', 
            'improvements': travis_data_dir / 'IMP_DET.TXT',
            'improvement_attributes': travis_data_dir / 'IMP_ATR.TXT',
            'land_details': travis_data_dir / 'LAND_DET.TXT',
            'improvement_info': travis_data_dir / 'IMP_INFO.TXT',
            'agents': travis_data_dir / 'AGENT.TXT',
            'subdivisions': travis_data_dir / 'ABS_SUBD.TXT'
        }
        
        # Initialize field extractor
        self.field_extractor = TravisFieldExtractor()
    
    def diagnose_files(self) -> Dict[str, dict]:
        """Diagnose Travis County data files and their structure."""
        results = {}
        
        for file_type, file_path in self.files.items():
            if not file_path.exists():
                results[file_type] = {
                    'status': 'missing',
                    'path': str(file_path),
                    'error': f'File does not exist: {file_path}'
                }
                continue
                
            try:
                # Get basic file info
                file_size = file_path.stat().st_size
                file_size_mb = file_size / (1024 * 1024)
                
                # Try to read first few lines to understand structure
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = [f.readline().rstrip() for _ in range(5)]
                    lines = [line for line in lines if line]  # Remove empty lines
                
                # Count total lines (approximate)
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    line_count = sum(1 for _ in f)
                
                results[file_type] = {
                    'status': 'available',
                    'path': str(file_path),
                    'size_mb': round(file_size_mb, 2),
                    'line_count': line_count,
                    'sample_lines': lines,
                    'line_length': len(lines[0]) if lines else 0,
                    'format': 'fixed_width' if lines and len(lines[0]) > 200 else 'unknown'
                }
                
            except Exception as e:
                results[file_type] = {
                    'status': 'error', 
                    'path': str(file_path),
                    'error': str(e)
                }
        
        return results
    
    def _analyze_fixed_width_structure(self, file_path: Path, sample_size: int = 100) -> Dict:
        """Analyze the fixed-width structure of a Travis County file."""
        try:
            # Read sample lines
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample_lines = [f.readline().rstrip() for _ in range(sample_size)]
                sample_lines = [line for line in sample_lines if line]
            
            if not sample_lines:
                return {'error': 'No readable lines found'}
            
            # Analyze line lengths
            line_lengths = [len(line) for line in sample_lines]
            avg_length = sum(line_lengths) / len(line_lengths)
            
            # Try to identify potential field boundaries by looking for patterns
            # This is a heuristic approach - we'll need to refine based on actual field definitions
            
            return {
                'sample_count': len(sample_lines),
                'line_lengths': {
                    'min': min(line_lengths),
                    'max': max(line_lengths),
                    'avg': round(avg_length, 1)
                },
                'sample_records': sample_lines[:3],  # Show first 3 records
                'analysis': 'Fixed-width format detected - need field specifications'
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def get_file_info(self, file_type: str) -> Dict:
        """Get detailed information about a specific Travis County file."""
        if file_type not in self.files:
            return {'error': f'Unknown file type: {file_type}'}
        
        file_path = self.files[file_type]
        
        if not file_path.exists():
            return {'error': f'File does not exist: {file_path}'}
        
        # Basic info
        info = {
            'file_type': file_type,
            'path': str(file_path),
            'size_mb': round(file_path.stat().st_size / (1024 * 1024), 2)
        }
        
        # Detailed structure analysis
        structure = self._analyze_fixed_width_structure(file_path)
        info.update(structure)
        
        return info
    
    def load_properties_sample(self, sample_size: int = 1000) -> Optional[pd.DataFrame]:
        """Load a sample of properties from PROP.TXT for analysis."""
        prop_file = self.files['properties']
        
        if not prop_file.exists():
            self.console.print(f"[red]Properties file not found: {prop_file}[/red]")
            return None
        
        try:
            # For now, read as single-column to analyze structure
            df = pd.read_csv(
                prop_file,
                sep='\t',  # Try tab-delimited first
                nrows=sample_size,
                dtype=str,
                encoding='utf-8',
                on_bad_lines='skip',
                header=None
            )
            
            self.console.print(f"[green]✅ Loaded {len(df)} property records (sample)[/green]")
            return df
            
        except Exception as e:
            self.console.print(f"[red]Error loading properties: {e}[/red]")
            
            # Try reading as fixed-width (single column)
            try:
                with open(prop_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = [f.readline().rstrip() for _ in range(sample_size)]
                    lines = [line for line in lines if line]
                
                df = pd.DataFrame({'raw_record': lines})
                self.console.print(f"[yellow]⚠️ Loaded {len(df)} records as raw fixed-width[/yellow]")
                return df
                
            except Exception as e2:
                self.console.print(f"[red]Failed to load as fixed-width: {e2}[/red]")
                return None
    
    def load_and_normalize_sample(self, sample_size: int = 100) -> List[Dict]:
        """Load and normalize a sample of Travis County data."""
        
        self.console.print(f"[blue]🏛️ Loading Travis County sample ({sample_size:,} properties)[/blue]")
        
        # Load property records
        prop_file = self.files['properties']
        prop_records = {}
        
        if not prop_file.exists():
            self.console.print(f"[red]Properties file not found: {prop_file}[/red]")
            return []
        
        try:
            with open(prop_file, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i >= sample_size:
                        break
                    
                    line = line.rstrip()
                    if not line:
                        continue
                    
                    # Extract property record
                    prop_record = self.field_extractor.extract_property_record(line)
                    if prop_record and prop_record.get('account_id'):
                        prop_records[prop_record['account_id']] = prop_record
            
            self.console.print(f"[green]✅ Loaded {len(prop_records):,} property records[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error loading properties: {e}[/red]")
            return []
        
        # Load related entity records
        ent_file = self.files['property_entities']
        entity_records = []
        
        if ent_file.exists():
            try:
                account_ids = set(prop_records.keys())
                
                with open(ent_file, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        line = line.rstrip()
                        if not line:
                            continue
                        
                        # Quick account ID check
                        account_id = self.field_extractor.get_account_id(line)
                        if account_id in account_ids:
                            entity_record = self.field_extractor.extract_entity_record(line)
                            if entity_record:
                                entity_records.append(entity_record)
                
                self.console.print(f"[green]✅ Loaded {len(entity_records):,} related entity records[/green]")
                
            except Exception as e:
                self.console.print(f"[yellow]⚠️ Could not load entity records: {e}[/yellow]")
        
        # Normalize to unified format
        self.console.print("[blue]🔄 Normalizing to unified format...[/blue]")
        normalized_records = []
        
        for account_id, prop_record in prop_records.items():
            # Get related entity records for this property
            related_entities = [ent for ent in entity_records if ent.get('account_id') == account_id]
            
            # Map to unified format
            unified_record = map_to_unified_model(prop_record, related_entities)
            unified_record['metadata']['last_updated'] = datetime.now().isoformat()
            
            normalized_records.append(unified_record)
        
        self.console.print(f"[green]🎉 Successfully normalized {len(normalized_records):,} records[/green]")
        return normalized_records
    
    def save_sample_output(self, normalized_records: List[Dict], output_path: Path):
        """Save normalized records to JSON file for inspection."""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(normalized_records, f, indent=2, ensure_ascii=False)
            
            self.console.print(f"[green]💾 Saved normalized data to: {output_path}[/green]")
            self.console.print(f"📊 Records: {len(normalized_records):,}")
            self.console.print(f"📁 Size: {output_path.stat().st_size / 1024:.1f} KB")
            
        except Exception as e:
            self.console.print(f"[red]Error saving output: {e}[/red]")
    
    def compare_with_harris_model(self, travis_sample: List[Dict]) -> Dict[str, Any]:
        """Compare Travis County fields with Harris County model to identify gaps."""
        
        if not travis_sample:
            return {"error": "No Travis sample data to compare"}
        
        # Analyze first record structure
        sample_record = travis_sample[0]
        
        comparison = {
            "travis_unique_fields": [],
            "harris_missing_fields": [],
            "common_fields": [],
            "data_coverage": {},
            "recommendations": []
        }
        
        # Expected Harris County structure (from our existing model)
        harris_expected_fields = {
            'account_id', 'county', 'year', 'property_address', 'mailing_address',
            'property_details', 'valuation', 'legal_status', 'owners', 'deeds', 
            'permits', 'parcel_relationships', 'metadata'
        }
        
        travis_fields = set(sample_record.keys())
        
        # Find field overlaps and differences
        comparison['common_fields'] = list(harris_expected_fields.intersection(travis_fields))
        comparison['travis_unique_fields'] = list(travis_fields - harris_expected_fields)
        comparison['harris_missing_fields'] = list(harris_expected_fields - travis_fields)
        
        # Analyze data coverage
        total_records = len(travis_sample)
        for field in travis_fields:
            non_null_count = sum(1 for record in travis_sample if record.get(field) is not None)
            coverage_pct = (non_null_count / total_records) * 100
            comparison['data_coverage'][field] = {
                'coverage_percentage': round(coverage_pct, 1),
                'non_null_count': non_null_count,
                'total_count': total_records
            }
        
        # Generate recommendations
        if 'tax_entities' in comparison['travis_unique_fields']:
            comparison['recommendations'].append(
                "Add 'tax_entities' field to unified model - Travis has rich tax jurisdiction data"
            )
        
        if comparison['data_coverage'].get('property_details', {}).get('coverage_percentage', 0) > 80:
            comparison['recommendations'].append(
                "Travis provides good property detail coverage - consider expanding unified model"
            )
        
        return comparison
