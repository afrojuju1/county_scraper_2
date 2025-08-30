"""
Travis County Data Normalizer

Handles parsing and normalizing Travis County appraisal district data.
Travis County uses a different format than Harris County:
- Fixed-width fields instead of CSV/TSV
- Multiple related tables (PROP.TXT, PROP_ENT.TXT, IMP_DET.TXT, etc.)
- Different primary key system

IMPROVED VERSION: Now uses corrected field specifications and better data processing
"""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Generator
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import json
from datetime import datetime
import logging

class TravisCountyNormalizer:
    """Normalizer for Travis County appraisal data."""
    
    def __init__(self, config=None):
        self.config = config
        self.console = Console()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
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
        # We'll import these when needed to avoid import issues
        # self.field_extractor = TravisFieldExtractor()
        
        # Data quality metrics
        self.processing_stats = {
            'total_properties': 0,
            'total_entities': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'missing_account_ids': 0,
            'processing_errors': []
        }
    
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
    
    def _read_file_in_chunks(self, file_path: Path, chunk_size: int = 1000) -> Generator[List[str], None, None]:
        """Read file in chunks to handle large files efficiently."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                chunk = []
                for i, line in enumerate(f):
                    line = line.rstrip()
                    if line:  # Skip empty lines
                        chunk.append(line)
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                
                # Yield remaining lines
                if chunk:
                    yield chunk
                    
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {e}")
            self.processing_stats['processing_errors'].append(str(e))
            yield []
    
    def extract_property_records(self, max_records: Optional[int] = None) -> Dict[str, Dict]:
        """Extract property records from PROP.TXT with improved error handling."""
        prop_file = self.files['properties']
        
        if not prop_file.exists():
            self.console.print(f"[red]Properties file not found: {prop_file}[/red]")
            return {}
        
        self.console.print(f"[blue]🏛️ Extracting property records from {prop_file.name}[/blue]")
        
        property_records = {}
        record_count = 0
        
        try:
            for chunk in self._read_file_in_chunks(prop_file):
                for line in chunk:
                    if max_records and record_count >= max_records:
                        break
                    
                    try:
                        # Extract property record using our corrected field specifications
                        prop_record = self.field_extractor.extract_property_record(line)
                        
                        if prop_record and prop_record.get('account_id'):
                            property_records[prop_record['account_id']] = prop_record
                            record_count += 1
                            self.processing_stats['successful_extractions'] += 1
                        else:
                            self.processing_stats['missing_account_ids'] += 1
                            
                    except Exception as e:
                        self.processing_stats['failed_extractions'] += 1
                        self.logger.warning(f"Failed to extract property record: {e}")
                        continue
                
                if max_records and record_count >= max_records:
                    break
            
            self.processing_stats['total_properties'] = len(property_records)
            self.console.print(f"[green]✅ Extracted {len(property_records):,} property records[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error extracting properties: {e}[/red]")
            self.processing_stats['processing_errors'].append(str(e))
        
        return property_records
    
    def extract_entity_records(self, property_account_ids: set, max_records: Optional[int] = None) -> Dict[str, List[Dict]]:
        """Extract property entity records from PROP_ENT.TXT, grouped by account_id."""
        ent_file = self.files['property_entities']
        
        if not ent_file.exists():
            self.console.print(f"[yellow]⚠️ Property entities file not found: {ent_file}[/yellow]")
            return {}
        
        self.console.print(f"[blue]🏢 Extracting property entity records from {ent_file.name}[/blue]")
        
        entity_records = {}
        record_count = 0
        
        try:
            for chunk in self._read_file_in_chunks(ent_file):
                for line in chunk:
                    if max_records and record_count >= max_records:
                        break
                    
                    try:
                        # Quick account ID check for efficiency
                        account_id = self.field_extractor.get_account_id(line)
                        
                        if account_id and account_id in property_account_ids:
                            entity_record = self.field_extractor.extract_entity_record(line)
                            
                            if entity_record:
                                if account_id not in entity_records:
                                    entity_records[account_id] = []
                                entity_records[account_id].append(entity_record)
                                record_count += 1
                                self.processing_stats['successful_extractions'] += 1
                        
                    except Exception as e:
                        self.processing_stats['failed_extractions'] += 1
                        self.logger.warning(f"Failed to extract entity record: {e}")
                        continue
                
                if max_records and record_count >= max_records:
                    break
            
            self.processing_stats['total_entities'] = sum(len(entities) for entities in entity_records.values())
            self.console.print(f"[green]✅ Extracted {self.processing_stats['total_entities']:,} entity records for {len(entity_records):,} properties[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error extracting entities: {e}[/red]")
            self.processing_stats['processing_errors'].append(str(e))
        
        return entity_records
    
    def normalize_to_unified_format(self, property_records: Dict[str, Dict], 
                                  entity_records: Dict[str, List[Dict]]) -> List[Dict]:
        """Transform raw Travis County data into our unified schema format."""
        self.console.print("[blue]🔄 Transforming to unified schema format...[/blue]")
        
        unified_records = []
        
        for account_id, prop_record in property_records.items():
            try:
                # Get related entity records for this property
                related_entities = entity_records.get(account_id, [])
                
                # Transform to unified format using our corrected field specifications
                unified_record = map_to_unified_model(prop_record, related_entities)
                
                # Add processing metadata
                unified_record['metadata']['last_updated'] = datetime.now().isoformat()
                unified_record['metadata']['processing_stats'] = {
                    'extraction_success': True,
                    'entity_count': len(related_entities),
                    'processing_timestamp': datetime.now().isoformat()
                }
                
                unified_records.append(unified_record)
                
            except Exception as e:
                self.logger.error(f"Failed to normalize record {account_id}: {e}")
                self.processing_stats['processing_errors'].append(f"Normalization failed for {account_id}: {e}")
                continue
        
        self.console.print(f"[green]🎉 Successfully normalized {len(unified_records):,} records[/green]")
        return unified_records
    
    def load_and_normalize_sample(self, sample_size: int = 100) -> List[Dict]:
        """Load and normalize a sample of Travis County data with improved processing."""
        
        self.console.print(f"[blue]🏛️ Processing Travis County sample ({sample_size:,} properties)[/blue]")
        
        # Reset processing stats
        self.processing_stats = {
            'total_properties': 0,
            'total_entities': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'missing_account_ids': 0,
            'processing_errors': []
        }
        
        # Step 1: Extract property records
        property_records = self.extract_property_records(max_records=sample_size)
        
        if not property_records:
            self.console.print("[red]❌ No property records extracted[/red]")
            return []
        
        # Step 2: Extract related entity records
        property_account_ids = set(property_records.keys())
        entity_records = self.extract_entity_records(property_account_ids)
        
        # Step 3: Transform to unified format
        normalized_records = self.normalize_to_unified_format(property_records, entity_records)
        
        # Display processing summary
        self._display_processing_summary()
        
        return normalized_records
    
    def _display_processing_summary(self):
        """Display a summary of the processing results."""
        stats = self.processing_stats
        
        self.console.print("\n" + "=" * 60)
        self.console.print("📊 PROCESSING SUMMARY")
        self.console.print("=" * 60)
        self.console.print(f"✅ Properties extracted: {stats['total_properties']:,}")
        self.console.print(f"✅ Entity records: {stats['total_entities']:,}")
        self.console.print(f"✅ Successful extractions: {stats['successful_extractions']:,}")
        self.console.print(f"❌ Failed extractions: {stats['failed_extractions']:,}")
        self.console.print(f"⚠️ Missing account IDs: {stats['missing_account_ids']:,}")
        
        if stats['processing_errors']:
            self.console.print(f"\n⚠️ Processing errors: {len(stats['processing_errors'])}")
            for error in stats['processing_errors'][:5]:  # Show first 5 errors
                self.console.print(f"   • {error}")
    
    def save_sample_output(self, normalized_records: List[Dict], output_path: Path):
        """Save normalized records to JSON file for inspection."""
        try:
            # Add processing metadata to output
            output_data = {
                "metadata": {
                    "county": "travis",
                    "extraction_date": datetime.now().isoformat(),
                    "total_records": len(normalized_records),
                    "data_source": "travis_county_appraisal_2025",
                    "parser_version": "improved_normalizer_v2",
                    "processing_stats": self.processing_stats,
                    "notes": "Improved normalizer with corrected field specifications"
                },
                "records": normalized_records
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            self.console.print(f"[green]💾 Saved normalized data to: {output_path}[/green]")
            self.console.print(f"📊 Records: {len(normalized_records):,}")
            self.console.print(f"📁 Size: {output_path.stat().st_size / 1024:.1f} KB")
            
        except Exception as e:
            self.console.print(f"[red]Error saving output: {e}[/red]")
            self.processing_stats['processing_errors'].append(f"Save failed: {e}")
    
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
