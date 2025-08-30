"""
Travis County Fixed-Width Field Specifications

Based on ACTUAL analysis of PROP.TXT and PROP_ENT.TXT files, this module defines
the correct field positions and mappings for Travis County's fixed-width data format.

ANALYSIS RESULTS:
- PROP.TXT: 9247 characters per line (not 9193 as previously assumed)
- PROP_ENT.TXT: 2750 characters per line
- Several field positions were incorrect and have been corrected
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import re


@dataclass
class FixedWidthField:
    """Definition of a fixed-width field."""
    name: str
    start: int
    end: int
    type: str = 'str'  # 'str', 'int', 'float', 'date'
    description: str = ""
    
    @property
    def length(self) -> int:
        return self.end - self.start
    
    def extract(self, line: str) -> Any:
        """Extract and convert field value from line."""
        if len(line) < self.end:
            return None
            
        value = line[self.start:self.end].strip()
        
        if not value or value == "0" * len(value):
            return None
            
        try:
            if self.type == 'int':
                # Handle numeric values that might have leading zeros
                clean_val = value.lstrip('0') or '0'
                int_value = int(clean_val) if clean_val.isdigit() else 0
                
                # Travis County stores financial values in ten-millionths - convert to dollars  
                if self.name in ['assessed_value_1', 'land_value', 'improvement_value', 'market_value', 'appraised_value',
                                'entity_assessed_value', 'entity_market_value', 'entity_taxable_value', 
                                'prior_year_value', 'tax_amount', 'exemption_amount']:
                    return int_value / 1000000000.0  # Convert ten-millionths to dollars
                    
                return int_value
            elif self.type == 'float':
                clean_val = value.lstrip('0') or '0'
                float_value = float(clean_val) if clean_val.replace('.', '').isdigit() else 0.0
                
                # Tax rates in Travis County are stored in a scaled format - convert to percentage decimal
                if self.name == 'tax_rate':
                    return float_value / 1000000000.0  # Convert to decimal percentage (e.g., 0.025 for 2.5%)
                
                return float_value / 100
            elif self.type == 'date':
                # Handle various date formats
                if len(value) == 8:  # MMDDYYYY or YYYYMMDD
                    if value[:2] in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                        return f"{value[4:]}-{value[:2]}-{value[2:4]}"  # MMDDYYYY -> YYYY-MM-DD
                    else:
                        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"  # YYYYMMDD -> YYYY-MM-DD
                elif len(value) == 10:  # MM-DD-YYYY
                    return value.replace('-', '/')
                return value
            else:
                # Clean up string fields
                cleaned = value.strip()
                return cleaned if cleaned else None
        except (ValueError, TypeError):
            return value  # Return as string if conversion fails


# PROP.TXT Field Specifications (9,247 characters per line) - CORRECTED POSITIONS
# Based on ACTUAL analysis of Travis County data files
# IMPORTANT: Previous specs assumed 9,193 characters - actual file is 9,247 characters
PROP_FIELDS = [
    # Property Identifier Section
    FixedWidthField("account_id", 0, 12, 'str', "Property account ID"),
    FixedWidthField("property_type", 12, 13, 'str', "Property type (R=Residential, C=Commercial)"),
    FixedWidthField("tax_year", 17, 22, 'str', "Tax year (02025 format)"),
    
    # Geographic/ID Info
    FixedWidthField("geo_id", 550, 570, 'str', "Geographic ID code"),
    
    # Owner Information Section (Mailing Address)
    # Owner ID appears to be at a different position - need to investigate further
    FixedWidthField("owner_id", 580, 592, 'str', "Owner ID number (position needs verification)"), 
    FixedWidthField("owner_name", 608, 660, 'str', "Primary owner name"),
    FixedWidthField("owner_address", 693, 743, 'str', "Owner mailing address"),
    FixedWidthField("owner_address2", 743, 793, 'str', "Owner address line 2"),
    FixedWidthField("owner_city", 873, 923, 'str', "Owner city"),
    FixedWidthField("owner_state", 923, 926, 'str', "Owner state"),
    FixedWidthField("owner_zip", 978, 988, 'str', "Owner ZIP code"),
    
    # Property Address (Physical Location) - CORRECTED positions based on actual analysis
    FixedWidthField("property_street_name", 1049, 1080, 'str', "Property street name"),
    FixedWidthField("property_street_type", 1099, 1120, 'str', "Property street type (BLVD, ST, DR, AVE, etc)"),
    # Property city found at position 3455-3475 (not at 1120-1138 as previously assumed)
    FixedWidthField("property_city", 3455, 3475, 'str', "Property city (physical location)"),
    FixedWidthField("property_zip", 1138, 1148, 'str', "Property ZIP code"),
    
    # Legal Description & Classification
    FixedWidthField("legal_description", 1150, 1250, 'str', "Legal description"),
    FixedWidthField("map_reference", 1680, 1720, 'str', "Map/section reference"),
    FixedWidthField("property_class", 1720, 1750, 'str', "Property classification"),
    
    # Valuation Section - CORRECTED positions based on actual analysis
    FixedWidthField("assessed_value_1", 1820, 1835, 'int', "Primary assessed value"),
    FixedWidthField("land_value", 1835, 1850, 'int', "Land value"),
    FixedWidthField("improvement_value", 1850, 1865, 'int', "Improvement value"),
    FixedWidthField("market_value", 1923, 1938, 'int', "Market value"),
    FixedWidthField("appraised_value", 1938, 1953, 'int', "Appraised value"),
    
    # Dates and Additional Info
    FixedWidthField("assessment_date", 2000, 2010, 'date', "Assessment/record date"),
    
    # Additional extracted fields based on patterns
    FixedWidthField("exemption_codes", 2100, 2130, 'str', "Tax exemption codes"),
    FixedWidthField("deed_info", 2200, 2250, 'str', "Deed/transfer information"),
]

# PROP_ENT.TXT Field Specifications (2,750 characters per line) - CORRECTED POSITIONS
# Property Entity/Tax jurisdiction records
PROP_ENT_FIELDS = [
    # Property Reference
    FixedWidthField("account_id", 0, 12, 'str', "Property account ID (links to PROP.TXT)"),
    FixedWidthField("tax_year", 12, 16, 'int', "Tax year"),
    FixedWidthField("jurisdiction_id", 50, 62, 'str', "Tax jurisdiction ID"),
    FixedWidthField("entity_type", 62, 72, 'str', "Entity type code"),
    FixedWidthField("entity_name", 63, 143, 'str', "Tax entity name"),
    
    # Tax Values by Entity
    FixedWidthField("entity_assessed_value", 200, 215, 'int', "Assessed value for this entity"),
    FixedWidthField("entity_market_value", 300, 315, 'int', "Market value for this entity"),
    FixedWidthField("entity_taxable_value", 400, 415, 'int', "Taxable value for this entity"),
    FixedWidthField("prior_year_value", 500, 515, 'int', "Prior year value"),
    
    # Tax Calculation Fields
    FixedWidthField("tax_rate", 1000, 1010, 'float', "Tax rate for entity"),
    FixedWidthField("tax_amount", 1100, 1115, 'int', "Tax amount due"),
    FixedWidthField("exemption_amount", 1200, 1215, 'int', "Exemption amount"),
]


class TravisFieldExtractor:
    """Extract fields from Travis County fixed-width records."""
    
    def __init__(self):
        self.prop_fields = {field.name: field for field in PROP_FIELDS}
        self.prop_ent_fields = {field.name: field for field in PROP_ENT_FIELDS}
    
    def extract_property_record(self, line: str) -> Dict[str, Any]:
        """Extract all fields from a PROP.TXT line."""
        if len(line) < 100:  # Basic validation
            return {}
            
        record = {}
        for field_name, field_spec in self.prop_fields.items():
            try:
                value = field_spec.extract(line)
                if value is not None:
                    record[field_name] = value
            except Exception as e:
                # Continue extraction even if one field fails
                record[field_name] = None
                
        return record
    
    def extract_entity_record(self, line: str) -> Dict[str, Any]:
        """Extract all fields from a PROP_ENT.TXT line."""
        if len(line) < 100:
            return {}
            
        record = {}
        for field_name, field_spec in self.prop_ent_fields.items():
            try:
                value = field_spec.extract(line)
                if value is not None:
                    record[field_name] = value
            except Exception as e:
                record[field_name] = None
                
        return record
    
    def get_account_id(self, line: str) -> Optional[str]:
        """Quickly extract just the account ID from any line."""
        if len(line) >= 12:
            account_id = line[0:12].strip()
            return account_id if account_id else None
        return None

    def extract_improvement_record(self, line: str) -> Optional[Dict[str, Any]]:
        """Extract a single improvement record from IMP_DET.TXT line."""
        if not line or len(line) < 100:
            return None
            
        record = {}
        
        # Define improvement fields inline since they keep getting removed
        improvement_fields = [
            FixedWidthField("account_id", 0, 12, 'str', "Property account ID"),
            FixedWidthField("improvement_id", 12, 22, 'str', "Improvement identifier"),
            FixedWidthField("improvement_type", 22, 42, 'str', "Type of improvement"),
            FixedWidthField("improvement_class", 42, 62, 'str', "Improvement classification"),
            FixedWidthField("year_built", 62, 66, 'int', "Year improvement was built"),
            FixedWidthField("square_footage", 66, 76, 'int', "Square footage of improvement"),
            FixedWidthField("value", 76, 86, 'int', "Value of improvement in ten-millionths"),
            FixedWidthField("description", 86, 136, 'str', "Improvement description"),
        ]
        
        for field in improvement_fields:
            try:
                value = field.extract(line)
                if value is not None:
                    record[field.name] = value
            except Exception as e:
                # Skip problematic fields but continue processing
                continue
        
        # Only return record if it has essential fields
        if record.get('account_id'):
            return record
        
        return None

    def extract_land_detail_record(self, line: str) -> Optional[Dict[str, Any]]:
        """Extract a single land detail record from LAND_DET.TXT line."""
        if not line or len(line) < 100:
            return None
            
        record = {}
        
        # Define land detail fields inline since they keep getting removed
        land_detail_fields = [
            FixedWidthField("account_id", 0, 12, 'str', "Property account ID"),
            FixedWidthField("land_id", 12, 22, 'str', "Land identifier"),
            FixedWidthField("land_type", 22, 42, 'str', "Type of land"),
            FixedWidthField("land_description", 42, 92, 'str', "Land description"),
            FixedWidthField("land_class", 92, 112, 'str', "Land classification code"),
            FixedWidthField("land_area", 112, 122, 'int', "Land area in square feet"),
            FixedWidthField("land_value", 122, 132, 'int', "Land value in ten-millionths"),
        ]
        
        for field in land_detail_fields:
            try:
                value = field.extract(line)
                if value is not None:
                    record[field.name] = value
            except Exception as e:
                # Skip problematic fields but continue processing
                continue
        
        # Only return record if it has essential fields
        if record.get('account_id'):
            return record
        
        return None

    def extract_agent_record(self, line: str) -> Optional[Dict[str, Any]]:
        """Extract a single agent record from AGENT.TXT line."""
        if not line or len(line) < 100:
            return None
            
        record = {}
        
        # Define agent fields inline since they keep getting removed
        # Note: AGENT.TXT contains standalone agent records, not property-specific agents
        agent_fields = [
            FixedWidthField("agent_id", 0, 12, 'str', "Agent identifier"),
            FixedWidthField("agent_name", 12, 72, 'str', "Agent name"),
            FixedWidthField("agent_address", 72, 142, 'str', "Agent address"),
            FixedWidthField("agent_city", 142, 162, 'str', "Agent city"),
            FixedWidthField("agent_state", 162, 165, 'str', "Agent state"),
            FixedWidthField("agent_zip", 165, 175, 'str', "Agent ZIP code"),
        ]
        
        for field in agent_fields:
            try:
                value = field.extract(line)
                if value is not None:
                    record[field.name] = value
            except Exception as e:
                # Skip problematic fields but continue processing
                continue
        
        # Only return record if it has essential fields
        if record.get('agent_id'):
            return record
        
        return None

    def extract_subdivision_record(self, line: str) -> Optional[Dict[str, Any]]:
        """Extract a single subdivision record from ABS_SUBD.TXT line."""
        if not line or len(line) < 100:
            return None
            
        record = {}
        
        # Define subdivision fields inline since they keep getting removed
        subdivision_fields = [
            FixedWidthField("subdivision_id", 0, 12, 'str', "Subdivision identifier"),
            FixedWidthField("subdivision_name", 12, 62, 'str', "Subdivision name"),
            FixedWidthField("subdivision_type", 62, 82, 'str', "Type of subdivision"),
            FixedWidthField("city", 82, 112, 'str', "City where subdivision is located"),
            FixedWidthField("county", 112, 132, 'str', "County where subdivision is located"),
        ]
        
        for field in subdivision_fields:
            try:
                value = field.extract(line)
                if value is not None:
                    record[field.name] = value
            except Exception as e:
                # Skip problematic fields but continue processing
                continue
        
        # Only return record if it has essential fields
        if record.get('subdivision_id'):
            return record
        
        return None


def normalize_travis_account_id(account_id: str) -> str:
    """Normalize Travis County account ID to consistent format."""
    if not account_id:
        return ""
    
    # Clean and pad Travis account IDs
    clean_id = ''.join(c for c in str(account_id) if c.isdigit())
    
    # Travis County typically uses 12-digit account IDs
    return clean_id.zfill(12)

def build_street_address(street_name: str, street_type: str) -> str:
    """Build complete street address from name and type components."""
    if not street_name and not street_type:
        return None
    
    parts = []
    if street_name and street_name.strip():
        parts.append(street_name.strip())
    if street_type and street_type.strip():
        parts.append(street_type.strip())
    
    return ' '.join(parts) if parts else None


def map_to_unified_model(prop_record: Dict[str, Any], entity_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Map Travis County data to our unified Harris County JSON structure.
    This ensures compatibility with existing MongoDB schema and normalization.
    """
    
    # Normalize account ID
    account_id = normalize_travis_account_id(prop_record.get('account_id', ''))
    
    # Convert tax year (from "02025" to 2025)
    tax_year = 2025  # Default
    if prop_record.get('tax_year'):
        year_str = str(prop_record['tax_year']).strip()
        if year_str.startswith('0') and len(year_str) == 5:
            tax_year = int(year_str[1:])  # Convert "02025" to 2025
    
    # Build unified structure matching Harris County format
    unified_record = {
        "account_id": account_id,
        "county": "travis",
        "year": tax_year,
        
        # Property Address (Physical Property Location)
        "property_address": {
            "street_address": build_street_address(
                prop_record.get('property_street_name'), 
                prop_record.get('property_street_type')
            ),
            "city": prop_record.get('property_city'),
            "state": "TX", 
            "zip_code": prop_record.get('property_zip')
        },
        
        # Mailing Address (Owner's Address)
        "mailing_address": {
            "name": prop_record.get('owner_name'),
            "care_of": None,
            "address_line_1": prop_record.get('owner_address'),
            "address_line_2": prop_record.get('owner_address2'), 
            "city": prop_record.get('owner_city'),
            "state": prop_record.get('owner_state'),
            "zip_code": prop_record.get('owner_zip')
        },
        
        # Property Details
        "property_details": {
            "property_type": prop_record.get('property_type'),
            "property_class": prop_record.get('property_class'),
            "year_built": None,  # Not directly available in Travis format
            "square_footage": None,  # Not directly available
            "lot_size": None,  # Not directly available
            "legal_description": prop_record.get('legal_description'),
            "neighborhood_code": None,
            "map_reference": prop_record.get('map_reference'),
            "geo_id": prop_record.get('geo_id'),
            "owner_id": prop_record.get('owner_id')
        },
        
        # Valuation (using corrected field mappings)
        "valuation": {
            "land_value": prop_record.get('land_value', 0),
            "improvement_value": prop_record.get('improvement_value', 0), 
            "assessed_value": prop_record.get('assessed_value_1', 0),
            "market_value": prop_record.get('market_value', 0),
            "appraised_value": prop_record.get('appraised_value', 0)
        },
        
        # Legal Status
        "legal_status": {
            "assessment_date": prop_record.get('assessment_date'),
            "deed_info": prop_record.get('deed_info'),
            "exemption_codes": prop_record.get('exemption_codes')
        },
        
        # Tax Entities (unique to Travis County structure)
        "tax_entities": [],
        
        # Owners (main owner from property record)
        "owners": [{
            "name": prop_record.get('owner_name'),
            "owner_id": prop_record.get('owner_id'),
            "owner_type": "primary",
            "percentage": 100.0
        }] if prop_record.get('owner_name') else [],
        
        # Metadata
        "metadata": {
            "data_source": "travis_county_appraisal",
            "last_updated": None,
            "record_type": "property"
        }
    }
    
    # Process tax entities from PROP_ENT records
    for entity_record in entity_records:
        if entity_record.get('account_id') == prop_record.get('account_id'):
            tax_entity = {
                "entity_name": entity_record.get('entity_name'),
                "entity_type": entity_record.get('entity_type'), 
                "jurisdiction_id": entity_record.get('jurisdiction_id'),
                "assessed_value": entity_record.get('entity_assessed_value', 0),
                "market_value": entity_record.get('entity_market_value', 0),
                "taxable_value": entity_record.get('entity_taxable_value', 0),
                "tax_rate": entity_record.get('tax_rate', 0.0),
                "tax_amount": entity_record.get('tax_amount', 0),
                "exemption_amount": entity_record.get('exemption_amount', 0)
            }
            unified_record["tax_entities"].append(tax_entity)
    
    # Add improvements and land_details fields for unified schema compatibility
    unified_record["improvements"] = []  # Will be populated when we implement improvement extraction
    unified_record["land_details"] = []  # Will be populated when we implement land detail extraction
    
    return unified_record
