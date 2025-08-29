"""
Travis County Fixed-Width Field Specifications

Based on analysis of PROP.TXT and PROP_ENT.TXT, this module defines
the field positions and mappings for Travis County's fixed-width data format.
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
                return int(clean_val) if clean_val.isdigit() else 0
            elif self.type == 'float':
                clean_val = value.lstrip('0') or '0'
                return float(clean_val) / 100 if clean_val.replace('.', '').isdigit() else 0.0
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


# PROP.TXT Field Specifications (9,193 characters per line)
# Based on detailed analysis of actual Travis County data
PROP_FIELDS = [
    # Property Identifier Section
    FixedWidthField("account_id", 0, 12, 'str', "Property account ID"),
    FixedWidthField("property_type", 12, 13, 'str', "Property type (R=Residential, C=Commercial)"),
    FixedWidthField("tax_year", 17, 22, 'str', "Tax year (02025 format)"),
    
    # Geographic/ID Info
    FixedWidthField("geo_id", 550, 570, 'str', "Geographic ID code"),
    
    # Owner Information Section (Mailing Address)
    FixedWidthField("owner_id", 580, 592, 'str', "Owner ID number"), 
    FixedWidthField("owner_name", 608, 660, 'str', "Primary owner name"),
    FixedWidthField("owner_address", 693, 743, 'str', "Owner mailing address"),
    FixedWidthField("owner_address2", 743, 793, 'str', "Owner address line 2"),
    FixedWidthField("owner_city", 873, 923, 'str', "Owner city"),
    FixedWidthField("owner_state", 923, 926, 'str', "Owner state"),
    FixedWidthField("owner_zip", 978, 988, 'str', "Owner ZIP code"),
    
    # Property Address (Physical Location)
    FixedWidthField("property_zip", 1138, 1143, 'str', "Property ZIP code"),
    
    # Legal Description & Classification
    FixedWidthField("legal_description", 1150, 1250, 'str', "Legal description"),
    FixedWidthField("map_reference", 1680, 1720, 'str', "Map/section reference"),
    FixedWidthField("property_class", 1720, 1750, 'str', "Property classification"),
    
    # Valuation Section - Found actual positions
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

# PROP_ENT.TXT Field Specifications (2,750 characters per line)
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


def normalize_travis_account_id(account_id: str) -> str:
    """Normalize Travis County account ID to consistent format."""
    if not account_id:
        return ""
    
    # Clean and pad Travis account IDs
    clean_id = ''.join(c for c in str(account_id) if c.isdigit())
    
    # Travis County typically uses 12-digit account IDs
    return clean_id.zfill(12)


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
            "street_address": None,  # Travis doesn't have separate property address
            "city": None,
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
    
    return unified_record
