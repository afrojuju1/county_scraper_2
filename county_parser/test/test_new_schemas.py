#!/usr/bin/env python3
"""
Test script for the new enhanced schemas.
"""

import sys
import os
import json
from pathlib import Path

# Add the parent directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'models'))

# Import our new schemas
from schemas import (
    TaxEntityRecord,
    ImprovementRecord,
    ImprovementAttributeRecord,
    LandDetailRecord,
    AgentRecord,
    UnifiedPropertyRecord
)

def test_new_schemas():
    """Test the new schemas with sample Travis County data."""
    
    print("🧪 Testing New Enhanced Schemas")
    print("=" * 50)
    
    # Test 1: Tax Entity Record
    print("\n1. Testing TaxEntityRecord...")
    try:
        tax_entity = TaxEntityRecord(
            account_id="000000100008",
            tax_year=2025,
            jurisdiction_id="0000A",
            entity_type="TRAVIS CE",
            entity_name="TRAVIS CENTRAL APP DIST",
            entity_taxable_value=433206.6,
            tax_rate=1.0
        )
        print(f"   ✅ TaxEntityRecord created successfully: {tax_entity.entity_name}")
    except Exception as e:
        print(f"   ❌ TaxEntityRecord failed: {e}")
    
    # Test 2: Improvement Record
    print("\n2. Testing ImprovementRecord...")
    try:
        improvement = ImprovementRecord(
            account_id="000000100008",
            tax_year=2025,
            improvement_id="0006960813",
            improvement_type="1st Floor",
            improvement_class="C",
            year_built=2013,
            square_footage=2986.0,
            value=80037.0
        )
        print(f"   ✅ ImprovementRecord created successfully: {improvement.improvement_type}")
    except Exception as e:
        print(f"   ❌ ImprovementRecord failed: {e}")
    
    # Test 3: Land Detail Record
    print("\n3. Testing LandDetailRecord...")
    try:
        land_detail = LandDetailRecord(
            account_id="000000100008",
            tax_year=2025,
            land_id="0007284336",
            land_type="LAND",
            land_description="Land",
            land_class="F1",
            land_area=100000.0,
            land_value=235120.0
        )
        print(f"   ✅ LandDetailRecord created successfully: {land_detail.land_type}")
    except Exception as e:
        print(f"   ❌ LandDetailRecord failed: {e}")
    
    # Test 4: Agent Record
    print("\n4. Testing AgentRecord...")
    try:
        agent = AgentRecord(
            account_id="000000100008",
            agent_id="0000016008",
            agent_name="DJB INVESTMENT PROPERTY LLC",
            agent_type="Owner",
            agent_address="41 DOOLITTLE DR",
            agent_city="WOODCREEK",
            agent_state="TX",
            agent_zip="78676"
        )
        print(f"   ✅ AgentRecord created successfully: {agent.agent_name}")
    except Exception as e:
        print(f"   ❌ AgentRecord failed: {e}")
    
    # Test 5: Unified Property Record
    print("\n5. Testing UnifiedPropertyRecord...")
    try:
        unified_property = UnifiedPropertyRecord(
            account_id="000000100008",
            county="travis",
            year=2025,
            property_address={
                "street_address": "LAMAR BLVD",
                "city": "AUSTIN",
                "state": "TX",
                "zip_code": "78704"
            },
            mailing_address={
                "name": "DJB INVESTMENT PROPERTY LLC",
                "address_line_1": "41 DOOLITTLE DR",
                "city": "WOODCREEK",
                "state": "TX",
                "zip_code": "78676"
            },
            property_details={
                "property_type": "R",
                "property_class": "1-4",
                "legal_description": "OT 1-4 TEMPLER LOTS"
            },
            valuation={
                "market_value": 433206.6,
                "assessed_value": 321600.0
            },
            legal_status={
                "assessment_date": "2025-01-01"
            },
            tax_entities=[tax_entity],
            improvements=[improvement],
            land_details=[land_detail],
            agents=[agent],
            metadata={
                "created_at": "2025-08-29T22:30:00Z",
                "source": "travis_county_parser"
            }
        )
        print(f"   ✅ UnifiedPropertyRecord created successfully: {unified_property.account_id}")
        print(f"      Tax Entities: {len(unified_property.tax_entities)}")
        print(f"      Improvements: {len(unified_property.improvements)}")
        print(f"      Land Details: {len(unified_property.land_details)}")
        print(f"      Agents: {len(unified_property.agents)}")
        
    except Exception as e:
        print(f"   ❌ UnifiedPropertyRecord failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 6: Schema Validation
    print("\n6. Testing Schema Validation...")
    try:
        # Test with invalid data
        invalid_tax_entity = TaxEntityRecord(
            account_id="",  # Empty account_id should fail
            tax_year=2025,
            jurisdiction_id="0000A",
            entity_type="TRAVIS CE",
            entity_name="TRAVIS CENTRAL APP DIST"
        )
        print(f"   ❌ Validation should have failed for empty account_id")
    except Exception as e:
        print(f"   ✅ Validation correctly caught error: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Schema testing completed!")
    
    # Save sample data
    try:
        output_file = Path("../../output/schema_test_sample.json")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        sample_data = {
            "tax_entity": tax_entity.dict(),
            "improvement": improvement.dict(),
            "land_detail": land_detail.dict(),
            "agent": agent.dict(),
            "unified_property": unified_property.dict()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Sample data saved to: {output_file}")
        
    except Exception as e:
        print(f"⚠️ Could not save sample data: {e}")

if __name__ == "__main__":
    test_new_schemas()
