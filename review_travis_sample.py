#!/usr/bin/env python3
"""
Travis County Sample Review Script

This script extracts 50 Travis County properties and analyzes data quality for missing fields.
"""

import json
import os
from datetime import datetime
from pathlib import Path

# Import our services
from county_parser.services.mongodb_service import MongoDBService
from county_parser.models.config import Config

def review_travis_sample():
    """Review Travis County sample data for missing fields and data quality."""
    
    print("🏛️ Travis County Sample Review")
    print("=" * 50)
    
    # Initialize MongoDB service
    config = Config()
    mongodb = MongoDBService()
    
    if not mongodb.connect():
        print("❌ Failed to connect to MongoDB")
        return
    
    try:
        print("✅ Connected to MongoDB successfully!")
        
        # Get Travis County sample (50 properties)
        print(f"\n🏠 Extracting 50 Travis County properties...")
        travis_pipeline = [
            {'$match': {'county': 'travis'}},
            {'$sample': {'size': 50}}
        ]
        
        travis_properties = list(mongodb.properties_collection.aggregate(travis_pipeline))
        
        # Convert ObjectId to string for JSON serialization
        for prop in travis_properties:
            if '_id' in prop:
                prop['_id'] = str(prop['_id'])
        
        print(f"✅ Retrieved {len(travis_properties):,} Travis County properties")
        
        # Analyze data quality and missing fields
        print(f"\n🔍 Data Quality Analysis:")
        
        # Define key fields to check
        key_fields = {
            'account_id': 'Account ID',
            'county': 'County',
            'year': 'Year',
            'property_address': 'Property Address',
            'mailing_address': 'Mailing Address',
            'valuation': 'Valuation',
            'legal_status': 'Legal Status',
            'tax_entities': 'Tax Entities',
            'improvements': 'Improvements',
            'land_details': 'Land Details'
        }
        
        # Check field presence and data quality
        field_stats = {}
        for field, display_name in key_fields.items():
            present_count = 0
            non_empty_count = 0
            
            for prop in travis_properties:
                if field in prop:
                    present_count += 1
                    # Check if the field has actual data (not just empty dict/list)
                    field_value = prop[field]
                    if isinstance(field_value, dict) and field_value:
                        non_empty_count += 1
                    elif isinstance(field_value, list) and field_value:
                        non_empty_count += 1
                    elif field_value and str(field_value).strip():
                        non_empty_count += 1
            
            field_stats[field] = {
                'display_name': display_name,
                'present': present_count,
                'non_empty': non_empty_count,
                'total': len(travis_properties),
                'presence_rate': (present_count / len(travis_properties)) * 100,
                'data_quality_rate': (non_empty_count / len(travis_properties)) * 100
            }
        
        # Display field statistics
        for field, stats in field_stats.items():
            print(f"   {stats['display_name']}:")
            print(f"     Present: {stats['present']}/{stats['total']} ({stats['presence_rate']:.1f}%)")
            print(f"     With Data: {stats['non_empty']}/{stats['total']} ({stats['data_quality_rate']:.1f}%)")
        
        # Check specific sub-fields for key structures
        print(f"\n📋 Detailed Field Analysis:")
        
        # Property Address analysis
        if 'property_address' in field_stats and field_stats['property_address']['present'] > 0:
            addr_fields = ['street_address', 'city', 'state', 'zip_code']
            print(f"   Property Address Sub-fields:")
            for addr_field in addr_fields:
                present = sum(1 for p in travis_properties 
                            if 'property_address' in p and p['property_address'] 
                            and addr_field in p['property_address'] 
                            and p['property_address'][addr_field])
                print(f"     {addr_field}: {present}/{len(travis_properties)} ({present/len(travis_properties)*100:.1f}%)")
        
        # Mailing Address analysis
        if 'mailing_address' in field_stats and field_stats['mailing_address']['present'] > 0:
            mail_fields = ['name', 'address_line_1', 'city', 'state', 'zip_code']
            print(f"   Mailing Address Sub-fields:")
            for mail_field in mail_fields:
                present = sum(1 for p in travis_properties 
                            if 'mailing_address' in p and p['mailing_address'] 
                            and mail_field in p['mailing_address'] 
                            and p['mailing_address'][mail_field])
                print(f"     {mail_field}: {present}/{len(travis_properties)} ({present/len(travis_properties)*100:.1f}%)")
        
        # Valuation analysis
        if 'valuation' in field_stats and field_stats['valuation']['present'] > 0:
            val_fields = ['market_value', 'assessed_value', 'total_market_value']
            print(f"   Valuation Sub-fields:")
            for val_field in val_fields:
                present = sum(1 for p in travis_properties 
                            if 'valuation' in p and p['valuation'] 
                            and val_field in p['valuation'] 
                            and p['valuation'][val_field])
                print(f"     {val_field}: {present}/{len(travis_properties)} ({present/len(travis_properties)*100:.1f}%)")
        
        # Show sample properties with missing data
        print(f"\n⚠️  Properties with Missing Key Data:")
        missing_data_examples = []
        
        for i, prop in enumerate(travis_properties):
            missing_fields = []
            for field in key_fields:
                if field not in prop or not prop[field]:
                    missing_fields.append(field)
            
            if missing_fields:
                missing_data_examples.append({
                    'index': i,
                    'account_id': prop.get('account_id', 'N/A'),
                    'missing_fields': missing_fields,
                    'property': prop
                })
        
        # Show top 5 examples of missing data
        for example in missing_data_examples[:5]:
            print(f"   Property {example['index'] + 1} (Account: {example['account_id']}):")
            print(f"     Missing: {', '.join(example['missing_fields'])}")
        
        if len(missing_data_examples) > 5:
            print(f"   ... and {len(missing_data_examples) - 5} more properties with missing data")
        
        # Prepare comprehensive results
        results = {
            'timestamp': datetime.now().isoformat(),
            'sample_size': len(travis_properties),
            'county': 'travis',
            'field_statistics': field_stats,
            'missing_data_examples': missing_data_examples,
            'sample_properties': travis_properties,
            'data_quality_summary': {
                'total_properties': len(travis_properties),
                'properties_with_complete_data': sum(1 for stats in field_stats.values() if stats['data_quality_rate'] == 100),
                'average_data_completeness': sum(stats['data_quality_rate'] for stats in field_stats.values()) / len(field_stats)
            }
        }
        
        # Save to output directory
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"travis_sample_review_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {output_file}")
        
        # Show data quality summary
        print(f"\n📊 Data Quality Summary:")
        print(f"   Total Properties: {len(travis_properties):,}")
        print(f"   Average Data Completeness: {results['data_quality_summary']['average_data_completeness']:.1f}%")
        print(f"   Properties with Complete Data: {results['data_quality_summary']['properties_with_complete_data']:,}")
        
        print(f"\n✅ Travis County sample review complete!")
        print(f"📁 Output saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error during Travis County review: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        mongodb.disconnect()
        print("🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    review_travis_sample()
