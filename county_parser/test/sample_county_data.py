#!/usr/bin/env python3
"""
Sample random properties from each county and save to JSON for analysis.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add the county_parser to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from county_parser.services.mongodb_service import MongoDBService

def sample_county_data():
    """Sample 50 random properties from each county and save to JSON."""
    print("🎯 Sampling Random County Data")
    print("=" * 50)
    
    try:
        # Connect to MongoDB
        mongo_service = MongoDBService()
        if not mongo_service.connect():
            print("❌ Failed to connect to MongoDB")
            return False
        
        # Get collection stats
        stats = mongo_service.get_collection_stats()
        total_properties = stats.get('properties_count', 0)
        print(f"📊 Total properties in database: {total_properties:,}")
        
        # Sample data for each county
        county_samples = {}
        
        counties = ['travis', 'dallas', 'harris']
        
        for county in counties:
            print(f"\n🏛️ Sampling {county.title()} County...")
            
            # Get 50 random properties from this county
            sample_properties = mongo_service.query_properties(
                filter_query={"county": county}, 
                limit=50
            )
            
            if sample_properties:
                county_samples[county] = {
                    'count': len(sample_properties),
                    'properties': sample_properties
                }
                print(f"   ✅ Sampled {len(sample_properties)} properties")
                
                # Show sample of property types based on county structure
                property_types = {}
                for prop in sample_properties:
                    prop_details = prop.get('property_details', {})
                    
                    # Different counties use different field names for property type
                    if county == 'travis':
                        prop_type = prop_details.get('property_type', 'Unknown')
                    elif county == 'dallas':
                        prop_type = prop_details.get('division_code', 'Unknown')
                    elif county == 'harris':
                        prop_type = prop_details.get('state_class', 'Unknown')
                    else:
                        prop_type = 'Unknown'
                    
                    property_types[prop_type] = property_types.get(prop_type, 0) + 1
                
                print(f"   📋 Property types: {dict(property_types)}")
            else:
                print(f"   ❌ No properties found for {county}")
                county_samples[county] = {'count': 0, 'properties': []}
        
        # Create output data structure
        output_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_properties_sampled': sum(data['count'] for data in county_samples.values()),
                'sampling_method': 'random_sample_50_per_county',
                'database_stats': stats
            },
            'county_samples': county_samples
        }
        
        # Save to output directory
        output_file = Path("output") / f"county_samples_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_file.parent.mkdir(exist_ok=True)
        
        # Convert MongoDB objects to JSON-serializable format
        serializable_data = convert_to_serializable(output_data)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Sample data saved to: {output_file}")
        print(f"📊 Total properties sampled: {output_data['metadata']['total_properties_sampled']}")
        
        # Display summary
        print(f"\n📋 Sampling Summary:")
        for county, data in county_samples.items():
            print(f"   {county.title()}: {data['count']} properties")
        
        # Disconnect from MongoDB
        mongo_service.disconnect()
        
        return True
        
    except Exception as e:
        print(f"❌ Sampling failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

def convert_to_serializable(obj):
    """Convert MongoDB objects to JSON-serializable format."""
    if isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif hasattr(obj, '__class__') and obj.__class__.__name__ == 'ObjectId':
        return str(obj)
    elif hasattr(obj, 'isoformat'):  # datetime objects
        return obj.isoformat()
    else:
        return obj

if __name__ == "__main__":
    success = sample_county_data()
    sys.exit(0 if success else 1)
