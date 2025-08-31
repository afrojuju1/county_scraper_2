#!/usr/bin/env python3
"""
Analyze the sampled county data and provide insights.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

def analyze_county_samples(sample_file_path):
    """Analyze the sampled county data and provide insights."""
    print("🔍 Analyzing County Sample Data")
    print("=" * 50)
    
    try:
        # Load the sample data
        with open(sample_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        metadata = data.get('metadata', {})
        county_samples = data.get('county_samples', {})
        
        print(f"📊 Sample Metadata:")
        print(f"   Generated: {metadata.get('generated_at', 'Unknown')}")
        print(f"   Total Properties: {metadata.get('total_properties_sampled', 0):,}")
        print(f"   Sampling Method: {metadata.get('sampling_method', 'Unknown')}")
        
        # Analyze each county
        for county_name, county_data in county_samples.items():
            print(f"\n🏛️ {county_name.upper()} COUNTY ANALYSIS:")
            print(f"   📈 Total Properties: {county_data['count']:,}")
            
            properties = county_data.get('properties', [])
            if not properties:
                print("   ❌ No properties to analyze")
                continue
            
            # Property type analysis
            property_types = defaultdict(int)
            cities = defaultdict(int)
            zip_codes = defaultdict(int)
            improvement_counts = defaultdict(int)
            entity_counts = defaultdict(int)
            
            for prop in properties:
                # Property types
                prop_details = prop.get('property_details', {})
                if county_name == 'travis':
                    prop_type = prop_details.get('property_type', 'Unknown')
                elif county_name == 'dallas':
                    prop_type = prop_details.get('division_code', 'Unknown')
                elif county_name == 'harris':
                    prop_type = prop_details.get('state_class', 'Unknown')
                else:
                    prop_type = 'Unknown'
                property_types[prop_type] += 1
                
                # Cities
                prop_address = prop.get('property_address', {})
                city = prop_address.get('city', 'Unknown')
                cities[city] += 1
                
                # ZIP codes
                zip_code = prop_address.get('zip_code', 'Unknown')
                zip_codes[zip_code] += 1
                
                # Improvements count
                improvements = prop.get('improvements', [])
                improvement_counts[len(improvements)] += 1
                
                # Entity count
                owners = prop.get('owners', [])
                entity_counts[len(owners)] += 1
            
            # Display property type breakdown
            print(f"   🏠 Property Types:")
            for prop_type, count in sorted(property_types.items()):
                percentage = (count / len(properties)) * 100
                print(f"      {prop_type}: {count} ({percentage:.1f}%)")
            
            # Display top cities
            print(f"   🏙️ Top Cities:")
            top_cities = sorted(cities.items(), key=lambda x: x[1], reverse=True)[:5]
            for city, count in top_cities:
                percentage = (count / len(properties)) * 100
                print(f"      {city}: {count} ({percentage:.1f}%)")
            
            # Display improvement distribution
            print(f"   🏗️ Improvement Distribution:")
            for count, prop_count in sorted(improvement_counts.items()):
                percentage = (prop_count / len(properties)) * 100
                print(f"      {count} improvements: {prop_count} properties ({percentage:.1f}%)")
            
            # Display entity distribution
            print(f"   👥 Entity Distribution:")
            for count, prop_count in sorted(entity_counts.items()):
                percentage = (prop_count / len(properties)) * 100
                print(f"      {count} entities: {prop_count} properties ({percentage:.1f}%)")
            
            # Sample property details
            print(f"   📋 Sample Property Details:")
            sample_prop = properties[0]
            print(f"      Account ID: {sample_prop.get('account_id', 'N/A')}")
            print(f"      Address: {sample_prop.get('property_address', {}).get('street_address', 'N/A')}")
            print(f"      City: {sample_prop.get('property_address', {}).get('city', 'N/A')}")
            
            # Check for special features
            has_improvements = any(prop.get('improvements') for prop in properties)
            has_land_details = any(prop.get('land_details') for prop in properties)
            has_agents = any(prop.get('agents') for prop in properties)
            
            print(f"   🔧 Data Features:")
            print(f"      Improvements: {'✅' if has_improvements else '❌'}")
            print(f"      Land Details: {'✅' if has_land_details else '❌'}")
            print(f"      Agents: {'✅' if has_agents else '❌'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Analysis failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_samples.py <sample_file_path>")
        sys.exit(1)
    
    sample_file = sys.argv[1]
    success = analyze_county_samples(sample_file)
    sys.exit(0 if success else 1)
