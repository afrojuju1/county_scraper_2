#!/usr/bin/env python3
"""
Test script for enhanced Dallas County parser.
Shows the new features: commercial details, enhanced improvements, and better land details.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from services.mongodb_service import MongoDBService
from models.config import Config
import json
from datetime import datetime

def test_dallas_enhanced():
    """Test the enhanced Dallas County parser features."""
    print("🔍 Testing Enhanced Dallas County Parser")
    print("=" * 50)
    
    try:
        # Connect to MongoDB
        mongo_service = MongoDBService()
        if not mongo_service.connect():
            print("❌ Failed to connect to MongoDB")
            return False
        
        # Get Dallas County sample
        print("🏛️ Sampling Dallas County properties...")
        dallas_properties = mongo_service.query_properties(
            filter_query={"county": "dallas"}, 
            limit=10
        )
        
        if not dallas_properties:
            print("❌ No Dallas County properties found")
            return False
        
        print(f"✅ Found {len(dallas_properties)} Dallas County properties")
        
        # Analyze the enhanced features
        print("\n🔍 Analyzing Enhanced Features:")
        print("-" * 30)
        
        commercial_count = 0
        enhanced_improvements = 0
        enhanced_land_details = 0
        property_types = {}
        
        for i, prop in enumerate(dallas_properties[:5]):  # Show first 5
            print(f"\n📋 Property {i+1}: {prop.get('account_id', 'N/A')}")
            
            # Check property type mapping
            prop_details = prop.get('property_details', {})
            prop_type = prop_details.get('property_type', 'Unknown')
            property_types[prop_type] = property_types.get(prop_type, 0) + 1
            print(f"   🏠 Property Type: {prop_type}")
            
            # Check commercial details
            if prop_details.get('commercial_area_sf'):
                commercial_count += 1
                print(f"   🏢 Commercial Area: {prop_details.get('commercial_area_sf'):,} sq ft")
                print(f"   🏢 Property Name: {prop_details.get('property_name', 'N/A')}")
                print(f"   🏢 Quality: {prop_details.get('property_quality', 'N/A')}")
            
            # Check enhanced improvements
            improvements = prop.get('improvements', [])
            if improvements:
                enhanced_improvements += 1
                print(f"   🏗️ Improvements: {len(improvements)} found")
                
                for imp in improvements:
                    if imp.get('amenities'):
                        print(f"      🎯 Amenities: Pool={imp['amenities'].get('pool', False)}, "
                              f"Spa={imp['amenities'].get('spa', False)}, "
                              f"Deck={imp['amenities'].get('deck', False)}")
                    
                    if imp.get('exterior_features'):
                        print(f"      🏠 Exterior: {imp['exterior_features'].get('roof_type', 'N/A')} roof, "
                              f"{imp['exterior_features'].get('exterior_wall', 'N/A')} walls")
            
            # Check enhanced land details
            land_details = prop.get('land_details', [])
            if land_details:
                enhanced_land_details += 1
                print(f"   🌍 Land Details: {len(land_details)} found")
                
                for land in land_details:
                    if land.get('zoning'):
                        print(f"      📍 Zoning: {land.get('zoning', 'N/A')}")
                    if land.get('sptd_description'):
                        print(f"      🏞️ Land Class: {land.get('sptd_description', 'N/A')}")
        
        # Summary
        print(f"\n📊 Enhanced Features Summary:")
        print(f"   🏢 Commercial Properties: {commercial_count}/5")
        print(f"   🏗️ Enhanced Improvements: {enhanced_improvements}/5")
        print(f"   🌍 Enhanced Land Details: {enhanced_land_details}/5")
        print(f"   🏠 Property Types: {dict(property_types)}")
        
        # Save sample to file
        output_file = f"output/dallas_enhanced_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs("output", exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(dallas_properties[:5], f, indent=2, default=str)
        
        print(f"\n💾 Sample data saved to: {output_file}")
        
        # Disconnect
        mongo_service.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_dallas_enhanced()
