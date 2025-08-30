#!/usr/bin/env python3
"""
Database Results Review Script

This script extracts the same data that the web app displays and saves it to the output directory.
It uses the exact same MongoDB queries and data processing logic as web_app.py.
"""

import json
import os
from datetime import datetime
from pathlib import Path

# Import our services
from county_parser.services.mongodb_service import MongoDBService
from county_parser.models.config import Config

def review_database_results():
    """Review database results using the same logic as web_app.py"""
    
    print("🏛️ Database Results Review")
    print("=" * 50)
    
    # Initialize MongoDB service
    config = Config()
    mongodb = MongoDBService()
    
    if not mongodb.connect():
        print("❌ Failed to connect to MongoDB")
        return
    
    try:
        print("✅ Connected to MongoDB successfully!")
        
        # Get collection stats (same as web app)
        stats = mongodb.get_collection_stats()
        print(f"📊 Total properties: {stats['properties_count']:,}")
        print(f"📝 Total logs: {stats['logs_count']:,}")
        
        # Get county distribution (same as web app)
        county_pipeline = [
            {'$group': {'_id': '$county', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]
        county_stats = list(mongodb.properties_collection.aggregate(county_pipeline))
        
        print(f"\n🏛️ County Distribution:")
        for county in county_stats:
            print(f"   {county['_id'].title()}: {county['count']:,} properties")
        
        # Get value statistics (same as web app)
        print(f"\n💰 Value Statistics (from 1000 sample):")
        value_pipeline = [
            {'$sample': {'size': 1000}},
            {'$match': {
                '$or': [
                    {'valuation.market_value': {'$exists': True, '$gt': 0, '$lt': 100000000}},
                    {'valuation.total_market_value': {'$exists': True, '$gt': 0, '$lt': 100000000}},
                    {'valuation.assessed_value': {'$exists': True, '$gt': 0, '$lt': 100000000}}
                ]
            }},
            {'$addFields': {
                'effective_value': {
                    '$cond': {
                        'if': {'$and': [
                            {'$gt': ['$valuation.market_value', 0]},
                            {'$lt': ['$valuation.market_value', 100000000]}
                        ]},
                        'then': '$valuation.market_value',
                        'else': {
                            '$cond': {
                                'if': {'$and': [
                                    {'$gt': ['$valuation.total_market_value', 0]},
                                    {'$lt': ['$valuation.total_market_value', 100000000]}
                                ]},
                                'then': '$valuation.total_market_value',
                                'else': '$valuation.assessed_value'
                                }
                            }
                        }
                    }
                }},
            {'$group': {
                '_id': None,
                'avg_value': {'$avg': '$effective_value'},
                'min_value': {'$min': '$effective_value'},
                'max_value': {'$max': '$effective_value'},
                'count': {'$sum': 1}
            }}
        ]
        
        try:
            value_stats = list(mongodb.properties_collection.aggregate(value_pipeline))
            if value_stats:
                vs = value_stats[0]
                print(f"   Average Value: ${vs.get('avg_value', 0):,.2f}")
                print(f"   Min Value: ${vs.get('min_value', 0):,.2f}")
                print(f"   Max Value: ${vs.get('max_value', 0):,.2f}")
                print(f"   Sample Count: {vs.get('count', 0):,}")
        except Exception as e:
            print(f"   Error in value stats: {e}")
        
        # Get sample properties (same as web app - 100 properties from all counties)
        print(f"\n🏠 Sample Properties (100 from all counties):")
        sample_pipeline = [
            {'$sample': {'size': 100}}
        ]
        
        sample_properties = list(mongodb.properties_collection.aggregate(sample_pipeline))
        
        # Convert ObjectId to string for JSON serialization (same as web app)
        for prop in sample_properties:
            if '_id' in prop:
                prop['_id'] = str(prop['_id'])
        
        print(f"   Retrieved: {len(sample_properties):,} properties")
        
        # Get county-specific samples (same as web app logic)
        print(f"\n🏛️ County-Specific Samples:")
        
        county_samples = {}
        for county in ['travis', 'dallas', 'harris']:
            county_pipeline = [
                {'$match': {'county': county}},
                {'$sample': {'size': 50}}  # 50 from each county
            ]
            
            county_props = list(mongodb.properties_collection.aggregate(county_pipeline))
            
            # Convert ObjectId to string
            for prop in county_props:
                if '_id' in prop:
                    prop['_id'] = str(prop['_id'])
            
            county_samples[county] = county_props
            print(f"   {county.title()}: {len(county_props):,} properties")
        
        # Prepare comprehensive results
        results = {
            'timestamp': datetime.now().isoformat(),
            'database_stats': {
                'total_properties': stats['properties_count'],
                'total_logs': stats['logs_count'],
                'counties': county_stats,
                'values': value_stats[0] if value_stats else {}
            },
            'sample_data': {
                'all_counties_sample': sample_properties,
                'county_specific_samples': county_samples
            },
            'data_quality': {
                'total_samples': len(sample_properties),
                'county_breakdown': {county: len(props) for county, props in county_samples.items()}
            }
        }
        
        # Save to output directory
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"database_review_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {output_file}")
        
        # Show sample data preview
        print(f"\n🔍 Sample Data Preview:")
        if sample_properties:
            sample = sample_properties[0]
            print(f"   Sample Property:")
            print(f"     Account ID: {sample.get('account_id', 'N/A')}")
            print(f"     County: {sample.get('county', 'N/A')}")
            
            # Show address if available
            if 'property_address' in sample:
                addr = sample['property_address']
                street = addr.get('street_address', 'N/A')
                city = addr.get('city', 'N/A')
                print(f"     Address: {street}, {city}")
            elif 'mailing_address' in sample:
                addr = sample['mailing_address']
                street = addr.get('address_line_1', 'N/A')
                city = addr.get('city', 'N/A')
                print(f"     Address: {street}, {city}")
            
            # Show owner if available
            if 'mailing_address' in sample and 'name' in sample['mailing_address']:
                print(f"     Owner: {sample['mailing_address']['name']}")
            
            # Show value if available
            if 'valuation' in sample:
                val = sample['valuation']
                if 'market_value' in val and val['market_value']:
                    print(f"     Market Value: ${val['market_value']:,.2f}")
                elif 'total_market_value' in val and val['total_market_value']:
                    print(f"     Total Market Value: ${val['total_market_value']:,.2f}")
                elif 'assessed_value' in val and val['assessed_value']:
                    print(f"     Assessed Value: ${val['assessed_value']:,.2f}")
        
        print(f"\n✅ Database review complete!")
        print(f"📁 Output saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error during database review: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        mongodb.disconnect()
        print("🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    review_database_results()
