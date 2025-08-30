#!/usr/bin/env python3
"""
Comprehensive Dataset Validation Script

This script validates the entire multi-county dataset and identifies:
- Missing data fields
- Data quality issues
- Field completeness across counties
- Data structure consistency
"""

import json
import os
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Import our services
from ..services.mongodb_service import MongoDBService
from ..models.config import Config

def validate_dataset():
    """Comprehensive validation of the multi-county dataset."""
    
    print("🔍 Comprehensive Dataset Validation")
    print("=" * 60)
    
    # Initialize MongoDB service
    config = Config()
    mongodb = MongoDBService()
    
    if not mongodb.connect():
        print("❌ Failed to connect to MongoDB")
        return
    
    try:
        print("✅ Connected to MongoDB successfully!")
        
        # Get overall stats
        stats = mongodb.get_collection_stats()
        print(f"📊 Total properties: {stats['properties_count']:,}")
        print(f"📝 Total logs: {stats['logs_count']:,}")
        
        # Get county distribution
        county_pipeline = [
            {'$group': {'_id': '$county', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]
        county_stats = list(mongodb.properties_collection.aggregate(county_pipeline))
        
        print(f"\n🏛️ County Distribution:")
        for county in county_stats:
            print(f"   {county['_id'].title()}: {county['count']:,} properties")
        
        # Define comprehensive field validation schema
        validation_schema = {
            'core_fields': {
                'account_id': 'Account ID',
                'county': 'County',
                'year': 'Year'
            },
            'address_fields': {
                'property_address': 'Property Address',
                'mailing_address': 'Mailing Address'
            },
            'financial_fields': {
                'valuation': 'Valuation'
            },
            'legal_fields': {
                'legal_status': 'Legal Status'
            },
            'entity_fields': {
                'tax_entities': 'Tax Entities'
            },
            'detail_fields': {
                'improvements': 'Improvements',
                'land_details': 'Land Details',
                'property_details': 'Property Details'
            }
        }
        
        # Collect validation data for each county
        validation_results = {}
        
        for county_info in county_stats:
            county = county_info['_id']
            count = county_info['count']
            
            print(f"\n🔍 Validating {county.title()} County ({count:,} properties)...")
            
            # Sample up to 100 properties for validation (to avoid memory issues)
            sample_size = min(100, count)
            
            county_pipeline = [
                {'$match': {'county': county}},
                {'$sample': {'size': sample_size}}
            ]
            
            county_properties = list(mongodb.properties_collection.aggregate(county_pipeline))
            
            # Analyze field presence and data quality
            field_analysis = {}
            
            for category, fields in validation_schema.items():
                category_stats = {}
                
                for field, display_name in fields.items():
                    present_count = 0
                    non_empty_count = 0
                    data_examples = []
                    
                    for prop in county_properties:
                        if field in prop:
                            present_count += 1
                            
                            # Check if field has actual data
                            field_value = prop[field]
                            
                            if isinstance(field_value, dict) and field_value:
                                non_empty_count += 1
                                if len(data_examples) < 3:  # Keep first 3 examples
                                    data_examples.append(field_value)
                            elif isinstance(field_value, list) and field_value:
                                non_empty_count += 1
                                if len(data_examples) < 3:
                                    data_examples.append(field_value)
                            elif field_value and str(field_value).strip():
                                non_empty_count += 1
                                if len(data_examples) < 3:
                                    data_examples.append(field_value)
                    
                    category_stats[field] = {
                        'display_name': display_name,
                        'present': present_count,
                        'non_empty': non_empty_count,
                        'total': len(county_properties),
                        'presence_rate': (present_count / len(county_properties)) * 100,
                        'data_quality_rate': (non_empty_count / len(county_properties)) * 100,
                        'data_examples': data_examples
                    }
                
                field_analysis[category] = category_stats
            
            # Store results
            validation_results[county] = {
                'total_properties': count,
                'sample_size': sample_size,
                'field_analysis': field_analysis
            }
        
        # Generate comprehensive validation report
        print(f"\n📋 Validation Report Summary:")
        print("=" * 60)
        
        # Overall data quality metrics
        total_fields = sum(len(fields) for fields in validation_schema.values())
        overall_quality = {}
        
        for county, results in validation_results.items():
            county_quality = []
            for category, fields in results['field_analysis'].items():
                for field, stats in fields.items():
                    county_quality.append(stats['data_quality_rate'])
            
            avg_quality = sum(county_quality) / len(county_quality) if county_quality else 0
            overall_quality[county] = avg_quality
            
            print(f"\n🏛️ {county.title()} County:")
            print(f"   Overall Data Quality: {avg_quality:.1f}%")
            print(f"   Properties: {results['total_properties']:,}")
            print(f"   Sample Size: {results['sample_size']:,}")
            
            # Show category breakdown
            for category, fields in results['field_analysis'].items():
                category_quality = sum(fields[f]['data_quality_rate'] for f in fields) / len(fields)
                print(f"   {category.replace('_', ' ').title()}: {category_quality:.1f}%")
        
        # Identify critical missing data
        print(f"\n⚠️  Critical Data Gaps:")
        print("=" * 60)
        
        critical_issues = []
        for county, results in validation_results.items():
            for category, fields in results['field_analysis'].items():
                for field, stats in fields.items():
                    if stats['data_quality_rate'] < 50:  # Less than 50% data quality
                        critical_issues.append({
                            'county': county,
                            'category': category,
                            'field': field,
                            'display_name': stats['display_name'],
                            'quality_rate': stats['data_quality_rate'],
                            'sample_size': results['sample_size']
                        })
        
        # Sort by severity (lowest quality first)
        critical_issues.sort(key=lambda x: x['quality_rate'])
        
        for issue in critical_issues[:10]:  # Show top 10 most critical
            print(f"   {issue['county'].title()} - {issue['display_name']}: {issue['quality_rate']:.1f}%")
        
        if len(critical_issues) > 10:
            print(f"   ... and {len(critical_issues) - 10} more critical issues")
        
        # Data structure analysis
        print(f"\n🏗️  Data Structure Analysis:")
        print("=" * 60)
        
        # Check for consistent data structures across counties
        structure_analysis = {}
        for county, results in validation_results.items():
            sample_prop = None
            for prop in county_properties:
                if prop['county'] == county:
                    sample_prop = prop
                    break
            
            if sample_prop:
                structure_analysis[county] = {
                    'total_fields': len(sample_prop),
                    'field_names': list(sample_prop.keys()),
                    'nested_structures': {k: type(v).__name__ for k, v in sample_prop.items() if isinstance(v, (dict, list))}
                }
        
        for county, structure in structure_analysis.items():
            print(f"\n   {county.title()} County Structure:")
            print(f"     Total Fields: {structure['total_fields']}")
            print(f"     Nested Objects: {len(structure['nested_structures'])}")
            for field, field_type in structure['nested_structures'].items():
                print(f"       {field}: {field_type}")
        
        # Prepare comprehensive results
        validation_report = {
            'timestamp': datetime.now().isoformat(),
            'overview': {
                'total_properties': stats.get('properties_count', 0),
                'total_logs': stats.get('logs_count', 0),
                'counties_analyzed': len(validation_results)
            },
            'county_distribution': county_stats,
            'validation_results': validation_results,
            'overall_quality': overall_quality,
            'critical_issues': critical_issues,
            'structure_analysis': structure_analysis,
            'recommendations': generate_recommendations(validation_results, critical_issues)
        }
        
        # Save comprehensive report
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"dataset_validation_report_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(validation_report, f, indent=2, default=str)
        
        print(f"\n💾 Validation report saved to: {output_file}")
        
        # Final summary
        print(f"\n🎯 Validation Complete!")
        print("=" * 60)
        print(f"📊 Overall Dataset Quality:")
        for county, quality in overall_quality.items():
            print(f"   {county.title()}: {quality:.1f}%")
        
        avg_overall = sum(overall_quality.values()) / len(overall_quality)
        print(f"   Overall Average: {avg_overall:.1f}%")
        
        print(f"\n📁 Full report: {output_file}")
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        mongodb.disconnect()
        print("🔌 Disconnected from MongoDB")

def generate_recommendations(validation_results, critical_issues):
    """Generate recommendations based on validation results."""
    
    recommendations = []
    
    # Data completeness recommendations
    for county, results in validation_results.items():
        low_quality_fields = []
        for category, fields in results['field_analysis'].items():
            for field, stats in fields.items():
                if stats['data_quality_rate'] < 30:
                    low_quality_fields.append(f"{stats['display_name']} ({stats['data_quality_rate']:.1f}%)")
        
        if low_quality_fields:
            recommendations.append({
                'county': county,
                'type': 'data_completeness',
                'priority': 'high',
                'message': f"Critical data gaps in {county.title()} County: {', '.join(low_quality_fields)}",
                'action': "Review data extraction logic and source file completeness"
            })
    
    # Cross-county consistency recommendations
    if len(validation_results) > 1:
        recommendations.append({
            'county': 'all',
            'type': 'consistency',
            'priority': 'medium',
            'message': "Ensure consistent data structure across all counties",
            'action': "Standardize field names and data formats"
        })
    
    # Performance recommendations
    total_properties = sum(r['total_properties'] for r in validation_results.values())
    if total_properties > 10000:
        recommendations.append({
            'county': 'all',
            'type': 'performance',
            'priority': 'low',
            'message': f"Large dataset ({total_properties:,} properties) - consider indexing optimization",
            'action': "Review MongoDB indexes and query optimization"
        })
    
    return recommendations

if __name__ == "__main__":
    validate_dataset()
