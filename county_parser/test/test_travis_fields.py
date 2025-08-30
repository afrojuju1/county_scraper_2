#!/usr/bin/env python3
"""
Comprehensive test script for Travis County TXT file parsing.
Tests all available data files and validates field extraction.
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path
import time

# Get the absolute path to the parsers directory
parsers_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'parsers')
sys.path.insert(0, parsers_dir)

# Import directly from the file
import travis_field_specs

def analyze_file_structure(file_path: str, max_lines: int = 5) -> dict:
    """Analyze the structure of a TXT file."""
    analysis = {
        'file_path': file_path,
        'exists': False,
        'size_mb': 0,
        'line_count': 0,
        'line_length': 0,
        'sample_lines': [],
        'format': 'unknown'
    }
    
    if not os.path.exists(file_path):
        return analysis
    
    try:
        analysis['exists'] = True
        analysis['size_mb'] = round(os.path.getsize(file_path) / (1024 * 1024), 2)
        
        # Count lines and analyze structure
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = []
            line_count = 0
            
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                line = line.rstrip('\n\r')
                if line:  # Skip empty lines
                    lines.append(line)
                    if analysis['line_length'] == 0:
                        analysis['line_length'] = len(line)
                line_count += 1
            
            analysis['sample_lines'] = lines
            analysis['line_count'] = line_count
            
            # Determine format based on line length
            if analysis['line_length'] > 200:
                analysis['format'] = 'fixed_width'
            elif analysis['line_length'] < 100:
                analysis['format'] = 'delimited'
            else:
                analysis['format'] = 'mixed'
                
    except Exception as e:
        analysis['error'] = str(e)
    
    return analysis

def test_file_parsing(file_path: str, file_type: str, max_records: int = 10) -> dict:
    """Test parsing of a specific file type."""
    test_results = {
        'file_type': file_type,
        'file_path': file_path,
        'success': False,
        'records_extracted': 0,
        'sample_data': [],
        'errors': [],
        'processing_time': 0
    }
    
    if not os.path.exists(file_path):
        test_results['errors'].append("File does not exist")
        return test_results
    
    try:
        start_time = time.time()
        extractor = travis_field_specs.TravisFieldExtractor()
        
        # Different parsing strategies based on file type
        if file_type == 'properties':
            # Use property record extraction
            records = {}
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i >= max_records:
                        break
                    
                    if line.strip():
                        try:
                            record = extractor.extract_property_record(line)
                            if record and record.get('account_id'):
                                records[record['account_id']] = record
                        except Exception as e:
                            test_results['errors'].append(f"Line {i+1}: {str(e)}")
            
            test_results['records_extracted'] = len(records)
            test_results['sample_data'] = list(records.values())[:3]  # First 3 records
            
        elif file_type == 'property_entities':
            # Use entity record extraction
            records = []
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i >= max_records:
                        break
                    
                    if line.strip():
                        try:
                            record = extractor.extract_entity_record(line)
                            if record and record.get('account_id'):
                                records.append(record)
                        except Exception as e:
                            test_results['errors'].append(f"Line {i+1}: {str(e)}")
            
            test_results['records_extracted'] = len(records)
            test_results['sample_data'] = records[:3]  # First 3 records
            
        else:
            # For other file types, try basic account ID extraction
            records = []
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i >= max_records:
                        break
                    
                    if line.strip():
                        try:
                            account_id = extractor.get_account_id(line)
                            if account_id:
                                records.append({
                                    'account_id': account_id,
                                    'raw_line_preview': line[:100] + '...' if len(line) > 100 else line
                                })
                        except Exception as e:
                            test_results['errors'].append(f"Line {i+1}: {str(e)}")
            
            test_results['records_extracted'] = len(records)
            test_results['sample_data'] = records[:3]  # First 3 records
        
        test_results['processing_time'] = round(time.time() - start_time, 3)
        test_results['success'] = True
        
    except Exception as e:
        test_results['errors'].append(f"General error: {str(e)}")
    
    return test_results

def comprehensive_travis_test():
    """Run comprehensive testing of all Travis County TXT files."""
    
    print("🏛️ COMPREHENSIVE TRAVIS COUNTY DATA TESTING")
    print("=" * 80)
    
    # Get file paths
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    travis_data_dir = os.path.join(base_dir, 'data', 'travis_2025')
    
    # Define all the files we want to test
    test_files = {
        'properties': os.path.join(travis_data_dir, 'PROP.TXT'),
        'property_entities': os.path.join(travis_data_dir, 'PROP_ENT.TXT'),
        'improvements': os.path.join(travis_data_dir, 'IMP_DET.TXT'),
        'improvement_attributes': os.path.join(travis_data_dir, 'IMP_ATR.TXT'),
        'land_details': os.path.join(travis_data_dir, 'LAND_DET.TXT'),
        'improvement_info': os.path.join(travis_data_dir, 'IMP_INFO.TXT'),
        'agents': os.path.join(travis_data_dir, 'AGENT.TXT'),
        'subdivisions': os.path.join(travis_data_dir, 'ABS_SUBD.TXT'),
        'lawsuits': os.path.join(travis_data_dir, 'LAWSUIT.TXT'),
        'arbitration': os.path.join(travis_data_dir, 'ARB.TXT'),
        'mobile_homes': os.path.join(travis_data_dir, 'MOBILE_HOME_INFO.TXT'),
        'entity': os.path.join(travis_data_dir, 'ENTITY.TXT'),
        'country': os.path.join(travis_data_dir, 'COUNTRY.TXT'),
        'state_codes': os.path.join(travis_data_dir, 'STATE_CD.TXT'),
        'totals': os.path.join(travis_data_dir, 'TOTALS.TXT'),
        'tax_deferral': os.path.join(travis_data_dir, 'TAX_DEFERRAL_INFO.TXT'),
        'appraisal_header': os.path.join(travis_data_dir, 'APPR_HDR.TXT')
    }
    
    # Step 1: Analyze all file structures
    print("\n📁 STEP 1: Analyzing file structures...")
    print("-" * 50)
    
    file_analyses = {}
    for file_type, file_path in test_files.items():
        print(f"Analyzing {file_type}...")
        analysis = analyze_file_structure(file_path)
        file_analyses[file_type] = analysis
        
        if analysis['exists']:
            print(f"  ✅ {file_type}: {analysis['size_mb']} MB, {analysis['line_count']:,} lines, {analysis['format']}")
        else:
            print(f"  ❌ {file_type}: File not found")
    
    # Step 2: Test parsing of key files
    print("\n🔍 STEP 2: Testing file parsing...")
    print("-" * 50)
    
    # Focus on the most important files first
    priority_files = ['properties', 'property_entities', 'improvements', 'land_details', 'agents']
    
    parsing_results = {}
    for file_type in priority_files:
        if file_type in test_files:
            print(f"\nTesting {file_type} parsing...")
            result = test_file_parsing(test_files[file_type], file_type, max_records=20)
            parsing_results[file_type] = result
            
            if result['success']:
                print(f"  ✅ {file_type}: {result['records_extracted']} records in {result['processing_time']}s")
                if result['errors']:
                    print(f"     ⚠️ {len(result['errors'])} errors encountered")
            else:
                print(f"  ❌ {file_type}: Failed to parse")
                for error in result['errors'][:3]:  # Show first 3 errors
                    print(f"     • {error}")
    
    # Step 3: Test basic parsing of other files
    print("\n🔍 STEP 3: Testing other file types...")
    print("-" * 50)
    
    other_files = [ft for ft in test_files.keys() if ft not in priority_files]
    for file_type in other_files:
        if file_type in test_files:
            print(f"Testing {file_type}...")
            result = test_file_parsing(test_files[file_type], file_type, max_records=10)
            parsing_results[file_type] = result
            
            if result['success']:
                print(f"  ✅ {file_type}: {result['records_extracted']} records")
            else:
                print(f"  ❌ {file_type}: Failed")
    
    # Step 4: Generate comprehensive report
    print("\n📊 STEP 4: Generating comprehensive report...")
    print("-" * 50)
    
    # Create output data
    output_data = {
        "metadata": {
            "test_date": datetime.now().isoformat(),
            "test_type": "comprehensive_travis_parsing",
            "total_files_tested": len(test_files),
            "successful_parses": len([r for r in parsing_results.values() if r['success']]),
            "total_records_extracted": sum([r['records_extracted'] for r in parsing_results.values() if r['success']])
        },
        "file_analyses": file_analyses,
        "parsing_results": parsing_results,
        "summary": {
            "working_files": [ft for ft, result in parsing_results.items() if result['success']],
            "problematic_files": [ft for ft, result in parsing_results.items() if not result['success']],
            "recommendations": []
        }
    }
    
    # Generate recommendations
    if len([r for r in parsing_results.values() if r['success']]) > 10:
        output_data['summary']['recommendations'].append(
            "Most files are parsing successfully - Travis County parser is working well"
        )
    
    if any('properties' in ft for ft in parsing_results.keys()):
        prop_result = parsing_results.get('properties', {})
        if prop_result.get('success') and prop_result.get('records_extracted', 0) > 0:
            output_data['summary']['recommendations'].append(
                f"Property file parsing successful - extracted {prop_result['records_extracted']} records"
            )
    
    # Save comprehensive report
    output_file = os.path.join(base_dir, 'output', 'travis_comprehensive_test_report.json')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Comprehensive report saved to: {output_file}")
    
    # Display summary
    print("\n" + "=" * 80)
    print("📊 TESTING SUMMARY")
    print("=" * 80)
    
    working_files = [ft for ft, result in parsing_results.items() if result['success']]
    problematic_files = [ft for ft, result in parsing_results.items() if not result['success']]
    
    print(f"✅ Working files: {len(working_files)}")
    for ft in working_files:
        result = parsing_results[ft]
        print(f"   • {ft}: {result['records_extracted']} records")
    
    if problematic_files:
        print(f"\n❌ Problematic files: {len(problematic_files)}")
        for ft in problematic_files:
            print(f"   • {ft}")
    
    print(f"\n📁 Total files analyzed: {len(file_analyses)}")
    print(f"📊 Total records extracted: {output_data['metadata']['total_records_extracted']:,}")
    print(f"💾 Report saved to: {output_file}")
    
    print("\n" + "=" * 80)
    print("🎉 Comprehensive testing completed!")

if __name__ == "__main__":
    comprehensive_travis_test()
