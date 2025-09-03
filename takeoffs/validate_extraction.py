#!/usr/bin/env python3
"""
Validation Script for Construction Plan Extraction Results

This script validates the JSON output from tesseract_takeoff.py to identify
accuracy issues and track improvements over time.

Usage:
    python validate_extraction.py <json_file>
    python validate_extraction.py --all
    python validate_extraction.py --batch <batch_summary_file>
"""

import json
import argparse
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExtractionValidator:
    """Validates construction plan extraction results"""
    
    def __init__(self):
        self.validation_results = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warnings': 0,
            'critical_issues': [],
            'warnings_list': [],
            'score': 0.0
        }
        
        # Define realistic ranges for validation
        self.validation_rules = {
            'total_sqft': {
                'min': 500,
                'max': 10000,
                'description': 'Total square footage should be 500-10,000 sqft'
            },
            'doors': {
                'min': 3,
                'max': 25,
                'description': 'Door count should be 3-25 for residential'
            },
            'windows': {
                'min': 4,
                'max': 40,
                'description': 'Window count should be 4-40 for residential'
            },
            'electrical_outlets': {
                'min': 10,
                'max': 100,
                'description': 'Electrical outlets should be 10-100'
            },
            'plumbing_fixtures': {
                'min': 2,
                'max': 20,
                'description': 'Plumbing fixtures should be 2-20'
            },
            'ceiling_height': {
                'min': 8.0,
                'max': 12.0,
                'description': 'Ceiling height should be 8-12 feet'
            },
            'room_area': {
                'min': 50,
                'max': 500,
                'description': 'Individual room area should be 50-500 sqft'
            }
        }
    
    def validate_single_file(self, json_file_path: str) -> Dict:
        """Validate a single JSON extraction file"""
        logger.info(f"Validating: {json_file_path}")
        
        try:
            with open(json_file_path, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.error(f"File not found: {json_file_path}")
            return {'error': 'File not found'}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {json_file_path} - {e}")
            return {'error': 'Invalid JSON'}
        
        # Reset validation results for this file
        self.validation_results = {
            'file': json_file_path,
            'timestamp': datetime.now().isoformat(),
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warnings': 0,
            'critical_issues': [],
            'warnings_list': [],
            'score': 0.0
        }
        
        # Run all validation checks
        self._validate_basic_data(data)
        self._validate_room_data(data)
        self._validate_fixture_data(data)
        self._validate_structural_data(data)
        self._validate_system_data(data)
        self._validate_material_data(data)
        self._validate_consistency(data)
        
        # Calculate final score
        self._calculate_score()
        
        return self.validation_results
    
    def _validate_basic_data(self, data: Dict):
        """Validate basic extraction data"""
        logger.info("Validating basic data...")
        
        # Check total square footage
        self._check_range(
            data.get('total_sqft', 0),
            'total_sqft',
            'Total square footage'
        )
        
        # Check if total sqft is present
        if not data.get('total_sqft'):
            self._add_critical_issue("Missing total square footage")
        elif data.get('total_sqft') == 0:
            self._add_critical_issue("Total square footage is zero")
        
        # Check floor areas
        floor_areas = data.get('floor_areas', {})
        if not floor_areas:
            self._add_warning("No floor areas extracted")
        else:
            total_floor_area = sum(floor_areas.values())
            if total_floor_area > 0:
                self._check_consistency(
                    data.get('total_sqft', 0),
                    total_floor_area,
                    'Total sqft vs floor areas',
                    tolerance=0.3  # 30% tolerance
                )
    
    def _validate_room_data(self, data: Dict):
        """Validate room extraction data"""
        logger.info("Validating room data...")
        
        rooms = data.get('rooms', {})
        total_sqft = data.get('total_sqft', 0)
        
        # Check if rooms were extracted
        if not rooms:
            self._add_critical_issue("No rooms extracted - critical for material takeoffs")
        else:
            # Check room count vs house size
            room_count = len(rooms)
            expected_rooms = self._estimate_expected_rooms(total_sqft)
            
            if room_count < expected_rooms * 0.5:
                self._add_critical_issue(f"Too few rooms extracted: {room_count} (expected ~{expected_rooms})")
            elif room_count > expected_rooms * 2:
                self._add_warning(f"Unusually high room count: {room_count}")
            
            # Validate individual room areas
            total_room_area = 0
            for room_name, room_data in rooms.items():
                if isinstance(room_data, dict) and 'area' in room_data:
                    area = room_data['area']
                    total_room_area += area
                    
                    self._check_range(
                        area,
                        'room_area',
                        f"Room {room_name} area"
                    )
            
            # Check room area vs total sqft
            if total_room_area > 0 and total_sqft > 0:
                room_percentage = total_room_area / total_sqft
                if room_percentage < 0.3:
                    self._add_warning(f"Room areas ({total_room_area}) are only {room_percentage:.1%} of total sqft")
                elif room_percentage > 1.2:
                    self._add_critical_issue(f"Room areas ({total_room_area}) exceed total sqft by {(room_percentage-1)*100:.1f}%")
    
    def _validate_fixture_data(self, data: Dict):
        """Validate fixture extraction data"""
        logger.info("Validating fixture data...")
        
        fixtures = data.get('fixtures', {})
        total_sqft = data.get('total_sqft', 0)
        
        # Check each fixture type
        for fixture_type, count in fixtures.items():
            if fixture_type in self.validation_rules:
                self._check_range(
                    count,
                    fixture_type,
                    f"{fixture_type.replace('_', ' ').title()}"
                )
        
        # Check for unrealistic fixture counts
        if fixtures.get('doors', 0) > 50:
            self._add_critical_issue(f"Unrealistic door count: {fixtures.get('doors')}")
        
        if fixtures.get('windows', 0) > 50:
            self._add_critical_issue(f"Unrealistic window count: {fixtures.get('windows')}")
        
        # Check fixture density
        if total_sqft > 0:
            door_density = fixtures.get('doors', 0) / (total_sqft / 1000)
            window_density = fixtures.get('windows', 0) / (total_sqft / 1000)
            
            if door_density > 15:  # More than 15 doors per 1000 sqft
                self._add_critical_issue(f"Excessive door density: {door_density:.1f} doors per 1000 sqft")
            
            if window_density > 20:  # More than 20 windows per 1000 sqft
                self._add_critical_issue(f"Excessive window density: {window_density:.1f} windows per 1000 sqft")
    
    def _validate_structural_data(self, data: Dict):
        """Validate structural details"""
        logger.info("Validating structural data...")
        
        structural = data.get('structural_details', {})
        
        # Check foundation
        foundation = structural.get('foundation', {})
        if not foundation.get('type'):
            self._add_warning("No foundation type extracted")
        
        # Check roof
        roof = structural.get('roof', {})
        if not roof.get('type'):
            self._add_warning("No roof type extracted")
        
        # Check for ceiling heights
        ceiling_heights = data.get('ceiling_heights', [])
        if not ceiling_heights:
            self._add_warning("No ceiling heights extracted - needed for wall area calculations")
        else:
            for height in ceiling_heights:
                height_ft = self._parse_ceiling_height(height)
                if height_ft:
                    self._check_range(
                        height_ft,
                        'ceiling_height',
                        f"Ceiling height: {height}"
                    )
    
    def _validate_system_data(self, data: Dict):
        """Validate system details"""
        logger.info("Validating system data...")
        
        systems = data.get('system_details', {})
        
        # Check HVAC
        hvac = systems.get('hvac', {})
        if not hvac.get('equipment', {}).get('type'):
            self._add_warning("No HVAC equipment type extracted")
        
        # Check plumbing
        plumbing = systems.get('plumbing', {})
        water_heater = plumbing.get('water_heater', {})
        if not water_heater.get('type'):
            self._add_warning("No water heater type extracted")
        
        # Check electrical
        electrical = systems.get('electrical', {})
        main_panel = electrical.get('main_panel', {})
        if not main_panel.get('amperage'):
            self._add_warning("No main panel amperage extracted")
    
    def _validate_material_data(self, data: Dict):
        """Validate material specifications"""
        logger.info("Validating material data...")
        
        materials = data.get('material_specifications', {})
        
        # Check insulation
        insulation = materials.get('insulation', {})
        if not insulation.get('type'):
            self._add_warning("No insulation type extracted")
        
        # Check siding
        siding = materials.get('siding', {})
        if not siding.get('type'):
            self._add_warning("No siding type extracted")
        
        # Check flooring
        flooring = materials.get('flooring', {})
        if not flooring.get('types'):
            self._add_warning("No flooring types extracted")
    
    def _validate_consistency(self, data: Dict):
        """Validate data consistency across different sections"""
        logger.info("Validating data consistency...")
        
        total_sqft = data.get('total_sqft', 0)
        
        # Check dimensions vs total sqft
        dimensions = data.get('dimensions', [])
        if dimensions and total_sqft > 0:
            total_dimension_area = sum(dim.get('area_sqft', 0) for dim in dimensions)
            if total_dimension_area > total_sqft * 2:
                self._add_warning(f"Dimension areas ({total_dimension_area}) exceed total sqft by >100%")
        
        # Check wall lengths vs house size
        wall_lengths = data.get('wall_lengths', [])
        if wall_lengths and total_sqft > 0:
            total_wall_length = sum(wall_lengths)
            expected_perimeter = (total_sqft ** 0.5) * 4  # Rough perimeter estimate
            if total_wall_length > expected_perimeter * 3:
                self._add_warning(f"Wall lengths ({total_wall_length:.0f}') seem excessive for {total_sqft} sqft house")
    
    def _check_range(self, value: Any, rule_key: str, description: str):
        """Check if a value falls within expected range"""
        self.validation_results['total_checks'] += 1
        
        if rule_key not in self.validation_rules:
            return
        
        rule = self.validation_rules[rule_key]
        min_val = rule['min']
        max_val = rule['max']
        
        try:
            num_value = float(value)
            if min_val <= num_value <= max_val:
                self.validation_results['passed_checks'] += 1
                logger.debug(f"✅ {description}: {value} (within range {min_val}-{max_val})")
            else:
                self.validation_results['failed_checks'] += 1
                self._add_warning(f"{description}: {value} (expected {min_val}-{max_val})")
        except (ValueError, TypeError):
            self.validation_results['failed_checks'] += 1
            self._add_warning(f"{description}: {value} (invalid number)")
    
    def _check_consistency(self, value1: Any, value2: Any, description: str, tolerance: float = 0.1):
        """Check if two values are consistent within tolerance"""
        self.validation_results['total_checks'] += 1
        
        try:
            num1 = float(value1)
            num2 = float(value2)
            
            if num1 == 0 or num2 == 0:
                return
            
            ratio = abs(num1 - num2) / max(num1, num2)
            if ratio <= tolerance:
                self.validation_results['passed_checks'] += 1
                logger.debug(f"✅ {description}: {value1} vs {value2} (consistent)")
            else:
                self.validation_results['failed_checks'] += 1
                self._add_warning(f"{description}: {value1} vs {value2} (inconsistent, {ratio:.1%} difference)")
        except (ValueError, TypeError):
            self.validation_results['failed_checks'] += 1
            self._add_warning(f"{description}: {value1} vs {value2} (invalid numbers)")
    
    def _add_critical_issue(self, message: str):
        """Add a critical issue"""
        self.validation_results['critical_issues'].append(message)
        logger.error(f"🚨 CRITICAL: {message}")
    
    def _add_warning(self, message: str):
        """Add a warning"""
        self.validation_results['warnings'] += 1
        self.validation_results['warnings_list'].append(message)
        logger.warning(f"⚠️  WARNING: {message}")
    
    def _estimate_expected_rooms(self, total_sqft: int) -> int:
        """Estimate expected number of rooms based on house size"""
        if total_sqft < 1000:
            return 4  # 1 bed, 1 bath, kitchen, living
        elif total_sqft < 2000:
            return 6  # 2 bed, 2 bath, kitchen, living, dining, garage
        elif total_sqft < 3000:
            return 8  # 3 bed, 2.5 bath, kitchen, living, dining, garage, laundry
        else:
            return 10  # 4+ bed, 3+ bath, multiple living areas
    
    def _parse_ceiling_height(self, height_str: str) -> float:
        """Parse ceiling height string to feet"""
        try:
            # Handle formats like "10' - 0\"", "10'", "10'-0\""
            import re
            match = re.search(r'(\d+(?:\.\d+)?)', str(height_str))
            if match:
                return float(match.group(1))
        except (ValueError, AttributeError):
            pass
        return None
    
    def _calculate_score(self):
        """Calculate overall validation score"""
        total_checks = self.validation_results['total_checks']
        passed_checks = self.validation_results['passed_checks']
        critical_issues = len(self.validation_results['critical_issues'])
        
        if total_checks == 0:
            self.validation_results['score'] = 0.0
        else:
            # Base score from passed checks
            base_score = passed_checks / total_checks
            
            # Penalty for critical issues
            critical_penalty = min(critical_issues * 0.2, 0.8)  # Max 80% penalty
            
            # Final score
            self.validation_results['score'] = max(0.0, base_score - critical_penalty)
    
    def validate_batch(self, batch_summary_file: str) -> Dict:
        """Validate all files in a batch summary"""
        logger.info(f"Validating batch: {batch_summary_file}")
        
        try:
            with open(batch_summary_file, 'r') as f:
                batch_data = json.load(f)
        except FileNotFoundError:
            logger.error(f"Batch file not found: {batch_summary_file}")
            return {'error': 'Batch file not found'}
        
        results = []
        for result in batch_data.get('results', []):
            if result.get('success'):
                output_file = result.get('output_file')
                if output_file and os.path.exists(output_file):
                    validation_result = self.validate_single_file(output_file)
                    results.append(validation_result)
        
        return {
            'batch_file': batch_summary_file,
            'timestamp': datetime.now().isoformat(),
            'total_files': len(results),
            'results': results,
            'summary': self._calculate_batch_summary(results)
        }
    
    def _calculate_batch_summary(self, results: List[Dict]) -> Dict:
        """Calculate summary statistics for batch validation"""
        if not results:
            return {}
        
        total_score = sum(r.get('score', 0) for r in results)
        total_critical = sum(len(r.get('critical_issues', [])) for r in results)
        total_warnings = sum(r.get('warnings', 0) for r in results)
        
        return {
            'average_score': total_score / len(results),
            'total_critical_issues': total_critical,
            'total_warnings': total_warnings,
            'files_with_critical_issues': len([r for r in results if r.get('critical_issues')]),
            'best_score': max(r.get('score', 0) for r in results),
            'worst_score': min(r.get('score', 0) for r in results)
        }

def print_validation_report(result: Dict):
    """Print a formatted validation report"""
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    print("\n" + "="*80)
    print("🔍 EXTRACTION VALIDATION REPORT")
    print("="*80)
    
    if 'file' in result:
        print(f"📄 File: {result['file']}")
        print(f"⏰ Timestamp: {result['timestamp']}")
        print(f"📊 Score: {result['score']:.1%}")
        print(f"✅ Passed: {result['passed_checks']}/{result['total_checks']} checks")
        print(f"⚠️  Warnings: {result['warnings']}")
        print(f"🚨 Critical Issues: {len(result['critical_issues'])}")
        
        if result['critical_issues']:
            print("\n🚨 CRITICAL ISSUES:")
            for issue in result['critical_issues']:
                print(f"   • {issue}")
        
        if result['warnings_list']:
            print("\n⚠️  WARNINGS:")
            for warning in result['warnings_list'][:10]:  # Show first 10
                print(f"   • {warning}")
            if len(result['warnings_list']) > 10:
                print(f"   ... and {len(result['warnings_list']) - 10} more warnings")
    
    elif 'batch_file' in result:
        print(f"📁 Batch: {result['batch_file']}")
        print(f"⏰ Timestamp: {result['timestamp']}")
        print(f"📊 Files Validated: {result['total_files']}")
        
        summary = result['summary']
        print(f"📈 Average Score: {summary['average_score']:.1%}")
        print(f"🏆 Best Score: {summary['best_score']:.1%}")
        print(f"📉 Worst Score: {summary['worst_score']:.1%}")
        print(f"🚨 Total Critical Issues: {summary['total_critical_issues']}")
        print(f"⚠️  Total Warnings: {summary['total_warnings']}")
        print(f"📋 Files with Critical Issues: {summary['files_with_critical_issues']}/{result['total_files']}")
    
    print("="*80)

def main():
    parser = argparse.ArgumentParser(description='Validate construction plan extraction results')
    parser.add_argument('input', nargs='?', help='JSON file to validate')
    parser.add_argument('--all', action='store_true', help='Validate all JSON files in output directory')
    parser.add_argument('--batch', help='Validate batch summary file')
    parser.add_argument('--output', help='Save validation report to file')
    
    args = parser.parse_args()
    
    validator = ExtractionValidator()
    
    if args.batch:
        result = validator.validate_batch(args.batch)
    elif args.all:
        # Find all JSON files in output directory
        output_dir = 'output'
        if not os.path.exists(output_dir):
            print(f"❌ Output directory not found: {output_dir}")
            return
        
        json_files = [f for f in os.listdir(output_dir) if f.endswith('.json') and not f.startswith('batch_summary')]
        if not json_files:
            print(f"❌ No JSON files found in {output_dir}")
            return
        
        results = []
        for json_file in json_files:
            file_path = os.path.join(output_dir, json_file)
            result = validator.validate_single_file(file_path)
            results.append(result)
        
        # Calculate batch summary
        result = {
            'batch_type': 'all_files',
            'timestamp': datetime.now().isoformat(),
            'total_files': len(results),
            'results': results,
            'summary': validator._calculate_batch_summary(results)
        }
    elif args.input:
        result = validator.validate_single_file(args.input)
    else:
        parser.print_help()
        return
    
    # Print report
    print_validation_report(result)
    
    # Save report if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\n💾 Report saved to: {args.output}")

if __name__ == '__main__':
    main()
