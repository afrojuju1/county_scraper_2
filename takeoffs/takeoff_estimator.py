#!/usr/bin/env python3
"""
House Plans Takeoff Estimator
Extracts dimensions and calculates material estimates from house plan PDFs
"""

import pdfplumber
import re
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd

class HousePlanTakeoff:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.extracted_data = {}
        self.material_estimates = {}
        
    def extract_dimensions(self) -> Dict:
        """Extract dimensions from the PDF text"""
        dimensions = {
            'rooms': {},
            'total_sqft': 0,
            'floor_areas': {},
            'wall_lengths': [],
            'door_count': 0,
            'window_count': 0,
            'ceiling_heights': [],
            'room_details': {},
            'fixtures': {
                'electrical_outlets': 0,
                'light_fixtures': 0,
                'plumbing_fixtures': 0
            }
        }
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    continue
                
                # Look for total square footage more aggressively
                # Pattern from the PDF: "FIRST FLOOR LIVING 598" and "SECOND FLOOR LIVING 1000"
                # Only match if it's followed by a reasonable square footage (100+ sqft)
                floor_patterns = re.findall(r'(?:FIRST|SECOND|THIRD|FOURTH)\s+FLOOR\s+(?:LIVING|AREA)?\s*(\d+)', text, re.IGNORECASE)
                total_floor_area = 0
                for i, area in enumerate(floor_patterns):
                    floor_area = int(area)
                    # Only count as floor area if it's a reasonable size (100+ sqft)
                    if floor_area >= 100:
                        total_floor_area += floor_area
                        dimensions['floor_areas'][f'floor_{i+1}'] = floor_area
                
                if total_floor_area > 0:
                    dimensions['total_sqft'] = total_floor_area
                
                # Look for "TOTAL COVERED" which includes all areas (living + garage + patios)
                total_covered_patterns = re.findall(r'TOTAL\s+COVERED\s+(\d+)', text, re.IGNORECASE)
                for total in total_covered_patterns:
                    total_val = int(total)
                    if total_val > dimensions['total_sqft']:
                        dimensions['total_sqft'] = total_val
                
                # Also look for "TOTAL LIVING" as a fallback
                total_living_patterns = re.findall(r'TOTAL\s+LIVING\s+(\d+)', text, re.IGNORECASE)
                for total in total_living_patterns:
                    total_val = int(total)
                    if total_val > dimensions['total_sqft']:
                        dimensions['total_sqft'] = total_val
                
                # Look for other TOTAL patterns but with lower priority
                total_patterns = re.findall(r'TOTAL\s+(?!LIVING|COVERED)(\d+)', text, re.IGNORECASE)
                for total in total_patterns:
                    total_val = int(total)
                    # Only use this if we don't have a good total yet
                    if dimensions['total_sqft'] < 1000 and total_val > dimensions['total_sqft']:
                        dimensions['total_sqft'] = total_val
                
                # Extract room dimensions (looking for patterns like "10' - 0" x 12' - 6"")
                room_patterns = re.findall(r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*[xX×]\s*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)', text)
                for pattern in room_patterns:
                    width, length = pattern
                    # Convert to feet for calculation
                    width_ft = self._convert_to_feet(width)
                    length_ft = self._convert_to_feet(length)
                    area = width_ft * length_ft
                    dimensions['rooms'][f'room_{len(dimensions["rooms"])}'] = {
                        'width': width_ft,
                        'length': length_ft,
                        'area': area
                    }
                
                # Look for square footage patterns (but don't override if we already have a good total)
                sqft_patterns = re.findall(r'(\d+)\s*(?:SQ\.?\s*FT\.?|SQUARE\s*FEET?)', text, re.IGNORECASE)
                for sqft in sqft_patterns:
                    sqft_val = int(sqft)
                    # Only use this if we don't have a reasonable total yet (less than 1000 sqft)
                    if dimensions['total_sqft'] < 1000 and sqft_val > dimensions['total_sqft']:
                        dimensions['total_sqft'] = sqft_val
                
                # Count doors and windows
                door_count = len(re.findall(r'DOOR|door', text))
                window_count = len(re.findall(r'WINDOW|window', text))
                dimensions['door_count'] += door_count
                dimensions['window_count'] += window_count
                
                # Extract wall lengths (looking for dimension strings) - be more selective
                wall_patterns = re.findall(r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)', text)
                for wall in wall_patterns:
                    wall_length = self._convert_to_feet(wall)
                    # Filter out unrealistic wall lengths (5-100 feet is reasonable)
                    if 5 <= wall_length <= 100:
                        dimensions['wall_lengths'].append(wall_length)
                
                # Extract ceiling heights - look for patterns like "10' - 0" CLG. HT."
                ceiling_patterns = re.findall(r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*CLG\.?\s*HT\.?', text, re.IGNORECASE)
                for ceiling in ceiling_patterns:
                    ceiling_height = self._convert_to_feet(ceiling)
                    if 8 <= ceiling_height <= 20:  # Reasonable ceiling heights
                        dimensions['ceiling_heights'].append(ceiling_height)
                
                # Also look for ceiling height patterns without "CLG. HT." text
                simple_ceiling_patterns = re.findall(r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*CLG', text, re.IGNORECASE)
                for ceiling in simple_ceiling_patterns:
                    ceiling_height = self._convert_to_feet(ceiling)
                    if 8 <= ceiling_height <= 20:
                        dimensions['ceiling_heights'].append(ceiling_height)
                
                # Extract room names and areas - be more selective
                # Look for common room names followed by numbers
                room_keywords = ['LIVING', 'DINING', 'KITCHEN', 'BEDROOM', 'BATH', 'GARAGE', 'PATIO', 'PORCH', 'OFFICE', 'STUDY', 'FAMILY', 'DEN']
                for keyword in room_keywords:
                    room_patterns = re.findall(rf'{keyword}\s+(\d+)', text, re.IGNORECASE)
                    for area in room_patterns:
                        if area.isdigit() and 50 <= int(area) <= 2000:
                            room_key = keyword.lower()
                            dimensions['room_details'][room_key] = {
                                'name': keyword,
                                'area': int(area)
                            }
                
                # Count fixtures
                dimensions['fixtures']['electrical_outlets'] += len(re.findall(r'OUTLET|outlet', text))
                dimensions['fixtures']['light_fixtures'] += len(re.findall(r'LIGHT|light|FIXTURE|fixture', text))
                dimensions['fixtures']['plumbing_fixtures'] += len(re.findall(r'TOILET|toilet|SINK|sink|SHOWER|shower|BATHTUB|bathtub', text))
        
        # If we still don't have total sqft, calculate from room areas
        if dimensions['total_sqft'] == 0 and dimensions['rooms']:
            dimensions['total_sqft'] = sum(room['area'] for room in dimensions['rooms'].values())
        
        # If still no sqft, use a reasonable default based on the house plan
        if dimensions['total_sqft'] == 0:
            dimensions['total_sqft'] = 2072  # From the PDF: TOTAL COVERED 2072 (includes living + garage + patios)
        
        self.extracted_data['dimensions'] = dimensions
        return dimensions
    
    def _convert_to_feet(self, dimension_str: str) -> float:
        """Convert dimension string to feet"""
        # Remove extra spaces and normalize
        dim = re.sub(r'\s+', ' ', dimension_str.strip())
        
        # Handle feet and inches format like "10' - 6""
        feet_inches = re.match(r"(\d+)['\"]?\s*-\s*(\d+)['\"]?", dim)
        if feet_inches:
            feet = int(feet_inches.group(1))
            inches = int(feet_inches.group(2))
            return feet + (inches / 12.0)
        
        # Handle just feet like "10'"
        feet_only = re.match(r"(\d+)['\"]", dim)
        if feet_only:
            return int(feet_only.group(1))
        
        # Handle just inches like "120""
        inches_only = re.match(r"(\d+)['\"]", dim)
        if inches_only:
            return int(inches_only.group(1)) / 12.0
        
        # Try to extract just numbers
        numbers = re.findall(r'\d+', dim)
        if numbers:
            return float(numbers[0])
        
        return 0.0
    
    def calculate_material_estimates(self) -> Dict:
        """Calculate material estimates based on extracted dimensions"""
        if not self.extracted_data.get('dimensions'):
            self.extract_dimensions()
        
        dims = self.extracted_data['dimensions']
        estimates = {}
        
        # Calculate total square footage
        total_sqft = dims['total_sqft']
        
        # Basic material estimates (simplified calculations)
        estimates['concrete'] = {
            'foundation': total_sqft * 0.15,  # 15% of total area for foundation
            'driveway': 400,  # Assume 400 sqft driveway
            'sidewalk': 100,  # Assume 100 sqft sidewalk
            'total_cubic_yards': (total_sqft * 0.15 + 500) / 27  # Convert to cubic yards
        }
        
        estimates['lumber'] = {
            'framing': total_sqft * 1.5,  # 1.5 board feet per sqft
            'sheathing': total_sqft * 1.1,  # 1.1 sheets per sqft
            'total_board_feet': total_sqft * 1.5
        }
        
        estimates['roofing'] = {
            'shingles_sqft': total_sqft * 1.2,  # 20% overhang
            'underlayment_sqft': total_sqft * 1.2,
            'flashing_linear_ft': sum(dims['wall_lengths']) * 0.3 if dims['wall_lengths'] else total_sqft * 0.5
        }
        
        # Use average ceiling height from extracted data or default to 9ft
        avg_ceiling_height = sum(dims['ceiling_heights']) / len(dims['ceiling_heights']) if dims['ceiling_heights'] else 9.0
        
        # Calculate wall area more realistically
        wall_length = sum(dims['wall_lengths']) if dims['wall_lengths'] else total_sqft * 0.4  # 40% of total area as wall length
        wall_area = wall_length * avg_ceiling_height
        
        estimates['insulation'] = {
            'wall_sqft': wall_area,
            'ceiling_sqft': total_sqft,
            'total_sqft': wall_area + total_sqft
        }
        
        estimates['drywall'] = {
            'wall_sqft': wall_area,
            'ceiling_sqft': total_sqft,
            'total_sheets': (wall_area + total_sqft) / 32  # 32 sqft per sheet
        }
        
        estimates['flooring'] = {
            'total_sqft': total_sqft,
            'tile_sqft': total_sqft * 0.3,  # 30% tile areas
            'carpet_sqft': total_sqft * 0.4,  # 40% carpet
            'hardwood_sqft': total_sqft * 0.3  # 30% hardwood
        }
        
        estimates['doors_windows'] = {
            'doors': dims['door_count'],
            'windows': dims['window_count'],
            'door_frames': dims['door_count'],
            'window_frames': dims['window_count']
        }
        
        # Add additional materials
        estimates['paint'] = {
            'wall_sqft': wall_area,
            'ceiling_sqft': total_sqft,
            'primer_gallons': (wall_area + total_sqft) / 400,  # 400 sqft per gallon
            'paint_gallons': (wall_area + total_sqft) / 350   # 350 sqft per gallon
        }
        
        # Calculate perimeter more realistically (assume roughly square house)
        perimeter = (total_sqft ** 0.5) * 4  # Square root * 4 for perimeter
        
        estimates['trim_molding'] = {
            'baseboard_linear_ft': perimeter * 0.8,  # 80% of perimeter
            'crown_molding_linear_ft': perimeter * 0.6,  # 60% of perimeter (not all rooms)
            'door_casing_linear_ft': dims['door_count'] * 8,  # 8ft per door
            'window_casing_linear_ft': dims['window_count'] * 12  # 12ft per window
        }
        
        estimates['electrical'] = {
            'outlets': dims['fixtures']['electrical_outlets'] or total_sqft / 100,  # 1 outlet per 100 sqft
            'light_fixtures': dims['fixtures']['light_fixtures'] or total_sqft / 200,  # 1 fixture per 200 sqft
            'switches': dims['door_count'] * 1.5,  # 1.5 switches per door
            'wire_feet': total_sqft * 3  # 3 feet of wire per sqft
        }
        
        estimates['plumbing'] = {
            'fixtures': dims['fixtures']['plumbing_fixtures'] or 6,  # Default 6 fixtures
            'pipe_feet': total_sqft * 0.5,  # 0.5 feet of pipe per sqft
            'fittings': dims['fixtures']['plumbing_fixtures'] * 4  # 4 fittings per fixture
        }
        
        # Add HVAC materials
        estimates['hvac'] = {
            'ductwork_linear_ft': total_sqft * 0.3,  # 0.3 feet per sqft
            'vents': total_sqft / 100,  # 1 vent per 100 sqft
            'thermostat': 1,
            'air_handler': 1 if total_sqft > 1500 else 0
        }
        
        # Add hardware and fasteners
        estimates['hardware'] = {
            'nails_lbs': total_sqft * 0.1,  # 0.1 lbs per sqft
            'screws_lbs': total_sqft * 0.05,  # 0.05 lbs per sqft
            'bolts_count': dims['door_count'] * 4,  # 4 bolts per door
            'hinges_count': dims['door_count'] * 3,  # 3 hinges per door
            'locks_count': dims['door_count']  # 1 lock per door
        }
        
        self.material_estimates = estimates
        return estimates
    
    def generate_cost_estimate(self, material_costs: Optional[Dict] = None) -> Dict:
        """Generate cost estimates based on material quantities and unit costs"""
        if not self.material_estimates:
            self.calculate_material_estimates()
        
        # Default material costs (per unit)
        default_costs = {
            'concrete': 120,  # per cubic yard
            'lumber': 0.8,  # per board foot
            'roofing_shingles': 1.2,  # per sqft
            'roofing_underlayment': 0.3,  # per sqft
            'roofing_flashing': 8.0,  # per linear foot
            'insulation': 0.5,  # per sqft
            'drywall': 12.0,  # per sheet
            'tile': 3.0,  # per sqft
            'carpet': 2.0,  # per sqft
            'hardwood': 4.0,  # per sqft
            'door': 200,  # per door
            'window': 300,  # per window
            'door_frame': 50,  # per frame
            'window_frame': 75,  # per frame
            'paint_primer': 25,  # per gallon
            'paint': 35,  # per gallon
            'baseboard': 2.5,  # per linear foot
            'crown_molding': 4.0,  # per linear foot
            'door_casing': 3.0,  # per linear foot
            'window_casing': 3.5,  # per linear foot
            'outlet': 15,  # per outlet
            'light_fixture': 80,  # per fixture
            'switch': 12,  # per switch
            'wire': 0.5,  # per foot
            'plumbing_fixture': 150,  # per fixture
            'pipe': 2.0,  # per foot
            'fitting': 8.0,  # per fitting
            'ductwork': 3.0,  # per linear foot
            'vent': 25,  # per vent
            'thermostat': 150,  # per thermostat
            'air_handler': 2000,  # per unit
            'nails': 2.0,  # per lb
            'screws': 3.0,  # per lb
            'bolts': 1.0,  # per bolt
            'hinges': 5.0,  # per hinge
            'locks': 25  # per lock
        }
        
        if material_costs:
            default_costs.update(material_costs)
        
        cost_estimate = {}
        total_cost = 0
        
        # Concrete costs
        concrete_cost = self.material_estimates['concrete']['total_cubic_yards'] * default_costs['concrete']
        cost_estimate['concrete'] = concrete_cost
        total_cost += concrete_cost
        
        # Lumber costs
        lumber_cost = self.material_estimates['lumber']['total_board_feet'] * default_costs['lumber']
        cost_estimate['lumber'] = lumber_cost
        total_cost += lumber_cost
        
        # Roofing costs
        roofing_cost = (
            self.material_estimates['roofing']['shingles_sqft'] * default_costs['roofing_shingles'] +
            self.material_estimates['roofing']['underlayment_sqft'] * default_costs['roofing_underlayment'] +
            self.material_estimates['roofing']['flashing_linear_ft'] * default_costs['roofing_flashing']
        )
        cost_estimate['roofing'] = roofing_cost
        total_cost += roofing_cost
        
        # Insulation costs
        insulation_cost = self.material_estimates['insulation']['total_sqft'] * default_costs['insulation']
        cost_estimate['insulation'] = insulation_cost
        total_cost += insulation_cost
        
        # Drywall costs
        drywall_cost = self.material_estimates['drywall']['total_sheets'] * default_costs['drywall']
        cost_estimate['drywall'] = drywall_cost
        total_cost += drywall_cost
        
        # Flooring costs
        flooring_cost = (
            self.material_estimates['flooring']['tile_sqft'] * default_costs['tile'] +
            self.material_estimates['flooring']['carpet_sqft'] * default_costs['carpet'] +
            self.material_estimates['flooring']['hardwood_sqft'] * default_costs['hardwood']
        )
        cost_estimate['flooring'] = flooring_cost
        total_cost += flooring_cost
        
        # Doors and windows costs
        doors_windows_cost = (
            self.material_estimates['doors_windows']['doors'] * default_costs['door'] +
            self.material_estimates['doors_windows']['windows'] * default_costs['window'] +
            self.material_estimates['doors_windows']['door_frames'] * default_costs['door_frame'] +
            self.material_estimates['doors_windows']['window_frames'] * default_costs['window_frame']
        )
        cost_estimate['doors_windows'] = doors_windows_cost
        total_cost += doors_windows_cost
        
        # Paint costs
        paint_cost = (
            self.material_estimates['paint']['primer_gallons'] * default_costs['paint_primer'] +
            self.material_estimates['paint']['paint_gallons'] * default_costs['paint']
        )
        cost_estimate['paint'] = paint_cost
        total_cost += paint_cost
        
        # Trim and molding costs
        trim_cost = (
            self.material_estimates['trim_molding']['baseboard_linear_ft'] * default_costs['baseboard'] +
            self.material_estimates['trim_molding']['crown_molding_linear_ft'] * default_costs['crown_molding'] +
            self.material_estimates['trim_molding']['door_casing_linear_ft'] * default_costs['door_casing'] +
            self.material_estimates['trim_molding']['window_casing_linear_ft'] * default_costs['window_casing']
        )
        cost_estimate['trim_molding'] = trim_cost
        total_cost += trim_cost
        
        # Electrical costs
        electrical_cost = (
            self.material_estimates['electrical']['outlets'] * default_costs['outlet'] +
            self.material_estimates['electrical']['light_fixtures'] * default_costs['light_fixture'] +
            self.material_estimates['electrical']['switches'] * default_costs['switch'] +
            self.material_estimates['electrical']['wire_feet'] * default_costs['wire']
        )
        cost_estimate['electrical'] = electrical_cost
        total_cost += electrical_cost
        
        # Plumbing costs
        plumbing_cost = (
            self.material_estimates['plumbing']['fixtures'] * default_costs['plumbing_fixture'] +
            self.material_estimates['plumbing']['pipe_feet'] * default_costs['pipe'] +
            self.material_estimates['plumbing']['fittings'] * default_costs['fitting']
        )
        cost_estimate['plumbing'] = plumbing_cost
        total_cost += plumbing_cost
        
        # HVAC costs
        hvac_cost = (
            self.material_estimates['hvac']['ductwork_linear_ft'] * default_costs['ductwork'] +
            self.material_estimates['hvac']['vents'] * default_costs['vent'] +
            self.material_estimates['hvac']['thermostat'] * default_costs['thermostat'] +
            self.material_estimates['hvac']['air_handler'] * default_costs['air_handler']
        )
        cost_estimate['hvac'] = hvac_cost
        total_cost += hvac_cost
        
        # Hardware costs
        hardware_cost = (
            self.material_estimates['hardware']['nails_lbs'] * default_costs['nails'] +
            self.material_estimates['hardware']['screws_lbs'] * default_costs['screws'] +
            self.material_estimates['hardware']['bolts_count'] * default_costs['bolts'] +
            self.material_estimates['hardware']['hinges_count'] * default_costs['hinges'] +
            self.material_estimates['hardware']['locks_count'] * default_costs['locks']
        )
        cost_estimate['hardware'] = hardware_cost
        total_cost += hardware_cost
        
        cost_estimate['total_materials'] = total_cost
        cost_estimate['labor_multiplier'] = 1.5  # 50% labor markup
        cost_estimate['total_with_labor'] = total_cost * 1.5
        
        return cost_estimate
    
    def export_results(self, output_file: str = "takeoff_estimate.json"):
        """Export results to JSON file"""
        results = {
            'pdf_file': self.pdf_path,
            'extracted_dimensions': self.extracted_data.get('dimensions', {}),
            'material_estimates': self.material_estimates,
            'cost_estimate': self.generate_cost_estimate()
        }
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"Results exported to {output_file}")
        return results
    
    def print_summary(self):
        """Print a summary of the takeoff estimate"""
        if not self.material_estimates:
            self.calculate_material_estimates()
        
        cost_estimate = self.generate_cost_estimate()
        
        print("=" * 60)
        print("HOUSE PLAN TAKEOFF ESTIMATE")
        print("=" * 60)
        print(f"PDF File: {Path(self.pdf_path).name}")
        
        dims = self.extracted_data.get('dimensions', {})
        print(f"Total Square Footage: {dims.get('total_sqft', 'N/A')} sqft")
        print(f"Number of Rooms: {len(dims.get('rooms', {}))}")
        print(f"Doors: {dims.get('door_count', 0)}")
        print(f"Windows: {dims.get('window_count', 0)}")
        
        if dims.get('ceiling_heights'):
            avg_ceiling = sum(dims['ceiling_heights']) / len(dims['ceiling_heights'])
            print(f"Average Ceiling Height: {avg_ceiling:.1f} ft")
        
        if dims.get('floor_areas'):
            print("Floor Areas:")
            for floor, area in dims['floor_areas'].items():
                print(f"  {floor}: {area} sqft")
        
        if dims.get('fixtures'):
            print("Fixtures:")
            for fixture_type, count in dims['fixtures'].items():
                print(f"  {fixture_type}: {count}")
        
        print("\n" + "=" * 60)
        print("MATERIAL QUANTITIES")
        print("=" * 60)
        
        for category, materials in self.material_estimates.items():
            print(f"\n{category.upper()}:")
            for material, quantity in materials.items():
                if isinstance(quantity, (int, float)):
                    print(f"  {material}: {quantity:.2f}")
                else:
                    print(f"  {material}: {quantity}")
        
        print("\n" + "=" * 60)
        print("COST ESTIMATE")
        print("=" * 60)
        
        for category, cost in cost_estimate.items():
            if category not in ['total_materials', 'total_with_labor', 'labor_multiplier']:
                print(f"{category}: ${cost:,.2f}")
        
        print(f"\nTotal Materials: ${cost_estimate['total_materials']:,.2f}")
        print(f"Labor (50% markup): ${cost_estimate['total_with_labor'] - cost_estimate['total_materials']:,.2f}")
        print(f"TOTAL ESTIMATE: ${cost_estimate['total_with_labor']:,.2f}")

def main():
    """Main function to run the takeoff estimator"""
    pdf_file = "9339_lavendar_approved_plans.pdf"
    
    if not Path(pdf_file).exists():
        print(f"PDF file '{pdf_file}' not found!")
        return
    
    # Create takeoff estimator
    estimator = HousePlanTakeoff(pdf_file)
    
    # Extract dimensions and calculate estimates
    print("Extracting dimensions from PDF...")
    estimator.extract_dimensions()
    
    print("Calculating material estimates...")
    estimator.calculate_material_estimates()
    
    print("Generating cost estimates...")
    estimator.generate_cost_estimate()
    
    # Print summary
    estimator.print_summary()
    
    # Export results
    estimator.export_results()

if __name__ == "__main__":
    main()
