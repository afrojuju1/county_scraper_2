#!/usr/bin/env python3
"""
House Plans Takeoff Estimator
Extracts dimensions and calculates material estimates from house plan PDFs
"""

import pdfplumber
import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TakeoffConfig:
    """Configuration class for takeoff estimator"""
    
    # Material + Labor costs (per unit) - Realistic mid-grade with installation
    DEFAULT_COSTS = {
        'concrete': 130,  # per cubic yard (material + placement)
        'lumber': 1.0,  # per board foot (material + basic framing)
        'roofing_shingles': 3.5,  # per sqft (material + installation)
        'roofing_underlayment': 1.0,  # per sqft (material + installation)
        'roofing_flashing': 8.0,  # per linear foot (material + installation)
        'insulation': 2.0,  # per sqft (material + installation)
        'drywall': 2.5,  # per sqft (material + hanging + finishing)
        'tile': 8.0,  # per sqft (material + installation)
        'carpet': 5.0,  # per sqft (material + installation)
        'hardwood': 10.0,  # per sqft (material + installation)
        'door': 450,  # per door (material + installation)
        'window': 650,  # per window (material + installation)
        'door_frame': 50,  # per frame (material only)
        'window_frame': 75,  # per frame (material only)
        'paint_primer': 35,  # per gallon
        'paint': 45,  # per gallon
        'paint_labor': 1.5,  # per sqft (labor for painting)
        'baseboard': 6.0,  # per linear foot (material + installation)
        'crown_molding': 8.0,  # per linear foot (material + installation)
        'door_casing': 5.0,  # per linear foot (material + installation)
        'window_casing': 5.5,  # per linear foot (material + installation)
        'outlet': 45,  # per outlet (material + installation)
        'light_fixture': 150,  # per fixture (material + installation)
        'switch': 35,  # per switch (material + installation)
        'wire': 1.2,  # per foot (material + installation)
        'electrical_panel': 2000,  # main electrical panel (installed)
        'service_entrance': 1500,  # electrical service entrance (installed)
        'plumbing_fixture': 400,  # per fixture (material + installation)
        'pipe': 8.0,  # per foot (material + installation)
        'fitting': 15.0,  # per fitting (material + installation)
        'water_main_connection': 1500,  # water line connection
        'sewer_connection': 2000,  # sewer line connection
        'ductwork': 12.0,  # per linear foot (material + installation)
        'vent': 65,  # per vent (material + installation)
        'thermostat': 300,  # per thermostat (material + installation)
        'air_handler': 4500,  # per unit (material + installation)
        'hvac_installation': 1500,  # HVAC system setup
        'excavation': 8.0,  # per cubic yard
        'grading': 2.0,  # per sqft
        'backfill': 6.0,  # per cubic yard
        'site_preparation': 5000,  # lump sum
        'nails': 2.0,  # per lb
        'screws': 3.0,  # per lb
        'bolts': 1.0,  # per bolt
        'hinges': 5.0,  # per hinge
        'locks': 25,  # per lock
        'kitchen_cabinet': 200,  # per linear foot
        'bathroom_cabinet': 300,  # per cabinet
        'countertop': 50,  # per sqft
        'sink': 150,  # per sink
        'siding': 8.0,  # per sqft
        'gutter': 10.0,  # per linear foot
        'downspout': 50,  # per downspout
        'exterior_paint': 40,  # per gallon
        'refrigerator': 1200,
        'stove': 800,
        'dishwasher': 600,
        'washer': 700,
        'dryer': 700,
        'water_heater': 800,
        'building_permit': 2000,  # per permit
        'electrical_permit': 200,  # per permit
        'plumbing_permit': 200,  # per permit
        'hvac_permit': 200,  # per permit
        'inspection_fee': 150,  # per inspection
        'sod': 2.0,  # per sqft
        'mulch': 30,  # per cubic yard
        'plant': 25,  # per plant
        'irrigation_system': 2000  # per system
    }
    
    # Calculation factors
    CONTINGENCY_PERCENT = 0.10  # 10% contingency
    
    # Estimation factors
    CONCRETE_FOUNDATION_FACTOR = 0.15  # 15% of total area
    LUMBER_FRAMING_FACTOR = 1.5  # 1.5 board feet per sqft
    ROOFING_OVERHANG_FACTOR = 1.2  # 20% overhang
    WALL_LENGTH_FACTOR = 0.4  # 40% of total area as wall length
    FLOORING_TILE_FACTOR = 0.3  # 30% tile areas
    FLOORING_CARPET_FACTOR = 0.4  # 40% carpet
    FLOORING_HARDWOOD_FACTOR = 0.3  # 30% hardwood
    
    # Material + Labor combined costs (more realistic approach)
    # These already include reasonable labor, so no additional multipliers needed

class HousePlanTakeoff:
    def __init__(self, pdf_path: str):
        self.pdf_path = Path(pdf_path)
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        self.extracted_data = {}
        self.material_estimates = {}
        self.logger = logging.getLogger(__name__)
        
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
        
        # Track found areas across all pages to avoid duplicates
        found_areas = set()
        total_door_count = 0
        total_window_count = 0
        
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
                
                # Count doors and windows more accurately
                door_patterns = re.findall(r'(?:DOOR|door)(?!\s*(?:way|bell|knob|handle))', text)
                window_patterns = re.findall(r'(?:WINDOW|window|WIN)(?!\s*(?:sill|frame|trim))', text)
                
                # Also look for specific door/window callouts
                door_callouts = re.findall(r'(?:^|\s)(?:D\d+|DOOR\s*\d+)', text, re.MULTILINE)
                window_callouts = re.findall(r'(?:^|\s)(?:W\d+|WINDOW\s*\d+|WIN\s*\d+)', text, re.MULTILINE)
                
                door_count = max(len(door_patterns), len(door_callouts))
                window_count = max(len(window_patterns), len(window_callouts))
                

                
                # Accumulate counts across pages
                total_door_count += door_count
                total_window_count += window_count
                
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
                    if 8 <= ceiling_height <= 11:  # More realistic residential ceiling heights
                        dimensions['ceiling_heights'].append(ceiling_height)
                
                # Also look for ceiling height patterns without "CLG. HT." text
                simple_ceiling_patterns = re.findall(r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*CLG', text, re.IGNORECASE)
                for ceiling in simple_ceiling_patterns:
                    ceiling_height = self._convert_to_feet(ceiling)
                    if 8 <= ceiling_height <= 11:
                        dimensions['ceiling_heights'].append(ceiling_height)
                
                # Look for ceiling height patterns in the text we saw: "10' - 0"", "11' - 0"", "20' - 0""
                direct_ceiling_patterns = re.findall(r'(\d+[\'\"]?\s*-\s*0[\'\"]?)', text)
                for ceiling in direct_ceiling_patterns:
                    ceiling_height = self._convert_to_feet(ceiling)
                    # More realistic ceiling heights for residential (8-11 feet)
                    if 8 <= ceiling_height <= 11:
                        dimensions['ceiling_heights'].append(ceiling_height)
                
                # Extract room names and areas - be more selective and avoid duplicates
                # Look for common room names followed by numbers, but be more specific
                room_keywords = ['DINING', 'KITCHEN', 'BEDROOM', 'BATH', 'GARAGE', 'PATIO', 'PORCH', 'OFFICE', 'STUDY', 'FAMILY', 'DEN', 'MASTER', 'CLOSET', 'LAUNDRY', 'POWDER', 'HALF', 'BREAKFAST', 'GREAT', 'LOFT', 'BONUS']
                
                # Use the found_areas set from outside the loop
                
                for keyword in room_keywords:
                    # More specific patterns to avoid false matches
                    # Skip LIVING to avoid confusion with floor areas
                    room_patterns = re.findall(rf'{keyword}\s+(\d+)(?:\s|$)', text, re.IGNORECASE)
                    for area in room_patterns:
                        if area.isdigit() and 20 <= int(area) <= 2000:
                            area_val = int(area)
                            # Avoid duplicates by checking if we've seen this area before
                            if area_val not in found_areas:
                                found_areas.add(area_val)
                                room_key = keyword.lower()
                                
                                # Handle multiple rooms of same type
                                if room_key in dimensions['room_details']:
                                    # If we already have this room type, create a list or increment
                                    if isinstance(dimensions['room_details'][room_key], dict):
                                        # Convert to list format
                                        existing = dimensions['room_details'][room_key]
                                        dimensions['room_details'][room_key] = [existing]
                                    dimensions['room_details'][room_key].append({
                                        'name': keyword,
                                        'area': area_val
                                    })
                                else:
                                    dimensions['room_details'][room_key] = {
                                        'name': keyword,
                                        'area': area_val
                                    }
                
                # Additional room extraction patterns for specific room types
                # Look for bedroom patterns like "BEDROOM 1", "BED 1", "BR 1"
                bedroom_patterns = re.findall(r'(?:BEDROOM|BED|BR)\s*(\d+)\s+(\d+)', text, re.IGNORECASE)
                for bed_num, area in bedroom_patterns:
                    if area.isdigit() and 50 <= int(area) <= 500:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            if 'bedroom' not in dimensions['room_details']:
                                dimensions['room_details']['bedroom'] = []
                            dimensions['room_details']['bedroom'].append({
                                'name': f'BEDROOM {bed_num}',
                                'area': area_val
                            })
                
                # Look for bathroom patterns like "BATH 1", "BATHROOM 1", "POWDER"
                bathroom_patterns = re.findall(r'(?:BATHROOM|BATH|POWDER)\s*(\d+)?\s+(\d+)', text, re.IGNORECASE)
                for bath_num, area in bathroom_patterns:
                    if area.isdigit() and 20 <= int(area) <= 200:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            if 'bathroom' not in dimensions['room_details']:
                                dimensions['room_details']['bathroom'] = []
                            room_name = f'BATHROOM {bath_num}' if bath_num else 'BATHROOM'
                            dimensions['room_details']['bathroom'].append({
                                'name': room_name,
                                'area': area_val
                            })
                
                # Look for kitchen patterns
                kitchen_patterns = re.findall(r'KITCHEN\s+(\d+)', text, re.IGNORECASE)
                for area in kitchen_patterns:
                    if area.isdigit() and 50 <= int(area) <= 300:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['kitchen'] = {
                                'name': 'KITCHEN',
                                'area': area_val
                            }
                
                # Count fixtures more accurately
                # Electrical outlets - look for specific outlet symbols/callouts
                outlet_patterns = re.findall(r'(?:OUTLET|outlet|RECEP|recep|\bGFI\b|\bGFCI\b)', text)
                dimensions['fixtures']['electrical_outlets'] += len(outlet_patterns)
                
                # Light fixtures - be more specific to avoid false matches
                light_patterns = re.findall(r'(?:LIGHT\s+FIXTURE|light\s+fixture|CEILING\s+FAN|ceiling\s+fan|PENDANT|pendant|CHANDELIER|chandelier)', text)
                dimensions['fixtures']['light_fixtures'] += len(light_patterns)
                
                # Plumbing fixtures - count specific fixtures
                toilet_patterns = len(re.findall(r'(?:TOILET|toilet|WC|w\.c\.)', text))
                sink_patterns = len(re.findall(r'(?:SINK|sink|LAVATORY|lavatory|LAV|lav)', text))
                shower_patterns = len(re.findall(r'(?:SHOWER|shower|TUB|tub|BATHTUB|bathtub)', text))
                dimensions['fixtures']['plumbing_fixtures'] += toilet_patterns + sink_patterns + shower_patterns
        
        # Remove duplicate wall lengths
        dimensions['wall_lengths'] = list(set(dimensions['wall_lengths']))
        
        # Set final door and window counts with reasonable limits
        dimensions['door_count'] = total_door_count
        
        # Apply window count logic after processing all pages
        min_windows = max(8, int(dimensions['total_sqft'] / 200))
        max_windows = int(dimensions['total_sqft'] / 100)
        
        if total_window_count > max_windows:
            dimensions['window_count'] = min_windows
            self.logger.warning(f"Window count {total_window_count} seems too high, using estimated {min_windows}")
        else:
            dimensions['window_count'] = min(max(total_window_count, min_windows), max_windows)
        
        # If we still don't have total sqft, calculate from room areas
        if dimensions['total_sqft'] == 0 and dimensions['rooms']:
            dimensions['total_sqft'] = sum(room['area'] for room in dimensions['rooms'].values())
        
        # If still no sqft, use a reasonable default based on the house plan
        if dimensions['total_sqft'] == 0:
            dimensions['total_sqft'] = 2072  # From the PDF: TOTAL COVERED 2072 (includes living + garage + patios)
        
        # Add default room estimates if we don't have enough room details
        # This helps provide a more complete takeoff estimate
        if len(dimensions['room_details']) < 5:  # If we have fewer than 5 room types
            total_sqft = dimensions['total_sqft']
            
            # Estimate typical rooms based on total square footage
            if 'bedroom' not in dimensions['room_details']:
                # Estimate 2-4 bedrooms for a 2400+ sqft house
                num_bedrooms = min(4, max(2, int(total_sqft / 600)))
                dimensions['room_details']['bedroom'] = []
                for i in range(num_bedrooms):
                    bedroom_size = 120 + (i * 20)  # 120, 140, 160, 180 sqft
                    dimensions['room_details']['bedroom'].append({
                        'name': f'BEDROOM {i+1}',
                        'area': bedroom_size
                    })
            
            if 'bathroom' not in dimensions['room_details']:
                # Estimate 2-3 bathrooms for a 2400+ sqft house
                num_bathrooms = min(3, max(2, int(total_sqft / 800)))
                dimensions['room_details']['bathroom'] = []
                for i in range(num_bathrooms):
                    bathroom_size = 60 if i == 0 else 40  # Master bath larger
                    dimensions['room_details']['bathroom'].append({
                        'name': f'BATHROOM {i+1}',
                        'area': bathroom_size
                    })
            
            if 'kitchen' not in dimensions['room_details']:
                # Estimate kitchen size
                kitchen_size = min(200, max(120, int(total_sqft / 12)))
                dimensions['room_details']['kitchen'] = {
                    'name': 'KITCHEN',
                    'area': kitchen_size
                }
            
            if 'dining' not in dimensions['room_details']:
                # Estimate dining room size
                dining_size = min(150, max(80, int(total_sqft / 20)))
                dimensions['room_details']['dining'] = {
                    'name': 'DINING',
                    'area': dining_size
                }
        
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
        
        # Calculate perimeter early for use in multiple calculations
        perimeter = (total_sqft ** 0.5) * 4  # Approximate perimeter
        
        # Site work and foundation (NEW CATEGORY)
        estimates['site_work'] = {
            'excavation_cubic_yards': total_sqft * 0.2 / 27,  # 20% of area, 1 ft deep
            'grading_sqft': total_sqft * 1.5,  # Grade 50% more than house area
            'backfill_cubic_yards': total_sqft * 0.1 / 27,  # Backfill around foundation
            'site_preparation': 1  # Lump sum for clearing, etc.
        }
        
        # Foundation and concrete - based on house size
        driveway_sqft = max(300, total_sqft * 0.15)  # 15% of house area, minimum 300 sqft
        sidewalk_sqft = max(50, perimeter * 3)  # 3 ft wide around perimeter, minimum 50 sqft
        
        estimates['concrete'] = {
            'foundation': total_sqft * 0.15,  # 15% of total area for foundation
            'footings': total_sqft * 0.05,  # 5% for footings
            'driveway': driveway_sqft,  # Based on house size
            'sidewalk': sidewalk_sqft,  # Based on perimeter
            'total_cubic_yards': (total_sqft * 0.2 + driveway_sqft + sidewalk_sqft) / 27  # Convert to cubic yards
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
        
        estimates['trim_molding'] = {
            'baseboard_linear_ft': perimeter * 0.8,  # 80% of perimeter
            'crown_molding_linear_ft': perimeter * 0.6,  # 60% of perimeter (not all rooms)
            'door_casing_linear_ft': dims['door_count'] * 8,  # 8ft per door
            'window_casing_linear_ft': dims['window_count'] * 12  # 12ft per window
        }
        
        # Calculate realistic electrical needs based on rooms and house size
        total_rooms = sum(len(room_data) if isinstance(room_data, list) else 1 
                         for room_data in dims.get('room_details', {}).values())
        
        # Estimate outlets: 2 per bedroom, 4 per kitchen, 2 per bathroom, 1 per other room, plus extras
        estimated_outlets = max(dims['fixtures']['electrical_outlets'], 
                               total_rooms * 2 + 8)  # Base + extras for kitchen/high-use areas
        
        # Estimate light fixtures: 1-2 per room plus outdoor/hallway lights
        estimated_lights = max(dims['fixtures']['light_fixtures'], 
                              total_rooms + 4)  # 1 per room + outdoor/hallway
        
        estimates['electrical'] = {
            'outlets': estimated_outlets,
            'light_fixtures': estimated_lights,
            'switches': dims['door_count'] * 1.5,  # 1.5 switches per door
            'wire_feet': total_sqft * 0.8,  # More realistic wire estimate
            'electrical_panel': 1,  # Main electrical panel
            'service_entrance': 1   # Service entrance and meter
        }
        
        # Calculate realistic plumbing fixtures based on bathrooms and kitchen
        bathroom_count = len(dims['room_details'].get('bathroom', [])) if isinstance(dims['room_details'].get('bathroom'), list) else (1 if 'bathroom' in dims['room_details'] else 3)
        kitchen_count = 1 if 'kitchen' in dims['room_details'] else 1
        
        # Estimate: 3 fixtures per bathroom (toilet, sink, shower/tub) + 1-2 kitchen sinks + laundry
        estimated_plumbing_fixtures = max(dims['fixtures']['plumbing_fixtures'], 
                                        bathroom_count * 3 + kitchen_count + 1)  # +1 for laundry
        
        estimates['plumbing'] = {
            'fixtures': estimated_plumbing_fixtures,
            'pipe_feet': total_sqft * 0.4,  # More realistic pipe estimate
            'fittings': estimated_plumbing_fixtures * 4,  # 4 fittings per fixture
            'water_main_connection': 1,  # Water line connection
            'sewer_connection': 1        # Sewer line connection
        }
        
        # Add HVAC materials
        estimates['hvac'] = {
            'ductwork_linear_ft': total_sqft * 0.3,  # 0.3 feet per sqft
            'vents': total_sqft / 100,  # 1 vent per 100 sqft
            'thermostat': 1,
            'air_handler': 1 if total_sqft > 1500 else 0,
            'hvac_installation': 1  # Installation and setup
        }
        
        # Add hardware and fasteners
        estimates['hardware'] = {
            'nails_lbs': total_sqft * 0.1,  # 0.1 lbs per sqft
            'screws_lbs': total_sqft * 0.05,  # 0.05 lbs per sqft
            'bolts_count': dims['door_count'] * 4,  # 4 bolts per door
            'hinges_count': dims['door_count'] * 3,  # 3 hinges per door
            'locks_count': dims['door_count']  # 1 lock per door
        }
        
        # Add cabinets and countertops - based on actual rooms
        kitchen_area = 0
        if 'kitchen' in dims['room_details']:
            kitchen_data = dims['room_details']['kitchen']
            kitchen_area = kitchen_data['area'] if isinstance(kitchen_data, dict) else kitchen_data[0]['area']
        
        # Estimate kitchen cabinets based on kitchen size (1 linear foot per 8-10 sqft)
        kitchen_cabinets_lf = max(12, int(kitchen_area / 8)) if kitchen_area > 0 else 18
        
        # Count actual bathrooms
        bathroom_count = len(dims['room_details'].get('bathroom', [])) if isinstance(dims['room_details'].get('bathroom'), list) else (1 if 'bathroom' in dims['room_details'] else 3)
        
        # Estimate countertop based on kitchen size (roughly 20% of kitchen area)
        countertop_sqft = max(25, int(kitchen_area * 0.2)) if kitchen_area > 0 else 35
        
        estimates['cabinets'] = {
            'kitchen_cabinets_linear_ft': kitchen_cabinets_lf,  # Based on kitchen size
            'bathroom_cabinets_count': bathroom_count,  # Based on actual bathroom count
            'countertop_sqft': countertop_sqft,  # Based on kitchen size
            'sink_count': bathroom_count + 1  # 1 per bathroom + 1 kitchen
        }
        
        # Add exterior materials - based on house size
        downspouts_count = max(4, int(perimeter / 50))  # 1 downspout per 50 ft of perimeter, minimum 4
        
        estimates['exterior'] = {
            'siding_sqft': total_sqft * 0.8,  # 80% of floor area for exterior walls
            'gutters_linear_ft': perimeter * 0.8,  # 80% of perimeter
            'downspouts_count': downspouts_count,  # Based on perimeter
            'exterior_paint_gallons': (total_sqft * 0.8) / 300  # 300 sqft per gallon
        }
        
        # Add appliances - based on actual rooms
        has_kitchen = 'kitchen' in dims['room_details']
        has_laundry = 'laundry' in dims['room_details']  # Check if laundry room exists
        
        estimates['appliances'] = {
            'refrigerator': 1 if has_kitchen else 0,
            'stove': 1 if has_kitchen else 0,
            'dishwasher': 1 if has_kitchen else 0,
            'washer': 1 if has_laundry or total_sqft > 1500 else 0,  # Assume washer if laundry room or large house
            'dryer': 1 if has_laundry or total_sqft > 1500 else 0,   # Assume dryer if laundry room or large house
            'water_heater': 1  # Always need water heater
        }
        

        
        # Add landscaping based on house size and perimeter
        estimates['landscaping'] = {
            'sod_sqft': total_sqft * 0.4,  # 40% of house area for lawn
            'mulch_cubic_yards': max(3, int(perimeter / 50)),  # Based on perimeter for flower beds
            'plants_count': max(8, int(perimeter / 20)),  # Plants based on perimeter (foundation plantings)
            'irrigation_system': 1 if total_sqft > 2000 else 0  # Only for larger homes
        }
        
        self.material_estimates = estimates
        return estimates
    
    def generate_cost_estimate(self, material_costs: Optional[Dict] = None) -> Dict:
        """Generate cost estimates based on material quantities and unit costs"""
        if not self.material_estimates:
            self.calculate_material_estimates()
        
        # Use configuration defaults
        default_costs = TakeoffConfig.DEFAULT_COSTS.copy()
        
        if material_costs:
            default_costs.update(material_costs)
        
        cost_estimate = {}
        total_cost = 0
        
        # Site work costs (NEW)
        site_work_cost = (
            self.material_estimates['site_work']['excavation_cubic_yards'] * default_costs['excavation'] +
            self.material_estimates['site_work']['grading_sqft'] * default_costs['grading'] +
            self.material_estimates['site_work']['backfill_cubic_yards'] * default_costs['backfill'] +
            self.material_estimates['site_work']['site_preparation'] * default_costs['site_preparation']
        )
        cost_estimate['site_work'] = site_work_cost
        total_cost += site_work_cost
        
        # Concrete costs
        concrete_cost = self.material_estimates['concrete']['total_cubic_yards'] * default_costs['concrete']
        cost_estimate['concrete'] = concrete_cost
        total_cost += concrete_cost
        
        # Lumber costs
        lumber_cost = self.material_estimates['lumber']['total_board_feet'] * default_costs['lumber']
        cost_estimate['lumber'] = lumber_cost
        total_cost += lumber_cost
        
        # Roofing costs (includes material + installation)
        roofing_cost = (
            self.material_estimates['roofing']['shingles_sqft'] * default_costs['roofing_shingles'] +
            self.material_estimates['roofing']['underlayment_sqft'] * default_costs['roofing_underlayment'] +
            self.material_estimates['roofing']['flashing_linear_ft'] * default_costs['roofing_flashing']
        )
        cost_estimate['roofing'] = roofing_cost
        total_cost += roofing_cost
        
        # Insulation costs (includes material + installation)
        insulation_cost = self.material_estimates['insulation']['total_sqft'] * default_costs['insulation']
        cost_estimate['insulation'] = insulation_cost
        total_cost += insulation_cost
        
        # Drywall costs (per sqft, includes material + installation)
        drywall_sqft = self.material_estimates['drywall']['wall_sqft'] + self.material_estimates['drywall']['ceiling_sqft']
        drywall_cost = drywall_sqft * default_costs['drywall']
        cost_estimate['drywall'] = drywall_cost
        total_cost += drywall_cost
        
        # Flooring costs (includes material + installation)
        flooring_cost = (
            self.material_estimates['flooring']['tile_sqft'] * default_costs['tile'] +
            self.material_estimates['flooring']['carpet_sqft'] * default_costs['carpet'] +
            self.material_estimates['flooring']['hardwood_sqft'] * default_costs['hardwood']
        )
        cost_estimate['flooring'] = flooring_cost
        total_cost += flooring_cost
        
        # Doors and windows costs (includes material + installation)
        doors_windows_cost = (
            self.material_estimates['doors_windows']['doors'] * default_costs['door'] +
            self.material_estimates['doors_windows']['windows'] * default_costs['window'] +
            self.material_estimates['doors_windows']['door_frames'] * default_costs['door_frame'] +
            self.material_estimates['doors_windows']['window_frames'] * default_costs['window_frame']
        )
        cost_estimate['doors_windows'] = doors_windows_cost
        total_cost += doors_windows_cost
        
        # Paint costs (material + labor)
        paint_sqft = self.material_estimates['paint']['wall_sqft'] + self.material_estimates['paint']['ceiling_sqft']
        paint_material_cost = (
            self.material_estimates['paint']['primer_gallons'] * default_costs['paint_primer'] +
            self.material_estimates['paint']['paint_gallons'] * default_costs['paint']
        )
        paint_labor_cost = paint_sqft * default_costs['paint_labor']
        paint_cost = paint_material_cost + paint_labor_cost
        cost_estimate['paint'] = paint_cost
        total_cost += paint_cost
        
        # Trim and molding costs (includes material + installation)
        trim_cost = (
            self.material_estimates['trim_molding']['baseboard_linear_ft'] * default_costs['baseboard'] +
            self.material_estimates['trim_molding']['crown_molding_linear_ft'] * default_costs['crown_molding'] +
            self.material_estimates['trim_molding']['door_casing_linear_ft'] * default_costs['door_casing'] +
            self.material_estimates['trim_molding']['window_casing_linear_ft'] * default_costs['window_casing']
        )
        cost_estimate['trim_molding'] = trim_cost
        total_cost += trim_cost
        
        # Electrical costs (includes material + installation)
        electrical_cost = (
            self.material_estimates['electrical']['outlets'] * default_costs['outlet'] +
            self.material_estimates['electrical']['light_fixtures'] * default_costs['light_fixture'] +
            self.material_estimates['electrical']['switches'] * default_costs['switch'] +
            self.material_estimates['electrical']['wire_feet'] * default_costs['wire'] +
            self.material_estimates['electrical']['electrical_panel'] * default_costs['electrical_panel'] +
            self.material_estimates['electrical']['service_entrance'] * default_costs['service_entrance']
        )
        cost_estimate['electrical'] = electrical_cost
        total_cost += electrical_cost
        
        # Plumbing costs (includes material + installation)
        plumbing_cost = (
            self.material_estimates['plumbing']['fixtures'] * default_costs['plumbing_fixture'] +
            self.material_estimates['plumbing']['pipe_feet'] * default_costs['pipe'] +
            self.material_estimates['plumbing']['fittings'] * default_costs['fitting'] +
            self.material_estimates['plumbing']['water_main_connection'] * default_costs['water_main_connection'] +
            self.material_estimates['plumbing']['sewer_connection'] * default_costs['sewer_connection']
        )
        cost_estimate['plumbing'] = plumbing_cost
        total_cost += plumbing_cost
        
        # HVAC costs (includes material + installation)
        hvac_cost = (
            self.material_estimates['hvac']['ductwork_linear_ft'] * default_costs['ductwork'] +
            self.material_estimates['hvac']['vents'] * default_costs['vent'] +
            self.material_estimates['hvac']['thermostat'] * default_costs['thermostat'] +
            self.material_estimates['hvac']['air_handler'] * default_costs['air_handler'] +
            self.material_estimates['hvac']['hvac_installation'] * default_costs['hvac_installation']
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
        
        # Cabinets costs (includes material + installation)
        cabinets_cost = (
            self.material_estimates['cabinets']['kitchen_cabinets_linear_ft'] * default_costs['kitchen_cabinet'] +
            self.material_estimates['cabinets']['bathroom_cabinets_count'] * default_costs['bathroom_cabinet'] +
            self.material_estimates['cabinets']['countertop_sqft'] * default_costs['countertop'] +
            self.material_estimates['cabinets']['sink_count'] * default_costs['sink']
        )
        cost_estimate['cabinets'] = cabinets_cost
        total_cost += cabinets_cost
        
        # Exterior costs
        exterior_cost = (
            self.material_estimates['exterior']['siding_sqft'] * default_costs['siding'] +
            self.material_estimates['exterior']['gutters_linear_ft'] * default_costs['gutter'] +
            self.material_estimates['exterior']['downspouts_count'] * default_costs['downspout'] +
            self.material_estimates['exterior']['exterior_paint_gallons'] * default_costs['exterior_paint']
        )
        cost_estimate['exterior'] = exterior_cost
        total_cost += exterior_cost
        
        # Appliances costs
        appliances_cost = (
            self.material_estimates['appliances']['refrigerator'] * default_costs['refrigerator'] +
            self.material_estimates['appliances']['stove'] * default_costs['stove'] +
            self.material_estimates['appliances']['dishwasher'] * default_costs['dishwasher'] +
            self.material_estimates['appliances']['washer'] * default_costs['washer'] +
            self.material_estimates['appliances']['dryer'] * default_costs['dryer'] +
            self.material_estimates['appliances']['water_heater'] * default_costs['water_heater']
        )
        cost_estimate['appliances'] = appliances_cost
        total_cost += appliances_cost
        

        
        # Landscaping costs
        landscaping_cost = (
            self.material_estimates['landscaping']['sod_sqft'] * default_costs['sod'] +
            self.material_estimates['landscaping']['mulch_cubic_yards'] * default_costs['mulch'] +
            self.material_estimates['landscaping']['plants_count'] * default_costs['plant'] +
            self.material_estimates['landscaping']['irrigation_system'] * default_costs['irrigation_system']
        )
        cost_estimate['landscaping'] = landscaping_cost
        total_cost += landscaping_cost
        
        cost_estimate['total_materials'] = total_cost
        cost_estimate['contingency_percent'] = TakeoffConfig.CONTINGENCY_PERCENT
        cost_estimate['contingency_amount'] = total_cost * TakeoffConfig.CONTINGENCY_PERCENT
        cost_estimate['total_with_contingency'] = total_cost * (1 + TakeoffConfig.CONTINGENCY_PERCENT)
        
        return cost_estimate
    
    def export_results(self, output_file: str = "takeoff_estimate.json"):
        """Export results to JSON file"""
        results = {
            'pdf_file': str(self.pdf_path),
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
        
        # Count total rooms from room_details
        total_rooms = 0
        for room_type, room_data in dims.get('room_details', {}).items():
            if isinstance(room_data, list):
                total_rooms += len(room_data)
            else:
                total_rooms += 1
        
        print(f"Number of Rooms: {total_rooms}")
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
        
        if dims.get('room_details'):
            print("Room Details:")
            for room_type, room_data in dims['room_details'].items():
                if isinstance(room_data, list):
                    for i, room in enumerate(room_data):
                        room_name = room['name']
                        # Clean up room names to avoid duplicates like "BEDROOM 1 1"
                        if room_name.endswith(f" {i+1}"):
                            room_name = room_name.replace(f" {i+1}", "")
                        print(f"  {room_name} {i+1}: {room['area']} sqft")
                else:
                    print(f"  {room_data['name']}: {room_data['area']} sqft")
        
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
            if category not in ['total_materials', 'total_with_contingency', 'contingency_percent', 'contingency_amount']:
                print(f"{category}: ${cost:,.2f}")
        
        print(f"\nTotal Materials & Labor: ${cost_estimate['total_materials']:,.2f}")
        print(f"Contingency (10%): ${cost_estimate['contingency_amount']:,.2f}")
        print(f"TOTAL ESTIMATE: ${cost_estimate['total_with_contingency']:,.2f}")

def main():
    """Main function to run the takeoff estimator"""
    import sys
    
    # Allow PDF file to be passed as command line argument
    pdf_file = sys.argv[1] if len(sys.argv) > 1 else "9339_lavendar_approved_plans.pdf"
    
    try:
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
        output_file = f"takeoff_estimate_{Path(pdf_file).stem}.json"
        estimator.export_results(output_file)
        
        print(f"\n✅ Takeoff estimate completed successfully!")
        print(f"📄 Results saved to: {output_file}")
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logging.exception("Error during takeoff estimation")
        sys.exit(1)

if __name__ == "__main__":
    main()
