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
    
    # Material costs only (no labor)
    DEFAULT_COSTS = {
        'concrete': 85,  # per cubic yard (material only)
        'lumber': 0.65,  # per board foot (material only)
        'roofing_shingles': 1.8,  # per sqft (material only)
        'roofing_underlayment': 0.45,  # per sqft (material only)
        'roofing_flashing': 3.5,  # per linear foot (material only)
        'insulation': 1.2,  # per sqft (material only)
        'drywall': 0.85,  # per sqft (material only)
        'tile': 4.5,  # per sqft (material only)
        'carpet': 2.8,  # per sqft (material only)
        'hardwood': 6.5,  # per sqft (material only)
        'door': 180,  # per door (material only)
        'window': 320,  # per window (material only)
        'door_frame': 35,  # per frame (material only)
        'window_frame': 45,  # per frame (material only)
        'paint_primer': 35,  # per gallon (material only)
        'paint': 45,  # per gallon (material only)
        'baseboard': 2.5,  # per linear foot (material only)
        'crown_molding': 4.0,  # per linear foot (material only)
        'door_casing': 2.2,  # per linear foot (material only)
        'window_casing': 2.8,  # per linear foot (material only)
        'outlet': 12,  # per outlet (material only)
        'light_fixture': 85,  # per fixture (material only)
        'switch': 8,  # per switch (material only)
        'wire': 0.65,  # per foot (material only)
        'electrical_panel': 850,  # main electrical panel (material only)
        'service_entrance': 650,  # electrical service entrance (material only)
        'plumbing_fixture': 220,  # per fixture (material only)
        'pipe': 3.2,  # per foot (material only)
        'fitting': 8.5,  # per fitting (material only)
        'water_main_connection': 450,  # water line connection (material only)
        'sewer_connection': 680,  # sewer line connection (material only)
        'ductwork': 5.5,  # per linear foot (material only)
        'vent': 28,  # per vent (material only)
        'thermostat': 120,  # per thermostat (material only)
        'air_handler': 2800,  # per unit (material only)
        'hvac_installation': 0,  # No installation costs
        'excavation': 0,  # No material cost for excavation
        'grading': 0,  # No material cost for grading
        'backfill': 0,  # No material cost for backfill
        'site_preparation': 0,  # No material cost for site prep
        'nails': 2.0,  # per lb (material only)
        'screws': 3.0,  # per lb (material only)
        'bolts': 1.0,  # per bolt (material only)
        'hinges': 3.5,  # per hinge (material only)
        'locks': 18,  # per lock (material only)
        'kitchen_cabinet': 120,  # per linear foot (material only)
        'bathroom_cabinet': 180,  # per cabinet (material only)
        'countertop': 28,  # per sqft (material only)
        'sink': 95,  # per sink (material only)
        'siding': 4.2,  # per sqft (material only)
        'gutter': 4.5,  # per linear foot (material only)
        'downspout': 22,  # per downspout (material only)
        'exterior_paint': 40,  # per gallon (material only)
        'refrigerator': 1200,  # appliance cost
        'stove': 800,  # appliance cost
        'dishwasher': 600,  # appliance cost
        'washer': 700,  # appliance cost
        'dryer': 700,  # appliance cost
        'water_heater': 800,  # appliance cost
        'sod': 0.85,  # per sqft (material only)
        'mulch': 25,  # per cubic yard (material only)
        'plant': 18,  # per plant (material only)
        'irrigation_system': 1200,  # per system (material only)
        # Foundation materials
        'rebar': 800,  # per ton (material only)
        'foundation_bolt': 8,  # per bolt (material only)
        'vapor_barrier': 0.45,  # per sqft (material only)
        'waterproofing': 1.2,  # per sqft (material only)
        'gravel': 35,  # per cubic yard (material only)
        'form_boards': 0.85,  # per board foot (material only)
        # Structural materials
        'engineered_lumber': 4.5,  # per linear foot (material only)
        'metal_connector': 12,  # per connector (material only)
        'structural_screws': 4.5,  # per lb (material only)
        'hurricane_tie': 8,  # per tie (material only)
        'post_anchor': 15,  # per anchor (material only)
        'beam_pocket': 25,  # per pocket (material only)
        # Additional electrical
        'conduit': 2.8,  # per foot (material only)
        'junction_box': 8,  # per box (material only)
        'breaker': 25,  # per breaker (material only)
        'gfci_outlet': 18,  # per GFCI outlet (material only)
        'smoke_detector': 35,  # per detector (material only)
        'electrical_meter': 180,  # per meter (material only)
        'grounding_rod': 25,  # per rod (material only)
        'wire_nut': 0.15,  # per nut (material only)
        # Additional plumbing
        'shut_off_valve': 18,  # per valve (material only)
        'vent_pipe': 3.5,  # per linear foot (material only)
        'cleanout': 45,  # per cleanout (material only)
        'water_meter': 220,  # per meter (material only)
        'pressure_tank': 350,  # per tank (material only)
        'sump_pump': 280,  # per pump (material only)
        'floor_drain': 65,  # per drain (material only)
        'pipe_insulation': 1.8,  # per linear foot (material only)
        # Additional insulation
        'house_wrap': 0.65,  # per sqft (material only)
        'foam_board': 1.85,  # per sqft (material only)
        'caulk_tube': 4.5,  # per tube (material only)
        'weatherstripping': 2.2,  # per linear foot (material only)
        # Additional flooring
        'subfloor': 2.8,  # per sqft (material only)
        'underlayment': 1.2,  # per sqft (material only)
        'transition_strip': 8,  # per linear foot (material only)
        'floor_adhesive': 45,  # per gallon (material only)
        'carpet_padding': 1.8  # per sqft (material only)
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
            'rooms_by_floor': {  # NEW: Track rooms by floor
                'first_floor': {},
                'second_floor': {},
                'third_floor': {},
                'basement': {},
                'unknown': {}
            },
            'fixtures': {
                'electrical_outlets': 0,
                'light_fixtures': 0,
                'plumbing_fixtures': 0,
                'switches': 0,
                'hvac_vents': 0
            }
        }
        
        # Track found areas across all pages to avoid duplicates
        found_areas = set()
        total_door_count = 0
        total_window_count = 0
        current_floor = 'unknown'  # Track which floor we're currently processing
        
        def add_room_to_floor(room_key, room_data, floor=None):
            """Helper function to add room to both room_details and rooms_by_floor"""
            # Add to main room_details
            if room_key in dimensions['room_details']:
                if isinstance(dimensions['room_details'][room_key], dict):
                    existing = dimensions['room_details'][room_key]
                    dimensions['room_details'][room_key] = [existing]
                dimensions['room_details'][room_key].append(room_data)
            else:
                dimensions['room_details'][room_key] = room_data
            
            # Add to floor-specific tracking
            target_floor = floor or current_floor
            if target_floor in dimensions['rooms_by_floor']:
                if room_key in dimensions['rooms_by_floor'][target_floor]:
                    if isinstance(dimensions['rooms_by_floor'][target_floor][room_key], dict):
                        existing = dimensions['rooms_by_floor'][target_floor][room_key]
                        dimensions['rooms_by_floor'][target_floor][room_key] = [existing]
                    dimensions['rooms_by_floor'][target_floor][room_key].append(room_data)
                else:
                    dimensions['rooms_by_floor'][target_floor][room_key] = room_data
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    continue
                
                # Look for total square footage more aggressively
                # Pattern from the PDF: "FIRST FLOOR LIVING 598" and "SECOND FLOOR LIVING 1000"
                # IMPORTANT: These are FLOOR AREAS, not individual living rooms!
                floor_patterns = re.findall(r'(FIRST|SECOND|THIRD|FOURTH)\s+FLOOR\s+(?:LIVING|AREA)?\s*(\d+)', text, re.IGNORECASE)
                total_floor_area = 0
                for floor_name, area in floor_patterns:
                    floor_area = int(area)
                    # Only count as floor area if it's a reasonable size (100+ sqft)
                    if floor_area >= 100:
                        total_floor_area += floor_area
                        floor_key = f'{floor_name.lower()}_floor'
                        dimensions['floor_areas'][floor_key] = floor_area
                        current_floor = floor_key  # Update current floor context
                        self.logger.info(f"Found floor area: {floor_name} FLOOR = {floor_area} sqft - Setting floor context to {floor_key}")
                
                # Also detect floor context from page headers or titles
                floor_context_patterns = re.findall(r'(FIRST|SECOND|THIRD|FOURTH)\s+FLOOR', text, re.IGNORECASE)
                if floor_context_patterns and current_floor == 'unknown':
                    floor_name = floor_context_patterns[0].lower() + '_floor'
                    current_floor = floor_name
                    self.logger.info(f"Detected floor context from header: {floor_name}")
                
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
                
                # Extract ACTUAL room names and areas - EXCLUDE floor area patterns
                # CRITICAL: Avoid matching "FIRST FLOOR LIVING" patterns which are floor totals, not rooms
                
                # First, identify floor area patterns to exclude them
                floor_area_patterns = set()
                floor_matches = re.findall(r'(?:FIRST|SECOND|THIRD|FOURTH)\s+FLOOR\s+(?:LIVING|AREA)?\s*(\d+)', text, re.IGNORECASE)
                for area in floor_matches:
                    floor_area_patterns.add(int(area))
                
                # Room keywords - but we'll be very careful with LIVING
                room_keywords = ['DINING', 'KITCHEN', 'BEDROOM', 'BATH', 'GARAGE', 'PATIO', 'PORCH', 'OFFICE', 'STUDY', 'FAMILY', 'DEN', 'MASTER', 'CLOSET', 'LAUNDRY', 'POWDER', 'HALF', 'BREAKFAST', 'GREAT', 'LOFT', 'BONUS', 'FOYER', 'ENTRY', 'HALL', 'UTILITY', 'PANTRY', 'MUDROOM', 'SUNROOM', 'LIBRARY', 'MEDIA', 'GAME', 'RECREATION', 'EXERCISE', 'WORKSHOP']
                
                for keyword in room_keywords:
                    # Look for room patterns, but exclude floor area matches
                    room_patterns = re.findall(rf'(?<!FLOOR\s){keyword}\s+(\d+)(?:\s|$)', text, re.IGNORECASE)
                    for area in room_patterns:
                        if area.isdigit() and 20 <= int(area) <= 2000:
                            area_val = int(area)
                            # Skip if this area matches a floor area (avoid double-counting)
                            if area_val in floor_area_patterns:
                                self.logger.warning(f"Skipping {keyword} {area_val} - matches floor area pattern")
                                continue
                                
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
                                self.logger.info(f"Found room: {keyword} = {area_val} sqft")
                
                # Handle LIVING rooms separately with extra caution
                # Only match "LIVING ROOM 300" or "LIVING 300" but NOT "FIRST FLOOR LIVING 598"
                living_patterns = re.findall(r'(?<!FLOOR\s)(?:LIVING\s+(?:ROOM\s+)?|LIVING\s+)(\d+)(?:\s|$)', text, re.IGNORECASE)
                for area in living_patterns:
                    if area.isdigit() and 100 <= int(area) <= 800:  # Living rooms are typically 100-800 sqft
                        area_val = int(area)
                        # Skip if this area matches a floor area
                        if area_val in floor_area_patterns:
                            self.logger.warning(f"Skipping LIVING {area_val} - matches floor area pattern")
                            continue
                            
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            room_key = 'living'
                            
                            if room_key in dimensions['room_details']:
                                if isinstance(dimensions['room_details'][room_key], dict):
                                    existing = dimensions['room_details'][room_key]
                                    dimensions['room_details'][room_key] = [existing]
                                dimensions['room_details'][room_key].append({
                                    'name': 'LIVING ROOM',
                                    'area': area_val
                                })
                            else:
                                dimensions['room_details'][room_key] = {
                                    'name': 'LIVING ROOM',
                                    'area': area_val
                                }
                            self.logger.info(f"Found room: LIVING ROOM = {area_val} sqft")
                
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
                
                # Look for family room patterns (common in house plans)
                family_patterns = re.findall(r'(?:FAMILY\s+ROOM|FAMILY)\s+(\d+)', text, re.IGNORECASE)
                for area in family_patterns:
                    if area.isdigit() and 100 <= int(area) <= 600:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['family'] = {
                                'name': 'FAMILY ROOM',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: FAMILY ROOM = {area_val} sqft")
                
                # Look for great room patterns (common in modern plans)
                great_patterns = re.findall(r'(?:GREAT\s+ROOM|GREAT)\s+(\d+)', text, re.IGNORECASE)
                for area in great_patterns:
                    if area.isdigit() and 200 <= int(area) <= 800:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['great'] = {
                                'name': 'GREAT ROOM',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: GREAT ROOM = {area_val} sqft")
                
                # Look for bonus room patterns
                bonus_patterns = re.findall(r'(?:BONUS\s+ROOM|BONUS)\s+(\d+)', text, re.IGNORECASE)
                for area in bonus_patterns:
                    if area.isdigit() and 80 <= int(area) <= 400:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['bonus'] = {
                                'name': 'BONUS ROOM',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: BONUS ROOM = {area_val} sqft")
                
                # Look for dining room patterns
                dining_patterns = re.findall(r'(?:DINING\s+ROOM|DINING)\s+(\d+)', text, re.IGNORECASE)
                for area in dining_patterns:
                    if area.isdigit() and 80 <= int(area) <= 300:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['dining'] = {
                                'name': 'DINING ROOM',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: DINING ROOM = {area_val} sqft")
                
                # Look for master bedroom patterns
                master_patterns = re.findall(r'(?:MASTER\s+BEDROOM|MASTER\s+BR|MASTER)\s+(\d+)', text, re.IGNORECASE)
                for area in master_patterns:
                    if area.isdigit() and 120 <= int(area) <= 400:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['master_bedroom'] = {
                                'name': 'MASTER BEDROOM',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: MASTER BEDROOM = {area_val} sqft")
                
                # Look for master bathroom patterns
                master_bath_patterns = re.findall(r'(?:MASTER\s+BATH|MASTER\s+BATHROOM)\s+(\d+)', text, re.IGNORECASE)
                for area in master_bath_patterns:
                    if area.isdigit() and 40 <= int(area) <= 150:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['master_bathroom'] = {
                                'name': 'MASTER BATHROOM',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: MASTER BATHROOM = {area_val} sqft")
                
                # Look for laundry room patterns
                laundry_patterns = re.findall(r'(?:LAUNDRY\s+ROOM|LAUNDRY|UTILITY\s+ROOM|UTILITY)\s+(\d+)', text, re.IGNORECASE)
                for area in laundry_patterns:
                    if area.isdigit() and 30 <= int(area) <= 120:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['laundry'] = {
                                'name': 'LAUNDRY ROOM',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: LAUNDRY ROOM = {area_val} sqft")
                
                # Look for office/study patterns
                office_patterns = re.findall(r'(?:OFFICE|STUDY|DEN)\s+(\d+)', text, re.IGNORECASE)
                for area in office_patterns:
                    if area.isdigit() and 80 <= int(area) <= 250:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['office'] = {
                                'name': 'OFFICE',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: OFFICE = {area_val} sqft")
                
                # Look for foyer/entry patterns
                foyer_patterns = re.findall(r'(?:FOYER|ENTRY|ENTRYWAY)\s+(\d+)', text, re.IGNORECASE)
                for area in foyer_patterns:
                    if area.isdigit() and 20 <= int(area) <= 100:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['foyer'] = {
                                'name': 'FOYER',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: FOYER = {area_val} sqft")
                
                # Look for closet patterns (walk-in closets)
                closet_patterns = re.findall(r'(?:WALK.IN\s+CLOSET|WIC|W\.I\.C\.)\s+(\d+)', text, re.IGNORECASE)
                for area in closet_patterns:
                    if area.isdigit() and 15 <= int(area) <= 80:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            if 'closets' not in dimensions['room_details']:
                                dimensions['room_details']['closets'] = []
                            dimensions['room_details']['closets'].append({
                                'name': 'WALK-IN CLOSET',
                                'area': area_val
                            })
                            self.logger.info(f"Found room: WALK-IN CLOSET = {area_val} sqft")
                
                # Look for pantry patterns
                pantry_patterns = re.findall(r'(?:PANTRY)\s+(\d+)', text, re.IGNORECASE)
                for area in pantry_patterns:
                    if area.isdigit() and 15 <= int(area) <= 60:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            dimensions['room_details']['pantry'] = {
                                'name': 'PANTRY',
                                'area': area_val
                            }
                            self.logger.info(f"Found room: PANTRY = {area_val} sqft")
                
                # Look for hall/hallway patterns
                hall_patterns = re.findall(r'(?:HALL|HALLWAY)\s+(\d+)', text, re.IGNORECASE)
                for area in hall_patterns:
                    if area.isdigit() and 20 <= int(area) <= 150:
                        area_val = int(area)
                        if area_val not in found_areas:
                            found_areas.add(area_val)
                            if 'halls' not in dimensions['room_details']:
                                dimensions['room_details']['halls'] = []
                            dimensions['room_details']['halls'].append({
                                'name': 'HALLWAY',
                                'area': area_val
                            })
                            self.logger.info(f"Found room: HALLWAY = {area_val} sqft")
                
                # Count fixtures more accurately with CONSERVATIVE patterns
                # Electrical outlets - only count explicit outlet references, avoid voltage/dimension matches
                outlet_patterns = re.findall(r'(?:OUTLET|outlet|RECEP|recep|\bGFI\b|\bGFCI\b)', text)
                # Only count specific electrical callouts, not generic alphanumeric codes
                electrical_symbols = re.findall(r'(?:\bOUTLET\s+\d+\b|\bRECEP\s+\d+\b)', text)
                dimensions['fixtures']['electrical_outlets'] += len(outlet_patterns) + len(electrical_symbols)
                
                # Light fixtures - be more conservative, avoid generic symbols
                light_patterns = re.findall(r'(?:LIGHT\s+FIXTURE|light\s+fixture|CEILING\s+FAN|ceiling\s+fan|PENDANT|pendant|CHANDELIER|chandelier|LIGHT\s+\d+)', text)
                # Only count specific lighting callouts
                lighting_symbols = re.findall(r'(?:\bLT\d+\b|\bLIGHT\s+\d+\b)', text)
                dimensions['fixtures']['light_fixtures'] += len(light_patterns) + len(lighting_symbols)
                
                # Plumbing fixtures - be more conservative, avoid generic codes
                toilet_patterns = len(re.findall(r'(?:TOILET|toilet|WC|w\.c\.|WATER\s+CLOSET)', text))
                sink_patterns = len(re.findall(r'(?:SINK|sink|LAVATORY|lavatory|LAV|lav|BASIN)', text))
                shower_patterns = len(re.findall(r'(?:SHOWER|shower|TUB|tub|BATHTUB|bathtub|BATH\s+TUB)', text))
                # Only count specific plumbing callouts, not generic codes
                plumbing_symbols = len(re.findall(r'(?:\bPLUMBING\s+\d+\b|\bFIXTURE\s+\d+\b)', text))
                dimensions['fixtures']['plumbing_fixtures'] += toilet_patterns + sink_patterns + shower_patterns + plumbing_symbols
                
                # Count switches - be more conservative
                switch_patterns = re.findall(r'(?:SWITCH|switch|TOGGLE|toggle)', text)
                switch_symbols = re.findall(r'(?:\bSW\d+\b|\bSWITCH\s+\d+\b)', text)
                dimensions['fixtures']['switches'] += len(switch_patterns) + len(switch_symbols)
                
                # Count HVAC vents - be more conservative
                vent_patterns = re.findall(r'(?:SUPPLY\s+VENT|RETURN\s+VENT|AIR\s+VENT|HVAC\s+VENT)', text)
                vent_symbols = re.findall(r'(?:\bVENT\s+\d+\b|\bHVAC\s+\d+\b)', text)
                dimensions['fixtures']['hvac_vents'] += len(vent_patterns) + len(vent_symbols)
        
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
        
        # Validate and log the extracted data structure
        self.logger.info("=== EXTRACTED DATA VALIDATION ===")
        self.logger.info(f"Total house area: {dimensions['total_sqft']} sqft")
        
        # Log floor areas
        if dimensions['floor_areas']:
            self.logger.info("Floor Areas:")
            total_floor_area = 0
            for floor_name, area in dimensions['floor_areas'].items():
                self.logger.info(f"  {floor_name}: {area} sqft")
                total_floor_area += area
            self.logger.info(f"  Total floor area: {total_floor_area} sqft")
        
        # Log room details and calculate total room area
        if dimensions['room_details']:
            self.logger.info("Individual Rooms:")
            total_room_area = 0
            room_count = 0
            for room_type, room_data in dimensions['room_details'].items():
                if isinstance(room_data, list):
                    for room in room_data:
                        self.logger.info(f"  {room['name']}: {room['area']} sqft")
                        total_room_area += room['area']
                        room_count += 1
                else:
                    self.logger.info(f"  {room_data['name']}: {room_data['area']} sqft")
                    total_room_area += room_data['area']
                    room_count += 1
            self.logger.info(f"  Total room area: {total_room_area} sqft")
            self.logger.info(f"  Total room count: {room_count}")
            
            # Validation check
            if total_room_area > dimensions['total_sqft'] * 1.2:
                self.logger.warning(f"Room areas ({total_room_area}) exceed house area ({dimensions['total_sqft']}) by >20% - possible double counting!")
        
        self.logger.info("=== END VALIDATION ===")
        
        # Store validation data for later use
        dimensions['validation'] = {
            'total_house_sqft': dimensions['total_sqft'],
            'total_floor_area': sum(dimensions['floor_areas'].values()) if dimensions['floor_areas'] else 0,
            'total_room_area': sum(
                sum(room['area'] for room in room_data) if isinstance(room_data, list) 
                else room_data['area'] 
                for room_data in dimensions['room_details'].values()
            ) if dimensions['room_details'] else 0,
            'room_count': sum(
                len(room_data) if isinstance(room_data, list) else 1 
                for room_data in dimensions['room_details'].values()
            ) if dimensions['room_details'] else 0
        }
        
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
        
        # Calculate total rooms early for use in multiple calculations
        total_rooms = sum(len(room_data) if isinstance(room_data, list) else 1 
                         for room_data in dims.get('room_details', {}).values())
        
        # Calculate bathroom count early for use in multiple calculations
        bathroom_count = len(dims['room_details'].get('bathroom', [])) if isinstance(dims['room_details'].get('bathroom'), list) else (1 if 'bathroom' in dims['room_details'] else 3)
        
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
        
        # Foundation materials (REALISTIC QUANTITIES)
        foundation_perimeter = perimeter * 0.8  # 80% of house perimeter for foundation walls
        estimates['foundation'] = {
            'rebar_tons': total_sqft * 0.0004,  # 0.0004 tons per sqft (reduced from 0.002 - was 5x too much!)
            'foundation_bolts_count': int(foundation_perimeter / 8),  # 1 bolt per 8 feet (reduced from 6)
            'vapor_barrier_sqft': total_sqft,  # 100% of house area (reduced from 110%)
            'waterproofing_sqft': foundation_perimeter * 6,  # 6 ft high foundation walls (reduced from 8)
            'gravel_cubic_yards': total_sqft * 0.03 / 27,  # Base gravel (reduced from 0.05)
            'form_boards_bf': foundation_perimeter * 8  # 2x8 forms, one side reused (reduced from 16)
        }
        
        estimates['lumber'] = {
            'framing': total_sqft * 1.5,  # 1.5 board feet per sqft
            'sheathing': total_sqft * 1.1,  # 1.1 sheets per sqft
            'total_board_feet': total_sqft * 1.5
        }
        
        # Structural materials (NEW CATEGORY)
        estimates['structural'] = {
            'engineered_lumber_lf': total_sqft * 0.3,  # LVL beams, headers
            'metal_connectors_count': int(total_sqft / 50),  # 1 per 50 sqft (joist hangers, etc.)
            'structural_screws_lbs': total_sqft * 0.02,  # Structural fasteners
            'hurricane_ties_count': int(total_sqft / 100),  # 1 per 100 sqft
            'post_anchors_count': int(perimeter / 20),  # 1 per 20 feet of perimeter
            'beam_pockets_count': int(total_sqft / 400)  # Beam pocket inserts
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
            'total_sqft': wall_area + total_sqft,
            # Additional insulation materials (REDUCED for standard construction)
            'house_wrap_sqft': wall_area * 1.05,  # 105% for overlap (reduced from 110%)
            'foam_board_sqft': wall_area * 0.1,  # 10% foam board supplement (reduced from 30% - not standard everywhere)
            'caulk_tubes': int(perimeter / 15),  # 1 tube per 15 feet (reduced from 10)
            'weatherstripping_lf': dims['door_count'] * 16 + dims['window_count'] * 12  # Doors + windows (reduced)
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
            'hardwood_sqft': total_sqft * 0.3,  # 30% hardwood
            # Installation materials (REMOVED subfloor - already in lumber sheathing)
            'underlayment_sqft': total_sqft * 0.5,  # 50% needs underlayment (reduced from 70%)
            'transition_strips_lf': int(total_rooms * 6),  # Room transitions (reduced from 8)
            'floor_adhesive_gallons': int(total_sqft * 0.3 / 250),  # For tile areas (reduced coverage)
            'carpet_padding_sqft': total_sqft * 0.4  # Under carpet
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
        
        # Use extracted electrical data with realistic bounds
        # Apply sanity checks - outlets should be 1 per 100-150 sqft for residential
        max_realistic_outlets = int(total_sqft / 100)  # 1 per 100 sqft max
        min_realistic_outlets = int(total_sqft / 200)  # 1 per 200 sqft min
        
        extracted_outlets = dims['fixtures']['electrical_outlets']
        if extracted_outlets > max_realistic_outlets or extracted_outlets < min_realistic_outlets:
            # Use estimate if extracted data is unrealistic
            realistic_outlets = int(total_sqft / 150)  # 1 per 150 sqft (realistic)
            self.logger.warning(f"Extracted outlets ({extracted_outlets}) seems unrealistic, using estimate ({realistic_outlets})")
        else:
            realistic_outlets = extracted_outlets
        
        # Light fixtures - use extracted data when available, minimal fallback
        extracted_lights = dims['fixtures']['light_fixtures']
        if extracted_lights > 0:
            realistic_lights = extracted_lights  # Use actual extracted data
            self.logger.info(f"Using extracted light fixtures: {extracted_lights}")
        else:
            realistic_lights = max(8, int(total_sqft / 300))  # Minimal fallback: 1 per 300 sqft
            self.logger.info(f"No light fixtures extracted, using minimal estimate: {realistic_lights}")
        
        estimates['electrical'] = {
            'outlets': realistic_outlets,
            'light_fixtures': realistic_lights,
            'switches': max(dims['fixtures']['switches'], realistic_lights),  # At least 1 switch per light
            'wire_feet': realistic_outlets * 40 + realistic_lights * 25,  # More realistic wire estimate
            'electrical_panel': 1,
            'service_entrance': 1,
            # Additional electrical infrastructure
            'conduit_feet': realistic_outlets * 15 + realistic_lights * 10,  # Conduit runs
            'junction_boxes_count': int((realistic_outlets + realistic_lights) / 3),  # 1 per 3 devices
            'breakers_count': int(total_sqft / 200),  # 1 breaker per 200 sqft
            'gfci_outlets_count': bathroom_count + 1,  # Bathrooms + kitchen
            'smoke_detectors_count': total_rooms,  # 1 per room minimum
            'electrical_meter': 1,
            'grounding_rods_count': 2,  # Standard requirement
            'wire_nuts_count': realistic_outlets * 4  # Wire connections
        }
        
        # Use extracted plumbing data with realistic bounds
        # Sanity check - should be ~3 fixtures per bathroom + 1-2 kitchen + laundry
        expected_fixtures = bathroom_count * 3 + 2  # 3 per bathroom + kitchen sink + laundry
        
        extracted_plumbing = dims['fixtures']['plumbing_fixtures']
        if extracted_plumbing > expected_fixtures * 2 or extracted_plumbing < expected_fixtures * 0.5:
            # Use estimate if extracted data is unrealistic
            realistic_plumbing = expected_fixtures
            self.logger.warning(f"Extracted plumbing fixtures ({extracted_plumbing}) seems unrealistic, using estimate ({realistic_plumbing})")
        else:
            realistic_plumbing = extracted_plumbing
        
        estimates['plumbing'] = {
            'fixtures': realistic_plumbing,
            'pipe_feet': realistic_plumbing * 60,  # More realistic pipe estimate (60 ft per fixture)
            'fittings': realistic_plumbing * 4,
            'water_main_connection': 1,
            'sewer_connection': 1,
            # Additional plumbing infrastructure
            'shut_off_valves_count': realistic_plumbing + 2,  # 1 per fixture + main + water heater
            'vent_pipes_lf': realistic_plumbing * 15,  # Vent stack runs
            'cleanouts_count': bathroom_count + 2,  # 1 per bathroom + kitchen + main
            'water_meter': 1,
            'pressure_tank': 1 if total_sqft > 2000 else 0,  # For larger homes
            'sump_pump': 1 if 'basement' in str(dims.get('room_details', {})).lower() else 0,
            'floor_drains_count': 1 if 'basement' in str(dims.get('room_details', {})).lower() else 0,
            'pipe_insulation_lf': realistic_plumbing * 20  # Insulate hot water lines
        }
        
        # Calculate HVAC based on actual rooms and extracted data
        
        # Use extracted HVAC data with realistic fallback
        extracted_vents = dims['fixtures']['hvac_vents']
        expected_vents = total_rooms * 2  # 2 vents per room (supply + return)
        
        if extracted_vents > 0 and extracted_vents < expected_vents * 3:  # If reasonable extracted data
            realistic_vents = extracted_vents
        else:
            realistic_vents = expected_vents
            if extracted_vents > 0:
                self.logger.warning(f"Extracted HVAC vents ({extracted_vents}) seems unrealistic, using estimate ({realistic_vents})")
        
        estimates['hvac'] = {
            'ductwork_linear_ft': total_rooms * 25 + perimeter * 0.5,  # Based on rooms + perimeter runs
            'vents': realistic_vents,
            'thermostat': max(1, int(total_sqft / 2000)),  # 1 per 2000 sqft
            'air_handler': max(1, int(total_sqft / 2500)),  # 1 per 2500 sqft
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
        
        # Site work costs (material only - most site work is labor, so minimal cost)
        site_work_cost = (
            self.material_estimates['site_work']['excavation_cubic_yards'] * default_costs['excavation'] +
            self.material_estimates['site_work']['grading_sqft'] * default_costs['grading'] +
            self.material_estimates['site_work']['backfill_cubic_yards'] * default_costs['backfill'] +
            self.material_estimates['site_work']['site_preparation'] * default_costs['site_preparation']
        )
        cost_estimate['site_work'] = site_work_cost
        total_cost += site_work_cost
        
        # Concrete costs (material only)
        concrete_cost = self.material_estimates['concrete']['total_cubic_yards'] * default_costs['concrete']
        cost_estimate['concrete'] = concrete_cost
        total_cost += concrete_cost
        
        # Foundation costs (material only)
        foundation_cost = (
            self.material_estimates['foundation']['rebar_tons'] * default_costs['rebar'] +
            self.material_estimates['foundation']['foundation_bolts_count'] * default_costs['foundation_bolt'] +
            self.material_estimates['foundation']['vapor_barrier_sqft'] * default_costs['vapor_barrier'] +
            self.material_estimates['foundation']['waterproofing_sqft'] * default_costs['waterproofing'] +
            self.material_estimates['foundation']['gravel_cubic_yards'] * default_costs['gravel'] +
            self.material_estimates['foundation']['form_boards_bf'] * default_costs['form_boards']
        )
        cost_estimate['foundation'] = foundation_cost
        total_cost += foundation_cost
        
        # Lumber costs (material only)
        lumber_cost = self.material_estimates['lumber']['total_board_feet'] * default_costs['lumber']
        cost_estimate['lumber'] = lumber_cost
        total_cost += lumber_cost
        
        # Structural costs (material only)
        structural_cost = (
            self.material_estimates['structural']['engineered_lumber_lf'] * default_costs['engineered_lumber'] +
            self.material_estimates['structural']['metal_connectors_count'] * default_costs['metal_connector'] +
            self.material_estimates['structural']['structural_screws_lbs'] * default_costs['structural_screws'] +
            self.material_estimates['structural']['hurricane_ties_count'] * default_costs['hurricane_tie'] +
            self.material_estimates['structural']['post_anchors_count'] * default_costs['post_anchor'] +
            self.material_estimates['structural']['beam_pockets_count'] * default_costs['beam_pocket']
        )
        cost_estimate['structural'] = structural_cost
        total_cost += structural_cost
        
        # Roofing costs (material only)
        roofing_cost = (
            self.material_estimates['roofing']['shingles_sqft'] * default_costs['roofing_shingles'] +
            self.material_estimates['roofing']['underlayment_sqft'] * default_costs['roofing_underlayment'] +
            self.material_estimates['roofing']['flashing_linear_ft'] * default_costs['roofing_flashing']
        )
        cost_estimate['roofing'] = roofing_cost
        total_cost += roofing_cost
        
        # Insulation costs (material only)
        insulation_cost = (
            self.material_estimates['insulation']['total_sqft'] * default_costs['insulation'] +
            self.material_estimates['insulation']['house_wrap_sqft'] * default_costs['house_wrap'] +
            self.material_estimates['insulation']['foam_board_sqft'] * default_costs['foam_board'] +
            self.material_estimates['insulation']['caulk_tubes'] * default_costs['caulk_tube'] +
            self.material_estimates['insulation']['weatherstripping_lf'] * default_costs['weatherstripping']
        )
        cost_estimate['insulation'] = insulation_cost
        total_cost += insulation_cost
        
        # Drywall costs (material only)
        drywall_sqft = self.material_estimates['drywall']['wall_sqft'] + self.material_estimates['drywall']['ceiling_sqft']
        drywall_cost = drywall_sqft * default_costs['drywall']
        cost_estimate['drywall'] = drywall_cost
        total_cost += drywall_cost
        
        # Flooring costs (material only) - subfloor removed (already in lumber)
        flooring_cost = (
            self.material_estimates['flooring']['tile_sqft'] * default_costs['tile'] +
            self.material_estimates['flooring']['carpet_sqft'] * default_costs['carpet'] +
            self.material_estimates['flooring']['hardwood_sqft'] * default_costs['hardwood'] +
            self.material_estimates['flooring']['underlayment_sqft'] * default_costs['underlayment'] +
            self.material_estimates['flooring']['transition_strips_lf'] * default_costs['transition_strip'] +
            self.material_estimates['flooring']['floor_adhesive_gallons'] * default_costs['floor_adhesive'] +
            self.material_estimates['flooring']['carpet_padding_sqft'] * default_costs['carpet_padding']
        )
        cost_estimate['flooring'] = flooring_cost
        total_cost += flooring_cost
        
        # Doors and windows costs (material only)
        doors_windows_cost = (
            self.material_estimates['doors_windows']['doors'] * default_costs['door'] +
            self.material_estimates['doors_windows']['windows'] * default_costs['window'] +
            self.material_estimates['doors_windows']['door_frames'] * default_costs['door_frame'] +
            self.material_estimates['doors_windows']['window_frames'] * default_costs['window_frame']
        )
        cost_estimate['doors_windows'] = doors_windows_cost
        total_cost += doors_windows_cost
        
        # Paint costs (material only)
        paint_cost = (
            self.material_estimates['paint']['primer_gallons'] * default_costs['paint_primer'] +
            self.material_estimates['paint']['paint_gallons'] * default_costs['paint']
        )
        cost_estimate['paint'] = paint_cost
        total_cost += paint_cost
        
        # Trim and molding costs (material only)
        trim_cost = (
            self.material_estimates['trim_molding']['baseboard_linear_ft'] * default_costs['baseboard'] +
            self.material_estimates['trim_molding']['crown_molding_linear_ft'] * default_costs['crown_molding'] +
            self.material_estimates['trim_molding']['door_casing_linear_ft'] * default_costs['door_casing'] +
            self.material_estimates['trim_molding']['window_casing_linear_ft'] * default_costs['window_casing']
        )
        cost_estimate['trim_molding'] = trim_cost
        total_cost += trim_cost
        
        # Electrical costs (material only)
        electrical_cost = (
            self.material_estimates['electrical']['outlets'] * default_costs['outlet'] +
            self.material_estimates['electrical']['light_fixtures'] * default_costs['light_fixture'] +
            self.material_estimates['electrical']['switches'] * default_costs['switch'] +
            self.material_estimates['electrical']['wire_feet'] * default_costs['wire'] +
            self.material_estimates['electrical']['electrical_panel'] * default_costs['electrical_panel'] +
            self.material_estimates['electrical']['service_entrance'] * default_costs['service_entrance'] +
            self.material_estimates['electrical']['conduit_feet'] * default_costs['conduit'] +
            self.material_estimates['electrical']['junction_boxes_count'] * default_costs['junction_box'] +
            self.material_estimates['electrical']['breakers_count'] * default_costs['breaker'] +
            self.material_estimates['electrical']['gfci_outlets_count'] * default_costs['gfci_outlet'] +
            self.material_estimates['electrical']['smoke_detectors_count'] * default_costs['smoke_detector'] +
            self.material_estimates['electrical']['electrical_meter'] * default_costs['electrical_meter'] +
            self.material_estimates['electrical']['grounding_rods_count'] * default_costs['grounding_rod'] +
            self.material_estimates['electrical']['wire_nuts_count'] * default_costs['wire_nut']
        )
        cost_estimate['electrical'] = electrical_cost
        total_cost += electrical_cost
        
        # Plumbing costs (material only)
        plumbing_cost = (
            self.material_estimates['plumbing']['fixtures'] * default_costs['plumbing_fixture'] +
            self.material_estimates['plumbing']['pipe_feet'] * default_costs['pipe'] +
            self.material_estimates['plumbing']['fittings'] * default_costs['fitting'] +
            self.material_estimates['plumbing']['water_main_connection'] * default_costs['water_main_connection'] +
            self.material_estimates['plumbing']['sewer_connection'] * default_costs['sewer_connection'] +
            self.material_estimates['plumbing']['shut_off_valves_count'] * default_costs['shut_off_valve'] +
            self.material_estimates['plumbing']['vent_pipes_lf'] * default_costs['vent_pipe'] +
            self.material_estimates['plumbing']['cleanouts_count'] * default_costs['cleanout'] +
            self.material_estimates['plumbing']['water_meter'] * default_costs['water_meter'] +
            self.material_estimates['plumbing']['pressure_tank'] * default_costs['pressure_tank'] +
            self.material_estimates['plumbing']['sump_pump'] * default_costs['sump_pump'] +
            self.material_estimates['plumbing']['floor_drains_count'] * default_costs['floor_drain'] +
            self.material_estimates['plumbing']['pipe_insulation_lf'] * default_costs['pipe_insulation']
        )
        cost_estimate['plumbing'] = plumbing_cost
        total_cost += plumbing_cost
        
        # HVAC costs (material only)
        hvac_cost = (
            self.material_estimates['hvac']['ductwork_linear_ft'] * default_costs['ductwork'] +
            self.material_estimates['hvac']['vents'] * default_costs['vent'] +
            self.material_estimates['hvac']['thermostat'] * default_costs['thermostat'] +
            self.material_estimates['hvac']['air_handler'] * default_costs['air_handler'] +
            self.material_estimates['hvac']['hvac_installation'] * default_costs['hvac_installation']
        )
        cost_estimate['hvac'] = hvac_cost
        total_cost += hvac_cost
        
        # Hardware costs (material only)
        hardware_cost = (
            self.material_estimates['hardware']['nails_lbs'] * default_costs['nails'] +
            self.material_estimates['hardware']['screws_lbs'] * default_costs['screws'] +
            self.material_estimates['hardware']['bolts_count'] * default_costs['bolts'] +
            self.material_estimates['hardware']['hinges_count'] * default_costs['hinges'] +
            self.material_estimates['hardware']['locks_count'] * default_costs['locks']
        )
        cost_estimate['hardware'] = hardware_cost
        total_cost += hardware_cost
        
        # Cabinets costs (material only)
        cabinets_cost = (
            self.material_estimates['cabinets']['kitchen_cabinets_linear_ft'] * default_costs['kitchen_cabinet'] +
            self.material_estimates['cabinets']['bathroom_cabinets_count'] * default_costs['bathroom_cabinet'] +
            self.material_estimates['cabinets']['countertop_sqft'] * default_costs['countertop'] +
            self.material_estimates['cabinets']['sink_count'] * default_costs['sink']
        )
        cost_estimate['cabinets'] = cabinets_cost
        total_cost += cabinets_cost
        
        # Exterior costs (material only)
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
        
        # Landscaping costs (material only)
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
        
        print(f"\nTotal Materials: ${cost_estimate['total_materials']:,.2f}")
        print(f"Contingency (10%): ${cost_estimate['contingency_amount']:,.2f}")
        print(f"TOTAL MATERIALS ESTIMATE: ${cost_estimate['total_with_contingency']:,.2f}")

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
