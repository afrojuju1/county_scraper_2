#!/usr/bin/env python3
"""
Tesseract-Based Construction Plans Takeoff Estimator
Focused on accurate text extraction from construction plans using Tesseract OCR
"""

import pytesseract
import cv2
import numpy as np
from pdf2image import convert_from_path
from PIL import Image
import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import concurrent.futures
import threading
from datetime import datetime
import time
import argparse
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TesseractTakeoffExtractor:
    """Tesseract-based extraction for construction plans"""
    
    def __init__(self, pdf_path: str):
        self.pdf_path = Path(pdf_path)
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        self.logger = logging.getLogger(f"{__name__}_{id(self)}")
        self.extracted_data = {}
        self.lock = threading.Lock()  # Thread safety for logging
        
        # Tesseract configuration for construction plans
        self.tesseract_configs = {
            'default': '--psm 6',
            'dimensions': '--psm 8',
            'room_labels': '--psm 7',
            'numbers_only': '--psm 8'
        }
    
    def extract_text_from_pdf(self) -> List[str]:
        """Extract text from PDF using Tesseract OCR with optimized preprocessing"""
        self.logger.info("Starting Tesseract OCR extraction...")
        
        # Convert PDF to images with high DPI for better OCR
        images = convert_from_path(str(self.pdf_path), dpi=300)
        self.logger.info(f"Converted PDF to {len(images)} images at 300 DPI")
        
        extracted_texts = []
        
        for page_num, image in enumerate(images):
            self.logger.info(f"Processing page {page_num + 1}/{len(images)}")
            
            # Preprocess image for better OCR
            processed_image = self._preprocess_image_for_ocr(image)
            
            # Extract text using different Tesseract configurations
            page_texts = self._extract_text_multiple_configs(processed_image, page_num)
            
            # Combine and clean the extracted text
            best_text = self._combine_page_texts(page_texts)
            extracted_texts.append(best_text)
            
            self.logger.info(f"Page {page_num + 1} extracted: {len(best_text)} characters")
        
        return extracted_texts
    
    def _preprocess_image_for_ocr(self, image: Image.Image) -> np.ndarray:
        """Preprocess image for optimal OCR results"""
        # Convert PIL to OpenCV format
        img_array = np.array(image)
        img_cv = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
        
        # Convert to grayscale
        gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
        
        # Apply adaptive thresholding for better text contrast
        thresh = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
        )
        
        # Morphological operations to clean up the image
        kernel = np.ones((1, 1), np.uint8)
        cleaned = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
        
        # Denoise
        denoised = cv2.medianBlur(cleaned, 3)
        
        return denoised
    
    def _extract_text_multiple_configs(self, image: np.ndarray, page_num: int) -> Dict[str, str]:
        """Extract text using multiple Tesseract configurations"""
        page_texts = {}
        
        for config_name, config in self.tesseract_configs.items():
            try:
                text = pytesseract.image_to_string(image, config=config)
                page_texts[config_name] = text
                self.logger.debug(f"Page {page_num + 1} - {config_name}: {len(text)} chars")
            except Exception as e:
                self.logger.warning(f"Failed to extract with {config_name}: {e}")
                page_texts[config_name] = ""
        
        return page_texts
    
    def _combine_page_texts(self, page_texts: Dict[str, str]) -> str:
        """Combine texts from different configurations to get the best result"""
        # Score each configuration based on construction-specific criteria
        scores = {}
        
        for config_name, text in page_texts.items():
            if not text or not text.strip():
                scores[config_name] = 0
                continue
            
            score = 0
            
            # Prefer text with construction/architectural terms
            construction_terms = [
                'FLOOR', 'ROOM', 'BEDROOM', 'BATHROOM', 'KITCHEN', 'LIVING', 'GARAGE',
                'SQ', 'FT', 'DIMENSION', 'WALL', 'DOOR', 'WINDOW', 'CEILING', 'HEIGHT'
            ]
            term_count = sum(1 for term in construction_terms if term in text.upper())
            score += term_count * 10
            
            # Prefer text with reasonable length
            text_length = len(text.strip())
            if 100 <= text_length <= 3000:
                score += 20
            elif text_length > 3000:
                score += 10
            
            # Prefer text with alphanumeric content
            alphanumeric_ratio = sum(c.isalnum() for c in text) / len(text) if text else 0
            score += alphanumeric_ratio * 30
            
            # Penalize garbled text
            special_char_ratio = sum(not c.isalnum() and not c.isspace() for c in text) / len(text) if text else 0
            if special_char_ratio > 0.4:
                score -= 15
            
            scores[config_name] = score
        
        # Return the text with the highest score
        if scores:
            best_config = max(scores.keys(), key=lambda k: scores[k])
            return page_texts[best_config]
        
        return ""
    
    def extract_construction_data(self) -> Dict:
        """Extract construction-specific data from the OCR text"""
        self.logger.info("Extracting construction data from OCR text...")
        
        # Get OCR text
        page_texts = self.extract_text_from_pdf()
        full_text = "\n".join(page_texts)
        
        # Initialize data structure
        construction_data = {
            'total_sqft': 0,
            'floor_areas': {},
            'rooms': {},
            'dimensions': [],
            'fixtures': {
                'doors': 0,
                'windows': 0,
                'electrical_outlets': 0,
                'light_fixtures': 0,
                'plumbing_fixtures': 0,
                'switches': 0,
                'hvac_vents': 0
            },
            'ceiling_heights': [],
            'wall_lengths': [],
            'structural_details': {
                'foundation': {
                    'type': None,
                    'area_sqft': 0,
                    'thickness': None,
                    'details': []
                },
                'roof': {
                    'type': None,
                    'pitch': None,
                    'area_sqft': 0,
                    'material': None,
                    'details': []
                },
                'beams': {
                    'sizes': [],
                    'count': 0,
                    'details': []
                },
                'joists': {
                    'size': None,
                    'spacing': None,
                    'count': 0,
                    'details': []
                },
                'framing': {
                    'stud_size': None,
                    'stud_spacing': None,
                    'details': []
                }
            },
            'system_details': {
                'hvac': {
                    'equipment': {
                        'type': None,
                        'capacity': None,
                        'efficiency': None,
                        'details': []
                    },
                    'ductwork': {
                        'material': None,
                        'sizes': [],
                        'linear_feet': 0,
                        'details': []
                    },
                    'ventilation': {
                        'exhaust_fans': 0,
                        'fresh_air_intake': None,
                        'details': []
                    }
                },
                'plumbing': {
                    'water_heater': {
                        'type': None,
                        'capacity': None,
                        'fuel_type': None,
                        'details': []
                    },
                    'pipes': {
                        'water_supply': None,
                        'drain_waste_vent': None,
                        'sizes': [],
                        'details': []
                    },
                    'fixtures': {
                        'toilets': 0,
                        'sinks': 0,
                        'tubs': 0,
                        'showers': 0,
                        'details': []
                    }
                },
                'electrical': {
                    'main_panel': {
                        'size': None,
                        'amperage': None,
                        'circuits': 0,
                        'details': []
                    },
                    'wire': {
                        'gauge': None,
                        'type': None,
                        'linear_feet': 0,
                        'details': []
                    },
                    'outlets': {
                        'standard': 0,
                        'gfci': 0,
                        'dedicated': 0,
                        'details': []
                    }
                }
            },
            'material_specifications': {
                'insulation': {
                    'wall_r_value': None,
                    'ceiling_r_value': None,
                    'floor_r_value': None,
                    'type': None,
                    'details': []
                },
                'siding': {
                    'type': None,
                    'material': None,
                    'area_sqft': 0,
                    'details': []
                },
                'flooring': {
                    'types': [],
                    'areas': {},
                    'details': []
                },
                'interior_finishes': {
                    'drywall': {
                        'thickness': None,
                        'area_sqft': 0,
                        'details': []
                    },
                    'paint': {
                        'primer': None,
                        'finish': None,
                        'area_sqft': 0,
                        'details': []
                    },
                    'trim': {
                        'baseboard': 0,
                        'crown_molding': 0,
                        'casing': 0,
                        'details': []
                    }
                }
            },
            'raw_text': full_text
        }
        
        # Extract different types of data
        self._extract_square_footage(full_text, construction_data)
        self._extract_floor_areas(full_text, construction_data)
        self._extract_room_data(full_text, construction_data)
        self._extract_dimensions(full_text, construction_data)
        self._extract_fixtures(full_text, construction_data)
        self._extract_ceiling_heights(full_text, construction_data)
        
        # Add validation and room estimation logic from takeoff_estimator.py
        self._validate_and_estimate_rooms(construction_data)
        
        # Add default ceiling heights if none found (CRITICAL for material estimates)
        self._add_default_ceiling_heights(construction_data)
        
        # Calculate wall areas (CRITICAL for material estimates)
        self._calculate_wall_areas(construction_data)
        
        # Extract structural details (foundation, roof, beams, joists)
        self._extract_structural_details(full_text, construction_data)
        
        # Extract system details (HVAC, plumbing, electrical)
        self._extract_system_details(full_text, construction_data)
        
        # Extract material specifications
        self._extract_material_specifications(full_text, construction_data)
        
        return construction_data
    
    def _validate_and_estimate_rooms(self, data: Dict):
        """Add default room estimates if we don't have enough room details"""
        # This helps provide a more complete takeoff estimate
        if len(data['rooms']) < 5:  # If we have fewer than 5 room types
            total_sqft = data['total_sqft']
            
            # Get floor areas for better room distribution
            first_floor_area = data['floor_areas'].get('first_floor', 0)
            second_floor_area = data['floor_areas'].get('second_floor', 0)
            
            # Estimate typical rooms based on total square footage and floor distribution
            if 'bedroom' not in data['rooms']:
                # Estimate 2-4 bedrooms for a 2400+ sqft house
                num_bedrooms = min(4, max(2, int(total_sqft / 600)))
                data['rooms']['bedroom'] = []
                for i in range(num_bedrooms):
                    bedroom_size = 120 + (i * 20)  # 120, 140, 160, 180 sqft
                    data['rooms']['bedroom'].append({
                        'name': f'BEDROOM {i+1}',
                        'area': bedroom_size,
                        'type': 'bedroom'
                    })
            
            if 'bathroom' not in data['rooms']:
                # Estimate 2-3 bathrooms for a 2400+ sqft house
                num_bathrooms = min(3, max(2, int(total_sqft / 800)))
                data['rooms']['bathroom'] = []
                for i in range(num_bathrooms):
                    bathroom_size = 60 if i == 0 else 40  # Master bath larger
                    data['rooms']['bathroom'].append({
                        'name': f'BATHROOM {i+1}',
                        'area': bathroom_size,
                        'type': 'bathroom'
                    })
            
            # Check if we should convert small bathrooms to half baths based on half bath indicators
            if 'half_bath' not in data['rooms']:
                # Look for half bath indicators in the raw text
                half_bath_indicators = re.findall(r'(?:HALF|POWDER|1/2|WC|W\.C\.)', data.get('raw_text', ''), re.IGNORECASE)
                if half_bath_indicators and len(half_bath_indicators) > 3:  # Multiple indicators suggest half baths
                    self.logger.info(f"Found {len(half_bath_indicators)} half bath indicators, converting small bathrooms...")
                    
                    # Convert the smallest bathroom to half bath
                    if 'bathroom' in data['rooms'] and isinstance(data['rooms']['bathroom'], list):
                        smallest_bath = min(data['rooms']['bathroom'], key=lambda x: x['area'])
                        if smallest_bath['area'] <= 50:  # Small bathroom
                            # Remove from regular bathrooms
                            data['rooms']['bathroom'] = [r for r in data['rooms']['bathroom'] if r != smallest_bath]
                            
                            # Add as half bath
                            data['rooms']['half_bath'] = [{
                                'name': 'HALF BATH',
                                'area': smallest_bath['area'],
                                'type': 'half_bath'
                            }]
                            self.logger.info(f"Converted estimated bathroom ({smallest_bath['area']} sqft) to HALF BATH")
            
            if 'kitchen' not in data['rooms']:
                # Estimate kitchen size - typically on first floor
                kitchen_size = min(200, max(120, int(first_floor_area / 4)))
                data['rooms']['kitchen'] = {
                    'name': 'KITCHEN',
                    'area': kitchen_size,
                    'type': 'kitchen'
                }
            
            if 'dining' not in data['rooms']:
                # Estimate dining room size - typically on first floor
                dining_size = min(150, max(80, int(first_floor_area / 6)))
                data['rooms']['dining'] = {
                    'name': 'DINING',
                    'area': dining_size,
                    'type': 'dining'
                }
            
            # Add living room if not found
            if 'living' not in data['rooms']:
                # Estimate living room size - typically on first floor
                living_size = min(300, max(200, int(first_floor_area / 3)))
                data['rooms']['living'] = {
                    'name': 'LIVING ROOM',
                    'area': living_size,
                    'type': 'living'
                }
            
            # Add family room if we have second floor
            if second_floor_area > 0 and 'family' not in data['rooms']:
                family_size = min(250, max(150, int(second_floor_area / 4)))
                data['rooms']['family'] = {
                    'name': 'FAMILY ROOM',
                    'area': family_size,
                    'type': 'family'
                }
        
        # Apply window count logic after processing all pages
        min_windows = max(8, int(data['total_sqft'] / 200))
        max_windows = int(data['total_sqft'] / 100)
        
        if data['fixtures']['windows'] > max_windows:
            data['fixtures']['windows'] = min_windows
            self.logger.warning(f"Window count {data['fixtures']['windows']} seems too high, using estimated {min_windows}")
        else:
            data['fixtures']['windows'] = min(max(data['fixtures']['windows'], min_windows), max_windows)
        
        # Log validation results
        self.logger.info("=== EXTRACTED DATA VALIDATION ===")
        self.logger.info(f"Total house area: {data['total_sqft']} sqft")
        
        # Log floor areas
        if data['floor_areas']:
            self.logger.info("Floor Areas:")
            total_floor_area = 0
            for floor_name, area in data['floor_areas'].items():
                self.logger.info(f"  {floor_name}: {area} sqft")
                total_floor_area += area
            self.logger.info(f"  Total floor area: {total_floor_area} sqft")
        
        # Log room details and calculate total room area
        if data['rooms']:
            self.logger.info("Individual Rooms:")
            total_room_area = 0
            room_count = 0
            for room_type, room_data in data['rooms'].items():
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
            if total_room_area > data['total_sqft'] * 1.2:
                self.logger.warning(f"Room areas ({total_room_area}) exceed house area ({data['total_sqft']}) by >20% - possible double counting!")
        
        self.logger.info("=== END VALIDATION ===")
    
    def _extract_square_footage(self, text: str, data: Dict):
        """Extract total square footage with high priority patterns"""
        # High priority patterns for total square footage
        patterns = [
            r'TOTAL\s+COVERED\s+(\d+)',
            r'TOTAL\s+LIVING\s+(\d+)',
            r'TOTAL\s+AREA\s+(\d+)',
            r'TOTAL\s+SQ\.?\s*FT\.?\s*(\d+)',
            r'TOTAL\s+(\d+)\s*SQ\.?\s*FT\.?'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                sqft = int(match)
                if sqft > data['total_sqft'] and sqft >= 500:  # Reasonable minimum
                    data['total_sqft'] = sqft
                    self.logger.info(f"Found total square footage: {sqft} sqft")
    
    def _extract_floor_areas(self, text: str, data: Dict):
        """Extract floor-specific areas"""
        floor_patterns = [
            r'(FIRST|SECOND|THIRD|FOURTH)\s+FLOOR\s+(?:LIVING|AREA)?\s*(\d+)',
            r'(FIRST|SECOND|THIRD|FOURTH)\s+FLOOR\s+(\d+)\s*SQ\.?\s*FT\.?'
        ]
        
        for pattern in floor_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for floor_name, area in matches:
                floor_area = int(area)
                if floor_area >= 100:  # Reasonable minimum
                    floor_key = f'{floor_name.lower()}_floor'
                    data['floor_areas'][floor_key] = floor_area
                    self.logger.info(f"Found {floor_name} floor: {floor_area} sqft")
    
    def _extract_room_data(self, text: str, data: Dict):
        """Extract room-specific data using sophisticated patterns from takeoff_estimator.py"""
        # Track found areas to avoid duplicates
        found_areas = set()
        
        # First, identify floor area patterns to exclude them (these are totals, not individual rooms)
        floor_area_patterns = set()
        floor_matches = re.findall(r'(?:FIRST|SECOND|THIRD|FOURTH)\s+FLOOR\s+(?:LIVING|AREA)?\s*(\d+)', text, re.IGNORECASE)
        for area in floor_matches:
            floor_area_patterns.add(int(area))
        
        # Room keywords - comprehensive list from working version
        room_keywords = [
            'DINING', 'KITCHEN', 'BEDROOM', 'BATH', 'GARAGE', 'PATIO', 'PORCH', 'OFFICE', 'STUDY', 'FAMILY', 'DEN', 
            'MASTER', 'CLOSET', 'LAUNDRY', 'POWDER', 'HALF', 'BREAKFAST', 'GREAT', 'LOFT', 'BONUS', 'FOYER', 
            'ENTRY', 'HALL', 'UTILITY', 'PANTRY', 'MUDROOM', 'SUNROOM', 'LIBRARY', 'MEDIA', 'GAME', 'RECREATION', 
            'EXERCISE', 'WORKSHOP'
        ]
        
        # Extract rooms using the proven patterns from takeoff_estimator.py
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
                        if room_key in data['rooms']:
                            # If we already have this room type, create a list or increment
                            if isinstance(data['rooms'][room_key], dict):
                                # Convert to list format
                                existing = data['rooms'][room_key]
                                data['rooms'][room_key] = [existing]
                            data['rooms'][room_key].append({
                                'name': keyword,
                                'area': area_val,
                                'type': self._classify_room_type(keyword)
                            })
                        else:
                            data['rooms'][room_key] = {
                                'name': keyword,
                                'area': area_val,
                                'type': self._classify_room_type(keyword)
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
                    
                    if room_key in data['rooms']:
                        if isinstance(data['rooms'][room_key], dict):
                            existing = data['rooms'][room_key]
                            data['rooms'][room_key] = [existing]
                        data['rooms'][room_key].append({
                            'name': 'LIVING ROOM',
                            'area': area_val,
                            'type': 'living'
                        })
                    else:
                        data['rooms'][room_key] = {
                            'name': 'LIVING ROOM',
                            'area': area_val,
                            'type': 'living'
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
                    if 'bedroom' not in data['rooms']:
                        data['rooms']['bedroom'] = []
                    data['rooms']['bedroom'].append({
                        'name': f'BEDROOM {bed_num}',
                        'area': area_val,
                        'type': 'bedroom'
                    })
        
        # Look for bathroom patterns like "BATH 1", "BATHROOM 1", "POWDER"
        bathroom_patterns = re.findall(r'(?:BATHROOM|BATH|POWDER)\s*(\d+)?\s+(\d+)', text, re.IGNORECASE)
        for bath_num, area in bathroom_patterns:
            if area.isdigit() and 20 <= int(area) <= 200:
                area_val = int(area)
                if area_val not in found_areas:
                    found_areas.add(area_val)
                    if 'bathroom' not in data['rooms']:
                        data['rooms']['bathroom'] = []
                    room_name = f'BATHROOM {bath_num}' if bath_num else 'BATHROOM'
                    data['rooms']['bathroom'].append({
                        'name': room_name,
                        'area': area_val,
                        'type': 'bathroom'
                    })
        
        # Look for half bath/powder room patterns specifically
        half_bath_patterns = [
            r'(?:HALF\s+BATH|POWDER\s+ROOM|POWDER|1/2\s+BATH)\s+(\d+)',
            r'(?:HALF\s+BATH|POWDER\s+ROOM|POWDER|1/2\s+BATH)\s*(\d+)',
            r'(\d+)\s*(?:HALF\s+BATH|POWDER\s+ROOM|POWDER|1/2\s+BATH)',
            r'(?:HALF|POWDER)\s*(\d+)',  # More flexible patterns
            r'(\d+)\s*(?:HALF|POWDER)',
            r'(?:WC|W\.C\.)\s*(\d+)',  # Water closet patterns
            r'(\d+)\s*(?:WC|W\.C\.)'
        ]
        
        # First, look for any half bath indicators in the text
        half_bath_indicators = re.findall(r'(?:HALF|POWDER|1/2|WC|W\.C\.)', text, re.IGNORECASE)
        if half_bath_indicators:
            self.logger.info(f"Found {len(half_bath_indicators)} half bath indicators in text: {half_bath_indicators[:5]}")
        
        half_bath_found = False
        for i, pattern in enumerate(half_bath_patterns):
            matches = re.findall(pattern, text, re.IGNORECASE)
            self.logger.debug(f"Half bath pattern {i+1}: Found {len(matches)} matches")
            for area in matches:
                if area.isdigit() and 15 <= int(area) <= 60:  # Half baths are typically smaller
                    area_val = int(area)
                    if area_val not in found_areas:
                        found_areas.add(area_val)
                        if 'half_bath' not in data['rooms']:
                            data['rooms']['half_bath'] = []
                        data['rooms']['half_bath'].append({
                            'name': 'HALF BATH',
                            'area': area_val,
                            'type': 'half_bath'
                        })
                        self.logger.info(f"Found room: HALF BATH = {area_val} sqft")
                        half_bath_found = True
        
        # If no explicit half bath found, look for small bathroom areas that might be half baths
        if not half_bath_found:
            self.logger.info("No explicit half bath found, checking for small bathroom areas...")
            # Look for small bathroom areas (15-50 sqft) that might be half baths
            small_bathroom_areas = []
            for room_type, room_data in data['rooms'].items():
                if room_type == 'bathroom' and isinstance(room_data, list):
                    for room in room_data:
                        if 15 <= room['area'] <= 50:  # Small bathroom might be half bath
                            small_bathroom_areas.append(room)
            
            self.logger.info(f"Found {len(small_bathroom_areas)} small bathroom areas: {[r['area'] for r in small_bathroom_areas]}")
            
            # If we have small bathrooms, convert the smallest one to half bath
            if small_bathroom_areas:
                smallest_bath = min(small_bathroom_areas, key=lambda x: x['area'])
                # Be more aggressive - if we found half bath indicators in text, convert any small bathroom
                if smallest_bath['area'] <= 50:  # More lenient threshold
                    # Remove from regular bathrooms
                    data['rooms']['bathroom'] = [r for r in data['rooms']['bathroom'] if r != smallest_bath]
                    
                    # Add as half bath
                    if 'half_bath' not in data['rooms']:
                        data['rooms']['half_bath'] = []
                    data['rooms']['half_bath'].append({
                        'name': 'HALF BATH',
                        'area': smallest_bath['area'],
                        'type': 'half_bath'
                    })
                    self.logger.info(f"Converted small bathroom ({smallest_bath['area']} sqft) to HALF BATH")
                    
                    # If we have multiple small bathrooms and found half bath indicators, convert another one
                    remaining_small = [r for r in small_bathroom_areas if r != smallest_bath and 15 <= r['area'] <= 45]
                    if remaining_small and len(half_bath_indicators) > 5:  # Multiple half bath indicators
                        second_smallest = min(remaining_small, key=lambda x: x['area'])
                        # Remove from regular bathrooms
                        data['rooms']['bathroom'] = [r for r in data['rooms']['bathroom'] if r != second_smallest]
                        
                        # Add as half bath
                        data['rooms']['half_bath'].append({
                            'name': 'HALF BATH',
                            'area': second_smallest['area'],
                            'type': 'half_bath'
                        })
                        self.logger.info(f"Converted second small bathroom ({second_smallest['area']} sqft) to HALF BATH")
        

        
        # Look for kitchen patterns
        kitchen_patterns = re.findall(r'KITCHEN\s+(\d+)', text, re.IGNORECASE)
        for area in kitchen_patterns:
            if area.isdigit() and 50 <= int(area) <= 300:
                area_val = int(area)
                if area_val not in found_areas:
                    found_areas.add(area_val)
                    data['rooms']['kitchen'] = {
                        'name': 'KITCHEN',
                        'area': area_val,
                        'type': 'kitchen'
                    }
        
        # Look for family room patterns (common in house plans)
        family_patterns = re.findall(r'(?:FAMILY\s+ROOM|FAMILY)\s+(\d+)', text, re.IGNORECASE)
        for area in family_patterns:
            if area.isdigit() and 100 <= int(area) <= 600:
                area_val = int(area)
                if area_val not in found_areas:
                    found_areas.add(area_val)
                    data['rooms']['family'] = {
                        'name': 'FAMILY ROOM',
                        'area': area_val,
                        'type': 'family'
                    }
                    self.logger.info(f"Found room: FAMILY ROOM = {area_val} sqft")
        
        # Look for dining room patterns
        dining_patterns = re.findall(r'(?:DINING\s+ROOM|DINING)\s+(\d+)', text, re.IGNORECASE)
        for area in dining_patterns:
            if area.isdigit() and 80 <= int(area) <= 300:
                area_val = int(area)
                if area_val not in found_areas:
                    found_areas.add(area_val)
                    data['rooms']['dining'] = {
                        'name': 'DINING ROOM',
                        'area': area_val,
                        'type': 'dining'
                    }
                    self.logger.info(f"Found room: DINING ROOM = {area_val} sqft")
        
        # Look for utility room patterns
        utility_patterns = re.findall(r'(?:UTILITY\s+ROOM|UTILITY)\s+(\d+)', text, re.IGNORECASE)
        for area in utility_patterns:
            if area.isdigit() and 30 <= int(area) <= 120:
                area_val = int(area)
                if area_val not in found_areas:
                    found_areas.add(area_val)
                    data['rooms']['utility'] = {
                        'name': 'UTILITY ROOM',
                        'area': area_val,
                        'type': 'utility'
                    }
                    self.logger.info(f"Found room: UTILITY ROOM = {area_val} sqft")
    
    def _extract_dimensions(self, text: str, data: Dict):
        """Extract dimensional data with better patterns for construction plans"""
        # Wall dimension patterns (looking for wall lengths)
        wall_patterns = re.findall(r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)', text)
        for wall in wall_patterns:
            wall_length = self._convert_to_feet(wall)
            # Filter out unrealistic wall lengths (5-100 feet is reasonable)
            if 5 <= wall_length <= 100:
                data['wall_lengths'].append(wall_length)
        
        # Enhanced room dimension patterns for construction plans
        dimension_patterns = [
            # Standard "width x length" patterns
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*[xX×]\s*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            # Patterns with "=" sign (common in construction plans)
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*=\s*[xX×]\s*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            # Patterns with spaces around dimensions
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s+[xX×]\s+(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            # Simple number patterns (for basic dimensions)
            r'(\d+)\s*[xX×]\s*(\d+)',
            # Patterns with feet and inches
            r'(\d+[\'\"])\s*[xX×]\s*(\d+[\'\"]?)',
            # Patterns with mixed formats
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*[xX×]\s*(\d+)',
            r'(\d+)\s*[xX×]\s*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            # Look for patterns with quotes and inches
            r'(\d+[\'\"])\s*[xX×]\s*(\d+[\'\"]?)',
            # Look for patterns with just numbers and x
            r'(\d+)\s*[xX×]\s*(\d+)',
            # Look for patterns with feet-inches format
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*[xX×]\s*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)'
        ]
        
        total_matches = 0
        for i, pattern in enumerate(dimension_patterns):
            matches = re.findall(pattern, text, re.IGNORECASE)
            total_matches += len(matches)
            self.logger.debug(f"Pattern {i+1}: Found {len(matches)} matches")
            
            for match in matches:
                width, length = match
                width_ft = self._convert_to_feet(width)
                length_ft = self._convert_to_feet(length)
                # Only add reasonable room dimensions (5-50 feet is typical for rooms)
                if 5 <= width_ft <= 50 and 5 <= length_ft <= 50:
                    data['dimensions'].append({
                        'width': width,
                        'length': length,
                        'width_ft': width_ft,
                        'length_ft': length_ft,
                        'area_sqft': width_ft * length_ft
                    })
                    self.logger.info(f"Found dimension: {width} x {length} = {width_ft:.1f}' x {length_ft:.1f}' = {width_ft * length_ft:.0f} sqft")
        
        self.logger.info(f"Total dimension pattern matches found: {total_matches}")
        
        # Also look for standalone dimension strings that might be room dimensions
        # Look for patterns like "12' - 6" x 15' - 0"" or "12' x 15'"
        standalone_dimensions = re.findall(r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*[xX×]\s*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)', text)
        for dim in standalone_dimensions:
            width, length = dim
            width_ft = self._convert_to_feet(width)
            length_ft = self._convert_to_feet(length)
            if 5 <= width_ft <= 50 and 5 <= length_ft <= 50:
                # Check if we already have this dimension
                existing = any(
                    abs(d['width_ft'] - width_ft) < 0.1 and abs(d['length_ft'] - length_ft) < 0.1
                    for d in data['dimensions']
                )
                if not existing:
                    data['dimensions'].append({
                        'width': width,
                        'length': length,
                        'width_ft': width_ft,
                        'length_ft': length_ft,
                        'area_sqft': width_ft * length_ft
                    })
                    self.logger.info(f"Found standalone dimension: {width} x {length} = {width_ft:.1f}' x {length_ft:.1f}' = {width_ft * length_ft:.0f} sqft")
        
        # Look for any remaining dimension-like patterns in the text
        # This is a fallback to catch patterns we might have missed
        all_dimension_like = re.findall(r'(\d+[\'\"]?)\s*[xX×]\s*(\d+[\'\"]?)', text)
        self.logger.info(f"Found {len(all_dimension_like)} total dimension-like patterns in text")
        
        for dim in all_dimension_like:
            width, length = dim
            width_ft = self._convert_to_feet(width)
            length_ft = self._convert_to_feet(length)
            if 3 <= width_ft <= 60 and 3 <= length_ft <= 60:  # More lenient range
                # Check if we already have this dimension
                existing = any(
                    abs(d['width_ft'] - width_ft) < 0.1 and abs(d['length_ft'] - length_ft) < 0.1
                    for d in data['dimensions']
                )
                if not existing:
                    data['dimensions'].append({
                        'width': width,
                        'length': length,
                        'width_ft': width_ft,
                        'length_ft': length_ft,
                        'area_sqft': width_ft * length_ft
                    })
                    self.logger.info(f"Found fallback dimension: {width} x {length} = {width_ft:.1f}' x {length_ft:.1f}' = {width_ft * length_ft:.0f} sqft")
    
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
    
    def _extract_fixtures(self, text: str, data: Dict):
        """Extract fixture counts using sophisticated patterns from takeoff_estimator.py"""
        # Count doors and windows more accurately
        door_patterns = re.findall(r'(?:DOOR|door)(?!\s*(?:way|bell|knob|handle))', text)
        window_patterns = re.findall(r'(?:WINDOW|window|WIN)(?!\s*(?:sill|frame|trim))', text)
        
        # Also look for specific door/window callouts
        door_callouts = re.findall(r'(?:^|\s)(?:D\d+|DOOR\s*\d+)', text, re.MULTILINE)
        window_callouts = re.findall(r'(?:^|\s)(?:W\d+|WINDOW\s*\d+|WIN\s*\d+)', text, re.MULTILINE)
        
        door_count = max(len(door_patterns), len(door_callouts))
        window_count = max(len(window_patterns), len(window_callouts))
        
        data['fixtures']['doors'] += door_count
        data['fixtures']['windows'] += window_count
        
        # Count fixtures more accurately with CONSERVATIVE patterns
        # Electrical outlets - only count explicit outlet references, avoid voltage/dimension matches
        outlet_patterns = re.findall(r'(?:OUTLET|outlet|RECEP|recep|\bGFI\b|\bGFCI\b)', text)
        # Only count specific electrical callouts, not generic alphanumeric codes
        electrical_symbols = re.findall(r'(?:\bOUTLET\s+\d+\b|\bRECEP\s+\d+\b)', text)
        data['fixtures']['electrical_outlets'] += len(outlet_patterns) + len(electrical_symbols)
        
        # Light fixtures - be more conservative, avoid generic symbols
        light_patterns = re.findall(r'(?:LIGHT\s+FIXTURE|light\s+fixture|CEILING\s+FAN|ceiling\s+fan|PENDANT|pendant|CHANDELIER|chandelier|LIGHT\s+\d+)', text)
        # Only count specific lighting callouts
        lighting_symbols = re.findall(r'(?:\bLT\d+\b|\bLIGHT\s+\d+\b)', text)
        data['fixtures']['light_fixtures'] += len(light_patterns) + len(lighting_symbols)
        
        # Plumbing fixtures - be more conservative, avoid generic codes
        toilet_patterns = len(re.findall(r'(?:TOILET|toilet|WC|w\.c\.|WATER\s+CLOSET)', text))
        sink_patterns = len(re.findall(r'(?:SINK|sink|LAVATORY|lavatory|LAV|lav|BASIN)', text))
        shower_patterns = len(re.findall(r'(?:SHOWER|shower|TUB|tub|BATHTUB|bathtub|BATH\s+TUB)', text))
        # Only count specific plumbing callouts, not generic codes
        plumbing_symbols = len(re.findall(r'(?:\bPLUMBING\s+\d+\b|\bFIXTURE\s+\d+\b)', text))
        data['fixtures']['plumbing_fixtures'] += toilet_patterns + sink_patterns + shower_patterns + plumbing_symbols
        
        # Count switches - be more conservative
        switch_patterns = re.findall(r'(?:SWITCH|switch|TOGGLE|toggle)', text)
        switch_symbols = re.findall(r'(?:\bSW\d+\b|\bSWITCH\s+\d+\b)', text)
        data['fixtures']['switches'] += len(switch_patterns) + len(switch_symbols)
        
        # Count HVAC vents - be more conservative
        vent_patterns = re.findall(r'(?:SUPPLY\s+VENT|RETURN\s+VENT|AIR\s+VENT|HVAC\s+VENT)', text)
        vent_symbols = re.findall(r'(?:\bVENT\s+\d+\b|\bHVAC\s+\d+\b)', text)
        data['fixtures']['hvac_vents'] += len(vent_patterns) + len(vent_symbols)
    
    def _extract_ceiling_heights(self, text: str, data: Dict):
        """Extract ceiling height information with better filtering"""
        height_patterns = [
            r'CEILING\s+HEIGHT\s+(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            r'CLG\.?\s+HT\.?\s+(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*CEILING'
        ]
        
        for pattern in height_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Filter out phone numbers and invalid heights
                if self._is_valid_ceiling_height(match):
                    data['ceiling_heights'].append(match)
    
    def _is_valid_ceiling_height(self, height_str: str) -> bool:
        """Validate if a string represents a realistic ceiling height"""
        # Remove quotes and normalize
        clean_height = re.sub(r'[\'"]', '', height_str.strip())
        
        # Check if it looks like a phone number (contains 7+ digits)
        if len(re.findall(r'\d', clean_height)) >= 7:
            return False
        
        # Check if it contains common phone number patterns
        if re.search(r'\d{3}-\d{4}', clean_height) or re.search(r'\d{3}\d{4}', clean_height):
            return False
        
        # Extract numbers and check if they're in realistic ceiling height range
        numbers = re.findall(r'\d+', clean_height)
        if numbers:
            try:
                # Convert to feet and check if it's in realistic range (8-12 feet)
                feet = int(numbers[0])
                if len(numbers) > 1:
                    inches = int(numbers[1])
                    total_inches = feet * 12 + inches
                    total_feet = total_inches / 12
                else:
                    total_feet = feet
                
                # Realistic residential ceiling heights: 8-12 feet
                return 8 <= total_feet <= 12
            except (ValueError, IndexError):
                return False
        
        return False
    
    def _add_default_ceiling_heights(self, data: Dict):
        """Add default ceiling heights if none were extracted (CRITICAL for material estimates)"""
        if not data['ceiling_heights']:
            # Standard residential ceiling heights based on floor
            first_floor_height = 10.0  # 10 feet for first floor
            second_floor_height = 9.0  # 9 feet for second floor
            
            data['ceiling_heights'] = [f"{first_floor_height}' - 0\"", f"{second_floor_height}' - 0\""]
            self.logger.info(f"Added default ceiling heights: {first_floor_height}' (first floor), {second_floor_height}' (second floor)")
            
            # Also add to data for easy access
            data['default_ceiling_heights'] = {
                'first_floor': first_floor_height,
                'second_floor': second_floor_height
            }
            
            # Calculate wall areas using ceiling heights and wall lengths
            self._calculate_wall_areas(data)
    
    def _calculate_wall_areas(self, data: Dict):
        """Calculate wall areas using ceiling heights and wall lengths (CRITICAL for material estimates)"""
        if not data.get('wall_lengths') or not data.get('default_ceiling_heights'):
            return
            
        wall_lengths = data['wall_lengths']
        first_floor_height = data['default_ceiling_heights']['first_floor']
        second_floor_height = data['default_ceiling_heights']['second_floor']
        
        # Estimate wall distribution between floors
        # Assume 60% of walls are first floor, 40% are second floor
        first_floor_walls = wall_lengths[:int(len(wall_lengths) * 0.6)]
        second_floor_walls = wall_lengths[int(len(wall_lengths) * 0.6):]
        
        # Calculate wall areas
        first_floor_wall_area = sum(first_floor_walls) * first_floor_height
        second_floor_wall_area = sum(second_floor_walls) * second_floor_height
        total_wall_area = first_floor_wall_area + second_floor_wall_area
        
        data['wall_areas'] = {
            'first_floor_sqft': first_floor_wall_area,
            'second_floor_sqft': second_floor_wall_area,
            'total_sqft': total_wall_area
        }
        
        self.logger.info(f"Calculated wall areas: {total_wall_area:.0f} sqft total ({first_floor_wall_area:.0f} first floor, {second_floor_wall_area:.0f} second floor)")
    
    def _extract_structural_details(self, text: str, data: Dict):
        """Extract structural details including foundation, roof, beams, and joists"""
        self.logger.info("Extracting structural details...")
        
        # Extract foundation details
        self._extract_foundation_details(text, data)
        
        # Extract roof details
        self._extract_roof_details(text, data)
        
        # Extract beam and joist details
        self._extract_beam_joist_details(text, data)
        
        # Extract framing details
        self._extract_framing_details(text, data)
        
        # Log structural summary
        self._log_structural_summary(data)
    
    def _extract_foundation_details(self, text: str, data: Dict):
        """Extract foundation type, size, and specifications"""
        foundation = data['structural_details']['foundation']
        
        # Foundation type patterns
        foundation_types = [
            r'(?:SLAB\s+FOUNDATION|CONCRETE\s+SLAB|SLAB\s+ON\s+GRADE)',
            r'(?:CRAWL\s+SPACE|CRAWLSPACE)',
            r'(?:BASEMENT|FULL\s+BASEMENT)',
            r'(?:PIER\s+AND\s+BEAM|PIER\s+BEAM)',
            r'(?:STEM\s+WALL|FOUNDATION\s+WALL)'
        ]
        
        for pattern in foundation_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                foundation['type'] = matches[0].upper()
                foundation['details'].append(f"Foundation type: {matches[0]}")
                self.logger.info(f"Found foundation type: {matches[0]}")
                break
        
        # Foundation thickness patterns
        thickness_patterns = [
            r'FOUNDATION\s+THICKNESS[:\s]*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            r'SLAB\s+THICKNESS[:\s]*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*THICK\s+(?:FOUNDATION|SLAB)',
            r'(\d+[\'\"]?)\s*THICK\s+(?:FOUNDATION|SLAB)'
        ]
        
        for pattern in thickness_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                thickness = matches[0]
                foundation['thickness'] = thickness
                foundation['details'].append(f"Foundation thickness: {thickness}")
                self.logger.info(f"Found foundation thickness: {thickness}")
                break
        
        # Foundation area (if not already calculated, use total sqft)
        if not foundation['area_sqft'] and data['total_sqft']:
            foundation['area_sqft'] = data['total_sqft']
            foundation['details'].append(f"Foundation area: {data['total_sqft']} sqft")
    
    def _extract_roof_details(self, text: str, data: Dict):
        """Extract roof type, pitch, material, and area"""
        roof = data['structural_details']['roof']
        
        # Roof type patterns
        roof_types = [
            r'(?:GABLE\s+ROOF|GABLE)',
            r'(?:HIP\s+ROOF|HIP)',
            r'(?:SHED\s+ROOF|SHED)',
            r'(?:MANSARD\s+ROOF|MANSARD)',
            r'(?:GAMBREL\s+ROOF|GAMBREL)',
            r'(?:FLAT\s+ROOF|FLAT)'
        ]
        
        for pattern in roof_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                roof['type'] = matches[0].upper()
                roof['details'].append(f"Roof type: {matches[0]}")
                self.logger.info(f"Found roof type: {matches[0]}")
                break
        
        # Roof pitch patterns
        pitch_patterns = [
            r'ROOF\s+PITCH[:\s]*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            r'PITCH[:\s]*(\d+[\'\"]?\s*-\s*\d+[\'\"]?)',
            r'(\d+[\'\"]?\s*-\s*\d+[\'\"]?)\s*PITCH',
            r'(\d+:\d+)\s*PITCH',
            r'(\d+/\d+)\s*PITCH'
        ]
        
        for pattern in pitch_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                pitch = matches[0]
                roof['pitch'] = pitch
                roof['details'].append(f"Roof pitch: {pitch}")
                self.logger.info(f"Found roof pitch: {pitch}")
                break
        
        # Roof material patterns
        roof_materials = [
            r'(?:COMPOSITION\s+SHINGLES|COMP\s+SHINGLES|ASPHALT\s+SHINGLES)',
            r'(?:METAL\s+ROOFING|METAL\s+ROOF)',
            r'(?:TILE\s+ROOF|CLAY\s+TILE|CONCRETE\s+TILE)',
            r'(?:WOOD\s+SHINGLES|CEDAR\s+SHINGLES)',
            r'(?:SLATE\s+ROOF|SLATE)',
            r'(?:EPDM\s+ROOFING|RUBBER\s+ROOF)'
        ]
        
        for pattern in roof_materials:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                roof['material'] = matches[0].upper()
                roof['details'].append(f"Roof material: {matches[0]}")
                self.logger.info(f"Found roof material: {matches[0]}")
                break
        
        # Calculate roof area (typically 1.1-1.3x floor area for pitched roofs)
        if data['total_sqft'] and not roof['area_sqft']:
            # Estimate roof area based on pitch and overhangs
            roof_multiplier = 1.2  # Default for typical residential roof
            if roof['pitch']:
                # Higher pitch = more roof area
                if '12' in str(roof['pitch']) or '6' in str(roof['pitch']):
                    roof_multiplier = 1.3
                elif '4' in str(roof['pitch']) or '3' in str(roof['pitch']):
                    roof_multiplier = 1.15
            
            roof['area_sqft'] = int(data['total_sqft'] * roof_multiplier)
            roof['details'].append(f"Estimated roof area: {roof['area_sqft']} sqft")
    
    def _extract_beam_joist_details(self, text: str, data: Dict):
        """Extract beam sizes, joist specifications, and structural elements"""
        beams = data['structural_details']['beams']
        joists = data['structural_details']['joists']
        
        # Beam size patterns
        beam_patterns = [
            r'(\d+x\d+)\s*(?:BEAM|GLULAM|LVL)',
            r'(?:BEAM|GLULAM|LVL)\s*(\d+x\d+)',
            r'(\d+x\d+)\s*(?:HEADER|GIRDER)',
            r'(?:HEADER|GIRDER)\s*(\d+x\d+)',
            r'(\d+x\d+)\s*(?:LUMBER|WOOD)'
        ]
        
        for pattern in beam_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if match not in beams['sizes']:
                    beams['sizes'].append(match)
                    beams['details'].append(f"Beam size: {match}")
                    self.logger.info(f"Found beam size: {match}")
        
        # Joist size and spacing patterns
        joist_patterns = [
            r'(\d+x\d+)\s*(?:JOIST|FLOOR\s+JOIST)',
            r'(?:JOIST|FLOOR\s+JOIST)\s*(\d+x\d+)',
            r'(\d+[\'\"]?)\s*O\.?C\.?\s*(?:JOIST|SPACING)',
            r'(?:JOIST|SPACING)\s*(\d+[\'\"]?)\s*O\.?C\.?'
        ]
        
        for pattern in joist_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if 'x' in match:  # Size pattern
                    joists['size'] = match
                    joists['details'].append(f"Joist size: {match}")
                    self.logger.info(f"Found joist size: {match}")
                else:  # Spacing pattern
                    joists['spacing'] = match
                    joists['details'].append(f"Joist spacing: {match}")
                    self.logger.info(f"Found joist spacing: {match}")
        
        # Estimate joist count based on floor area and spacing
        if data['total_sqft'] and joists['spacing']:
            try:
                spacing = float(re.findall(r'\d+', joists['spacing'])[0])
                # Estimate joist count: floor width / spacing
                estimated_floor_width = (data['total_sqft'] ** 0.5) * 0.8  # Assume roughly square
                joist_count = int(estimated_floor_width / spacing)
                joists['count'] = joist_count
                joists['details'].append(f"Estimated joist count: {joist_count}")
            except (ValueError, IndexError):
                pass
    
    def _extract_framing_details(self, text: str, data: Dict):
        """Extract framing specifications including stud size and spacing"""
        framing = data['structural_details']['framing']
        
        # Stud size patterns
        stud_patterns = [
            r'(\d+x\d+)\s*(?:STUD|WALL\s+STUD)',
            r'(?:STUD|WALL\s+STUD)\s*(\d+x\d+)',
            r'(\d+x\d+)\s*(?:FRAMING|WALL\s+FRAMING)'
        ]
        
        for pattern in stud_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                framing['stud_size'] = matches[0]
                framing['details'].append(f"Stud size: {matches[0]}")
                self.logger.info(f"Found stud size: {matches[0]}")
                break
        
        # Stud spacing patterns
        spacing_patterns = [
            r'(\d+[\'\"]?)\s*O\.?C\.?\s*(?:STUD|SPACING)',
            r'(?:STUD|SPACING)\s*(\d+[\'\"]?)\s*O\.?C\.?',
            r'(\d+[\'\"]?)\s*ON\s+CENTER'
        ]
        
        for pattern in spacing_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                framing['stud_spacing'] = matches[0]
                framing['details'].append(f"Stud spacing: {matches[0]}")
                self.logger.info(f"Found stud spacing: {matches[0]}")
                break
        
        # Default values if not found
        if not framing['stud_size']:
            framing['stud_size'] = '2x4'
            framing['details'].append("Default stud size: 2x4")
        
        if not framing['stud_spacing']:
            framing['stud_spacing'] = '16"'
            framing['details'].append("Default stud spacing: 16\" O.C.")
    
    def _log_structural_summary(self, data: Dict):
        """Log a summary of extracted structural details"""
        structural = data['structural_details']
        
        self.logger.info("=== STRUCTURAL DETAILS SUMMARY ===")
        
        # Foundation summary
        if structural['foundation']['type']:
            self.logger.info(f"Foundation: {structural['foundation']['type']}")
            if structural['foundation']['thickness']:
                self.logger.info(f"  Thickness: {structural['foundation']['thickness']}")
            if structural['foundation']['area_sqft']:
                self.logger.info(f"  Area: {structural['foundation']['area_sqft']} sqft")
        
        # Roof summary
        if structural['roof']['type']:
            self.logger.info(f"Roof: {structural['roof']['type']}")
            if structural['roof']['pitch']:
                self.logger.info(f"  Pitch: {structural['roof']['pitch']}")
            if structural['roof']['material']:
                self.logger.info(f"  Material: {structural['roof']['material']}")
            if structural['roof']['area_sqft']:
                self.logger.info(f"  Area: {structural['roof']['area_sqft']} sqft")
        
        # Beams summary
        if structural['beams']['sizes']:
            self.logger.info(f"Beams: {', '.join(structural['beams']['sizes'])}")
        
        # Joists summary
        if structural['joists']['size']:
            self.logger.info(f"Joists: {structural['joists']['size']}")
            if structural['joists']['spacing']:
                self.logger.info(f"  Spacing: {structural['joists']['spacing']}")
            if structural['joists']['count']:
                self.logger.info(f"  Count: {structural['joists']['count']}")
        
        # Framing summary
        if structural['framing']['stud_size']:
            self.logger.info(f"Framing: {structural['framing']['stud_size']} studs")
            if structural['framing']['stud_spacing']:
                self.logger.info(f"  Spacing: {structural['framing']['stud_spacing']}")
        
        self.logger.info("=== END STRUCTURAL SUMMARY ===")
    
    def _extract_system_details(self, text: str, data: Dict):
        """Extract system details including HVAC, plumbing, and electrical"""
        self.logger.info("Extracting system details...")
        
        # Extract HVAC details
        self._extract_hvac_details(text, data)
        
        # Extract plumbing details
        self._extract_plumbing_details(text, data)
        
        # Extract electrical details
        self._extract_electrical_details(text, data)
        
        # Log system summary
        self._log_system_summary(data)
    
    def _extract_hvac_details(self, text: str, data: Dict):
        """Extract HVAC system details"""
        hvac = data['system_details']['hvac']
        
        # HVAC equipment type patterns
        hvac_types = [
            r'(?:HEAT\s+PUMP|HEAT\s+PUMP\s+SYSTEM)',
            r'(?:FURNACE|GAS\s+FURNACE|ELECTRIC\s+FURNACE)',
            r'(?:AIR\s+CONDITIONER|A/C|AC\s+UNIT)',
            r'(?:PACKAGE\s+UNIT|PACKAGED\s+UNIT)',
            r'(?:SPLIT\s+SYSTEM|SPLIT\s+UNIT)',
            r'(?:MINI\s+SPLIT|DUCTLESS)'
        ]
        
        for pattern in hvac_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                hvac['equipment']['type'] = matches[0].upper()
                hvac['equipment']['details'].append(f"HVAC type: {matches[0]}")
                self.logger.info(f"Found HVAC type: {matches[0]}")
                break
        
        # HVAC capacity patterns (tons, BTUs)
        capacity_patterns = [
            r'(\d+(?:\.\d+)?)\s*TON',
            r'(\d+)\s*BTU',
            r'(\d+)\s*MBH',
            r'(\d+(?:\.\d+)?)\s*TON\s*AC',
            r'(\d+)\s*BTU\s*COOLING'
        ]
        
        for pattern in capacity_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                capacity = matches[0]
                hvac['equipment']['capacity'] = capacity
                hvac['equipment']['details'].append(f"HVAC capacity: {capacity}")
                self.logger.info(f"Found HVAC capacity: {capacity}")
                break
        
        # HVAC efficiency patterns (SEER, AFUE, HSPF)
        efficiency_patterns = [
            r'SEER\s*(\d+(?:\.\d+)?)',
            r'AFUE\s*(\d+(?:\.\d+)?)',
            r'HSPF\s*(\d+(?:\.\d+)?)',
            r'(\d+(?:\.\d+)?)\s*SEER',
            r'(\d+(?:\.\d+)?)\s*AFUE'
        ]
        
        for pattern in efficiency_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                efficiency = matches[0]
                hvac['equipment']['efficiency'] = efficiency
                hvac['equipment']['details'].append(f"HVAC efficiency: {efficiency}")
                self.logger.info(f"Found HVAC efficiency: {efficiency}")
                break
        
        # Ductwork material patterns
        duct_materials = [
            r'(?:GALVANIZED\s+STEEL|GALV\s+STEEL)',
            r'(?:FLEXIBLE\s+DUCT|FLEX\s+DUCT)',
            r'(?:FIBERGLASS\s+DUCT|FIBERGLASS)',
            r'(?:SHEET\s+METAL|METAL\s+DUCT)',
            r'(?:RIGID\s+DUCT|RIGID)'
        ]
        
        for pattern in duct_materials:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                hvac['ductwork']['material'] = matches[0].upper()
                hvac['ductwork']['details'].append(f"Ductwork material: {matches[0]}")
                self.logger.info(f"Found ductwork material: {matches[0]}")
                break
        
        # Ductwork sizes
        duct_sizes = re.findall(r'(\d+[\'\"]?)\s*(?:X|x)\s*(\d+[\'\"]?)\s*(?:DUCT|DIA)', text, re.IGNORECASE)
        for size in duct_sizes:
            size_str = f"{size[0]} x {size[1]}"
            if size_str not in hvac['ductwork']['sizes']:
                hvac['ductwork']['sizes'].append(size_str)
                hvac['ductwork']['details'].append(f"Ductwork size: {size_str}")
                self.logger.info(f"Found ductwork size: {size_str}")
        
        # Estimate ductwork linear feet based on house size
        if data['total_sqft'] and not hvac['ductwork']['linear_feet']:
            # Typical residential: 1-2 linear feet per sqft
            estimated_ductwork = int(data['total_sqft'] * 1.5)
            hvac['ductwork']['linear_feet'] = estimated_ductwork
            hvac['ductwork']['details'].append(f"Estimated ductwork: {estimated_ductwork} linear feet")
        
        # Exhaust fans
        exhaust_fan_patterns = [
            r'(\d+)\s*(?:EXHAUST\s+FAN|BATH\s+FAN|VENT\s+FAN)',
            r'(?:EXHAUST\s+FAN|BATH\s+FAN|VENT\s+FAN).*?(\d+)',
            r'(\d+)\s*CFM\s*(?:EXHAUST|FAN)'
        ]
        
        for pattern in exhaust_fan_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                try:
                    fan_count = int(matches[0])
                    hvac['ventilation']['exhaust_fans'] = fan_count
                    hvac['ventilation']['details'].append(f"Exhaust fans: {fan_count}")
                    self.logger.info(f"Found exhaust fans: {fan_count}")
                    break
                except ValueError:
                    continue
    
    def _extract_plumbing_details(self, text: str, data: Dict):
        """Extract plumbing system details"""
        plumbing = data['system_details']['plumbing']
        
        # Water heater type patterns
        water_heater_types = [
            r'(?:ELECTRIC\s+WATER\s+HEATER|ELECTRIC\s+WH)',
            r'(?:GAS\s+WATER\s+HEATER|GAS\s+WH)',
            r'(?:TANKLESS\s+WATER\s+HEATER|TANKLESS)',
            r'(?:HEAT\s+PUMP\s+WATER\s+HEATER|HPWH)',
            r'(?:SOLAR\s+WATER\s+HEATER|SOLAR\s+WH)'
        ]
        
        for pattern in water_heater_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                plumbing['water_heater']['type'] = matches[0].upper()
                plumbing['water_heater']['details'].append(f"Water heater type: {matches[0]}")
                self.logger.info(f"Found water heater type: {matches[0]}")
                break
        
        # Water heater capacity patterns
        capacity_patterns = [
            r'(\d+)\s*GAL\s*(?:WATER\s+HEATER|WH)',
            r'(\d+)\s*GALLON\s*(?:WATER\s+HEATER|WH)',
            r'(\d+)\s*GAL',
            r'(\d+)\s*GALLON'
        ]
        
        for pattern in capacity_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                capacity = matches[0]
                plumbing['water_heater']['capacity'] = capacity
                plumbing['water_heater']['details'].append(f"Water heater capacity: {capacity} gallons")
                self.logger.info(f"Found water heater capacity: {capacity} gallons")
                break
        
        # Water heater fuel type
        fuel_types = [
            r'(?:NATURAL\s+GAS|GAS)',
            r'(?:ELECTRIC|ELECTRICAL)',
            r'(?:PROPANE|LP\s+GAS)',
            r'(?:SOLAR|SOLAR\s+ASSISTED)'
        ]
        
        for pattern in fuel_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                plumbing['water_heater']['fuel_type'] = matches[0].upper()
                plumbing['water_heater']['details'].append(f"Water heater fuel: {matches[0]}")
                self.logger.info(f"Found water heater fuel: {matches[0]}")
                break
        
        # Pipe material patterns
        pipe_materials = [
            r'(?:COPPER\s+PIPE|COPPER)',
            r'(?:PEX\s+PIPE|PEX)',
            r'(?:CPVC\s+PIPE|CPVC)',
            r'(?:PVC\s+PIPE|PVC)',
            r'(?:GALVANIZED\s+PIPE|GALV\s+PIPE)',
            r'(?:CAST\s+IRON\s+PIPE|CAST\s+IRON)'
        ]
        
        for pattern in pipe_materials:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                material = matches[0].upper()
                if 'COPPER' in material:
                    plumbing['pipes']['water_supply'] = material
                elif 'PVC' in material or 'CAST IRON' in material:
                    plumbing['pipes']['drain_waste_vent'] = material
                plumbing['pipes']['details'].append(f"Pipe material: {material}")
                self.logger.info(f"Found pipe material: {material}")
        
        # Pipe sizes
        pipe_sizes = re.findall(r'(\d+(?:/\d+)?)\s*(?:INCH|IN|[\'\"])\s*(?:PIPE|DIA)', text, re.IGNORECASE)
        for size in pipe_sizes:
            if size not in plumbing['pipes']['sizes']:
                plumbing['pipes']['sizes'].append(size)
                plumbing['pipes']['details'].append(f"Pipe size: {size}\"")
                self.logger.info(f"Found pipe size: {size}\"")
        
        # Plumbing fixtures (more detailed than basic fixture count)
        fixture_patterns = [
            r'(\d+)\s*(?:TOILET|WC|W\.C\.)',
            r'(\d+)\s*(?:SINK|LAVATORY|LAV)',
            r'(\d+)\s*(?:TUB|BATHTUB)',
            r'(\d+)\s*(?:SHOWER|SHOWER\s+STALL)',
            r'(\d+)\s*(?:FAUCET|FAUCETS)'
        ]
        
        for pattern in fixture_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                count = int(matches[0])
                if 'TOILET' in pattern or 'WC' in pattern:
                    plumbing['fixtures']['toilets'] = count
                elif 'SINK' in pattern or 'LAV' in pattern:
                    plumbing['fixtures']['sinks'] = count
                elif 'TUB' in pattern:
                    plumbing['fixtures']['tubs'] = count
                elif 'SHOWER' in pattern:
                    plumbing['fixtures']['showers'] = count
                plumbing['fixtures']['details'].append(f"Found {count} fixtures")
                self.logger.info(f"Found {count} fixtures")
    
    def _extract_electrical_details(self, text: str, data: Dict):
        """Extract electrical system details"""
        electrical = data['system_details']['electrical']
        
        # Main panel size patterns
        panel_patterns = [
            r'(\d+)\s*AMP\s*(?:MAIN\s+)?(?:PANEL|SERVICE)',
            r'(\d+)\s*A\s*(?:MAIN\s+)?(?:PANEL|SERVICE)',
            r'(\d+)\s*AMP\s*ELECTRICAL',
            r'(\d+)\s*A\s*ELECTRICAL'
        ]
        
        for pattern in panel_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                amperage = matches[0]
                electrical['main_panel']['amperage'] = amperage
                electrical['main_panel']['details'].append(f"Main panel: {amperage} amp")
                self.logger.info(f"Found main panel: {amperage} amp")
                break
        
        # Panel size (spaces/circuits)
        panel_size_patterns = [
            r'(\d+)\s*(?:SPACE|CIRCUIT)\s*(?:PANEL|BOX)',
            r'(\d+)\s*(?:SPACE|CIRCUIT)',
            r'(\d+)\s*SLOT\s*(?:PANEL|BOX)'
        ]
        
        for pattern in panel_size_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                size = matches[0]
                electrical['main_panel']['size'] = size
                electrical['main_panel']['details'].append(f"Panel size: {size} spaces")
                self.logger.info(f"Found panel size: {size} spaces")
                break
        
        # Wire gauge patterns
        wire_gauge_patterns = [
            r'(\d+)\s*AWG\s*(?:WIRE|CABLE)',
            r'(\d+)\s*GAUGE\s*(?:WIRE|CABLE)',
            r'(\d+)\s*GA\s*(?:WIRE|CABLE)',
            r'#(\d+)\s*(?:WIRE|CABLE)'
        ]
        
        for pattern in wire_gauge_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                gauge = matches[0]
                electrical['wire']['gauge'] = gauge
                electrical['wire']['details'].append(f"Wire gauge: {gauge} AWG")
                self.logger.info(f"Found wire gauge: {gauge} AWG")
                break
        
        # Wire type patterns
        wire_types = [
            r'(?:THWN\s+WIRE|THWN)',
            r'(?:THHN\s+WIRE|THHN)',
            r'(?:NM\s+CABLE|ROMEX)',
            r'(?:UF\s+CABLE|UF)',
            r'(?:COPPER\s+WIRE|COPPER)',
            r'(?:ALUMINUM\s+WIRE|ALUMINUM)'
        ]
        
        for pattern in wire_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                wire_type = matches[0].upper()
                electrical['wire']['type'] = wire_type
                electrical['wire']['details'].append(f"Wire type: {wire_type}")
                self.logger.info(f"Found wire type: {wire_type}")
                break
        
        # Estimate wire linear feet based on house size
        if data['total_sqft'] and not electrical['wire']['linear_feet']:
            # Typical residential: 1.5-2 linear feet per sqft
            estimated_wire = int(data['total_sqft'] * 1.5)
            electrical['wire']['linear_feet'] = estimated_wire
            electrical['wire']['details'].append(f"Estimated wire: {estimated_wire} linear feet")
        
        # Outlet types (more detailed than basic fixture count)
        outlet_patterns = [
            r'(\d+)\s*(?:GFCI\s+OUTLET|GFCI)',
            r'(\d+)\s*(?:DEDICATED\s+OUTLET|DEDICATED)',
            r'(\d+)\s*(?:STANDARD\s+OUTLET|STANDARD)',
            r'(\d+)\s*(?:RECEPTACLE|RECEPTACLES)'
        ]
        
        for pattern in outlet_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                count = int(matches[0])
                if 'GFCI' in pattern:
                    electrical['outlets']['gfci'] = count
                elif 'DEDICATED' in pattern:
                    electrical['outlets']['dedicated'] = count
                else:
                    electrical['outlets']['standard'] = count
                electrical['outlets']['details'].append(f"Found {count} outlets")
                self.logger.info(f"Found {count} outlets")
    
    def _extract_material_specifications(self, text: str, data: Dict):
        """Extract material specifications including insulation, siding, flooring, and finishes"""
        self.logger.info("Extracting material specifications...")
        
        # Extract insulation details
        self._extract_insulation_details(text, data)
        
        # Extract siding details
        self._extract_siding_details(text, data)
        
        # Extract flooring details
        self._extract_flooring_details(text, data)
        
        # Extract interior finish details
        self._extract_interior_finish_details(text, data)
        
        # Log material summary
        self._log_material_summary(data)
    
    def _extract_insulation_details(self, text: str, data: Dict):
        """Extract insulation specifications"""
        insulation = data['material_specifications']['insulation']
        
        # R-value patterns
        r_value_patterns = [
            r'R-(\d+(?:\.\d+)?)\s*(?:WALL|WALLS)',
            r'R(\d+(?:\.\d+)?)\s*(?:WALL|WALLS)',
            r'(\d+(?:\.\d+)?)\s*R-VALUE\s*(?:WALL|WALLS)',
            r'WALL\s*R-(\d+(?:\.\d+)?)',
            r'WALL\s*R(\d+(?:\.\d+)?)'
        ]
        
        for pattern in r_value_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                r_value = matches[0]
                insulation['wall_r_value'] = r_value
                insulation['details'].append(f"Wall R-value: R-{r_value}")
                self.logger.info(f"Found wall R-value: R-{r_value}")
                break
        
        # Ceiling R-value patterns
        ceiling_r_patterns = [
            r'R-(\d+(?:\.\d+)?)\s*(?:CEILING|CEILINGS|ATTIC)',
            r'R(\d+(?:\.\d+)?)\s*(?:CEILING|CEILINGS|ATTIC)',
            r'(\d+(?:\.\d+)?)\s*R-VALUE\s*(?:CEILING|CEILINGS|ATTIC)',
            r'CEILING\s*R-(\d+(?:\.\d+)?)',
            r'ATTIC\s*R-(\d+(?:\.\d+)?)'
        ]
        
        for pattern in ceiling_r_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                r_value = matches[0]
                insulation['ceiling_r_value'] = r_value
                insulation['details'].append(f"Ceiling R-value: R-{r_value}")
                self.logger.info(f"Found ceiling R-value: R-{r_value}")
                break
        
        # Insulation type patterns
        insulation_types = [
            r'(?:FIBERGLASS\s+BATT|FIBERGLASS)',
            r'(?:CELLULOSE\s+INSULATION|CELLULOSE)',
            r'(?:SPRAY\s+FOAM|SPRAY\s+INSULATION)',
            r'(?:BLOWN\s+IN\s+INSULATION|BLOWN\s+IN)',
            r'(?:RIGID\s+FOAM|RIGID\s+INSULATION)',
            r'(?:MINERAL\s+WOOL|ROCK\s+WOOL)'
        ]
        
        for pattern in insulation_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                insulation['type'] = matches[0].upper()
                insulation['details'].append(f"Insulation type: {matches[0]}")
                self.logger.info(f"Found insulation type: {matches[0]}")
                break
    
    def _extract_siding_details(self, text: str, data: Dict):
        """Extract siding specifications"""
        siding = data['material_specifications']['siding']
        
        # Siding type patterns
        siding_types = [
            r'(?:HARDIE\s+PLANK|HARDIE\s+SIDING)',
            r'(?:VINYL\s+SIDING|VINYL)',
            r'(?:WOOD\s+SIDING|WOOD)',
            r'(?:FIBER\s+CEMENT|FIBER\s+CEMENT\s+SIDING)',
            r'(?:BRICK\s+VENeer|BRICK)',
            r'(?:STONE\s+VENeer|STONE)',
            r'(?:STUCCO|STUCCO\s+FINISH)',
            r'(?:BOARD\s+AND\s+BATTEN|BOARD\s+BATTEN)'
        ]
        
        for pattern in siding_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                siding['type'] = matches[0].upper()
                siding['details'].append(f"Siding type: {matches[0]}")
                self.logger.info(f"Found siding type: {matches[0]}")
                break
        
        # Estimate siding area based on wall area
        if data.get('wall_areas', {}).get('total_sqft') and not siding['area_sqft']:
            # Siding typically covers exterior walls (roughly 60-70% of total wall area)
            wall_area = data['wall_areas']['total_sqft']
            siding_area = int(wall_area * 0.65)  # 65% of wall area
            siding['area_sqft'] = siding_area
            siding['details'].append(f"Estimated siding area: {siding_area} sqft")
    
    def _extract_flooring_details(self, text: str, data: Dict):
        """Extract flooring specifications"""
        flooring = data['material_specifications']['flooring']
        
        # Flooring type patterns
        flooring_types = [
            r'(?:HARDWOOD\s+FLOORING|HARDWOOD)',
            r'(?:LAMINATE\s+FLOORING|LAMINATE)',
            r'(?:TILE\s+FLOORING|CERAMIC\s+TILE)',
            r'(?:CARPET|CARPETING)',
            r'(?:VINYL\s+FLOORING|VINYL)',
            r'(?:LUXURY\s+VINYL\s+PLANK|LVP)',
            r'(?:ENGINEERED\s+HARDWOOD|ENGINEERED)',
            r'(?:BAMBOO\s+FLOORING|BAMBOO)'
        ]
        
        for pattern in flooring_types:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if match not in flooring['types']:
                    flooring['types'].append(match.upper())
                    flooring['details'].append(f"Flooring type: {match}")
                    self.logger.info(f"Found flooring type: {match}")
        
        # Estimate flooring areas by room type if not specified
        if data['total_sqft'] and not flooring['areas']:
            total_sqft = data['total_sqft']
            # Rough estimates by room type
            flooring['areas'] = {
                'living_areas': int(total_sqft * 0.6),  # 60% living areas
                'bedrooms': int(total_sqft * 0.25),     # 25% bedrooms
                'bathrooms': int(total_sqft * 0.08),    # 8% bathrooms
                'kitchen': int(total_sqft * 0.07)       # 7% kitchen
            }
            flooring['details'].append(f"Estimated flooring areas by room type")
    
    def _extract_interior_finish_details(self, text: str, data: Dict):
        """Extract interior finish specifications"""
        finishes = data['material_specifications']['interior_finishes']
        
        # Drywall thickness patterns
        drywall_thickness_patterns = [
            r'(\d+(?:/\d+)?)\s*(?:INCH|IN|[\'\"])\s*(?:DRYWALL|GYPSUM|SHEETROCK)',
            r'(\d+(?:/\d+)?)\s*(?:INCH|IN|[\'\"])\s*(?:GYP|GYP\s+BOARD)',
            r'(\d+(?:/\d+)?)\s*(?:INCH|IN|[\'\"])\s*(?:WALL\s+BOARD|WALLBOARD)'
        ]
        
        for pattern in drywall_thickness_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                thickness = matches[0]
                finishes['drywall']['thickness'] = thickness
                finishes['drywall']['details'].append(f"Drywall thickness: {thickness}\"")
                self.logger.info(f"Found drywall thickness: {thickness}\"")
                break
        
        # Estimate drywall area based on wall area
        if data.get('wall_areas', {}).get('total_sqft') and not finishes['drywall']['area_sqft']:
            wall_area = data['wall_areas']['total_sqft']
            # Drywall covers both sides of walls + ceilings
            drywall_area = int(wall_area * 2.2)  # Both sides + some ceiling
            finishes['drywall']['area_sqft'] = drywall_area
            finishes['drywall']['details'].append(f"Estimated drywall area: {drywall_area} sqft")
        
        # Paint specifications
        paint_patterns = [
            r'(?:LATEX\s+PAINT|LATEX)',
            r'(?:OIL\s+BASED\s+PAINT|OIL\s+BASED)',
            r'(?:EGG\s+SHELL\s+FINISH|EGG\s+SHELL)',
            r'(?:SEMI\s+GLOSS|SEMI\s+GLOSS\s+FINISH)',
            r'(?:FLAT\s+FINISH|FLAT)',
            r'(?:SATIN\s+FINISH|SATIN)'
        ]
        
        for pattern in paint_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                paint_type = matches[0].upper()
                if 'LATEX' in paint_type or 'OIL' in paint_type:
                    finishes['paint']['finish'] = paint_type
                else:
                    finishes['paint']['primer'] = paint_type
                finishes['paint']['details'].append(f"Paint type: {paint_type}")
                self.logger.info(f"Found paint type: {paint_type}")
        
        # Estimate paint area (same as drywall area)
        if finishes['drywall']['area_sqft'] and not finishes['paint']['area_sqft']:
            finishes['paint']['area_sqft'] = finishes['drywall']['area_sqft']
            finishes['paint']['details'].append(f"Estimated paint area: {finishes['drywall']['area_sqft']} sqft")
        
        # Trim specifications
        trim_patterns = [
            r'(\d+(?:\.\d+)?)\s*(?:INCH|IN|[\'\"])\s*(?:BASEBOARD|BASE)',
            r'(\d+(?:\.\d+)?)\s*(?:INCH|IN|[\'\"])\s*(?:CROWN\s+MOLDING|CROWN)',
            r'(\d+(?:\.\d+)?)\s*(?:INCH|IN|[\'\"])\s*(?:CASING|DOOR\s+CASING)'
        ]
        
        for pattern in trim_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                size = matches[0]
                if 'BASEBOARD' in pattern or 'BASE' in pattern:
                    finishes['trim']['baseboard'] = size
                elif 'CROWN' in pattern:
                    finishes['trim']['crown_molding'] = size
                elif 'CASING' in pattern:
                    finishes['trim']['casing'] = size
                finishes['trim']['details'].append(f"Trim size: {size}\"")
                self.logger.info(f"Found trim size: {size}\"")
    
    def _log_system_summary(self, data: Dict):
        """Log a summary of extracted system details"""
        systems = data['system_details']
        
        self.logger.info("=== SYSTEM DETAILS SUMMARY ===")
        
        # HVAC summary
        if systems['hvac']['equipment']['type']:
            self.logger.info(f"HVAC: {systems['hvac']['equipment']['type']}")
            if systems['hvac']['equipment']['capacity']:
                self.logger.info(f"  Capacity: {systems['hvac']['equipment']['capacity']}")
            if systems['hvac']['equipment']['efficiency']:
                self.logger.info(f"  Efficiency: {systems['hvac']['equipment']['efficiency']}")
            if systems['hvac']['ductwork']['linear_feet']:
                self.logger.info(f"  Ductwork: {systems['hvac']['ductwork']['linear_feet']} linear feet")
        
        # Plumbing summary
        if systems['plumbing']['water_heater']['type']:
            self.logger.info(f"Water Heater: {systems['plumbing']['water_heater']['type']}")
            if systems['plumbing']['water_heater']['capacity']:
                self.logger.info(f"  Capacity: {systems['plumbing']['water_heater']['capacity']} gallons")
            if systems['plumbing']['water_heater']['fuel_type']:
                self.logger.info(f"  Fuel: {systems['plumbing']['water_heater']['fuel_type']}")
        
        # Electrical summary
        if systems['electrical']['main_panel']['amperage']:
            self.logger.info(f"Electrical Panel: {systems['electrical']['main_panel']['amperage']} amp")
            if systems['electrical']['main_panel']['size']:
                self.logger.info(f"  Size: {systems['electrical']['main_panel']['size']} spaces")
            if systems['electrical']['wire']['linear_feet']:
                self.logger.info(f"  Wire: {systems['electrical']['wire']['linear_feet']} linear feet")
        
        self.logger.info("=== END SYSTEM SUMMARY ===")
    
    def _log_material_summary(self, data: Dict):
        """Log a summary of extracted material specifications"""
        materials = data['material_specifications']
        
        self.logger.info("=== MATERIAL SPECIFICATIONS SUMMARY ===")
        
        # Insulation summary
        if materials['insulation']['wall_r_value']:
            self.logger.info(f"Wall Insulation: R-{materials['insulation']['wall_r_value']}")
        if materials['insulation']['ceiling_r_value']:
            self.logger.info(f"Ceiling Insulation: R-{materials['insulation']['ceiling_r_value']}")
        if materials['insulation']['type']:
            self.logger.info(f"Insulation Type: {materials['insulation']['type']}")
        
        # Siding summary
        if materials['siding']['type']:
            self.logger.info(f"Siding: {materials['siding']['type']}")
            if materials['siding']['area_sqft']:
                self.logger.info(f"  Area: {materials['siding']['area_sqft']} sqft")
        
        # Flooring summary
        if materials['flooring']['types']:
            self.logger.info(f"Flooring Types: {', '.join(materials['flooring']['types'])}")
        
        # Interior finishes summary
        if materials['interior_finishes']['drywall']['area_sqft']:
            self.logger.info(f"Drywall: {materials['interior_finishes']['drywall']['area_sqft']} sqft")
        if materials['interior_finishes']['paint']['area_sqft']:
            self.logger.info(f"Paint: {materials['interior_finishes']['paint']['area_sqft']} sqft")
        
        self.logger.info("=== END MATERIAL SUMMARY ===")
    
    def _classify_room_type(self, room_name: str) -> str:
        """Classify room type based on name"""
        room_name_upper = room_name.upper()
        
        if any(word in room_name_upper for word in ['BEDROOM', 'BED', 'BR']):
            return 'bedroom'
        elif any(word in room_name_upper for word in ['BATH', 'BATHROOM', 'POWDER']):
            return 'bathroom'
        elif any(word in room_name_upper for word in ['KITCHEN', 'KIT']):
            return 'kitchen'
        elif any(word in room_name_upper for word in ['LIVING', 'LIV']):
            return 'living'
        elif any(word in room_name_upper for word in ['DINING', 'DIN']):
            return 'dining'
        elif any(word in room_name_upper for word in ['GARAGE', 'GAR']):
            return 'garage'
        elif any(word in room_name_upper for word in ['FAMILY', 'FAM']):
            return 'family'
        else:
            return 'other'
    
    def save_results(self, data: Dict, output_file: str = None):
        """Save extraction results to JSON file with timestamp"""
        if output_file is None:
            # Save results to JSON file in output directory with timestamp
            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = output_dir / f"tesseract_extraction_{self.pdf_path.stem}_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        with self.lock:
            self.logger.info(f"Results saved to: {output_file}")
        return output_file
    
    def print_summary(self, data: Dict):
        """Print a summary of extracted data"""
        print("\n" + "="*60)
        print("TESSERACT OCR EXTRACTION SUMMARY")
        print("="*60)
        print(f"PDF File: {self.pdf_path.name}")
        print(f"Total Square Footage: {data['total_sqft']} sqft")
        
        # Count total rooms from room_details
        total_rooms = 0
        for room_type, room_data in data['rooms'].items():
            if isinstance(room_data, list):
                total_rooms += len(room_data)
            else:
                total_rooms += 1
        
        print(f"Number of Rooms: {total_rooms}")
        print(f"Floor Areas: {data['floor_areas']}")
        print(f"Fixtures: {data['fixtures']}")
        print(f"Ceiling Heights: {data['ceiling_heights']}")
        print(f"Dimensions Found: {len(data['dimensions'])}")
        print(f"Wall Lengths: {len(data['wall_lengths'])} walls")
        
        # Show wall areas if calculated
        if 'wall_areas' in data:
            wall_areas = data['wall_areas']
            print(f"Wall Areas: {wall_areas['total_sqft']:.0f} sqft total ({wall_areas['first_floor_sqft']:.0f} first floor, {wall_areas['second_floor_sqft']:.0f} second floor)")
        
        # Show structural details if available
        if 'structural_details' in data:
            structural = data['structural_details']
            print(f"\nStructural Details:")
            
            # Foundation
            if structural['foundation']['type']:
                print(f"  Foundation: {structural['foundation']['type']}")
                if structural['foundation']['thickness']:
                    print(f"    Thickness: {structural['foundation']['thickness']}")
                if structural['foundation']['area_sqft']:
                    print(f"    Area: {structural['foundation']['area_sqft']} sqft")
            
            # Roof
            if structural['roof']['type']:
                print(f"  Roof: {structural['roof']['type']}")
                if structural['roof']['pitch']:
                    print(f"    Pitch: {structural['roof']['pitch']}")
                if structural['roof']['material']:
                    print(f"    Material: {structural['roof']['material']}")
                if structural['roof']['area_sqft']:
                    print(f"    Area: {structural['roof']['area_sqft']} sqft")
            
            # Beams
            if structural['beams']['sizes']:
                print(f"  Beams: {', '.join(structural['beams']['sizes'])}")
            
            # Joists
            if structural['joists']['size']:
                print(f"  Joists: {structural['joists']['size']}")
                if structural['joists']['spacing']:
                    print(f"    Spacing: {structural['joists']['spacing']}")
                if structural['joists']['count']:
                    print(f"    Count: {structural['joists']['count']}")
            
            # Framing
            if structural['framing']['stud_size']:
                print(f"  Framing: {structural['framing']['stud_size']} studs @ {structural['framing']['stud_spacing']}")
        
        # Show system details if available
        if 'system_details' in data:
            systems = data['system_details']
            print(f"\nSystem Details:")
            
            # HVAC
            if systems['hvac']['equipment']['type']:
                print(f"  HVAC: {systems['hvac']['equipment']['type']}")
                if systems['hvac']['equipment']['capacity']:
                    print(f"    Capacity: {systems['hvac']['equipment']['capacity']}")
                if systems['hvac']['equipment']['efficiency']:
                    print(f"    Efficiency: {systems['hvac']['equipment']['efficiency']}")
                if systems['hvac']['ductwork']['linear_feet']:
                    print(f"    Ductwork: {systems['hvac']['ductwork']['linear_feet']} linear feet")
            
            # Plumbing
            if systems['plumbing']['water_heater']['type']:
                print(f"  Water Heater: {systems['plumbing']['water_heater']['type']}")
                if systems['plumbing']['water_heater']['capacity']:
                    print(f"    Capacity: {systems['plumbing']['water_heater']['capacity']} gallons")
                if systems['plumbing']['water_heater']['fuel_type']:
                    print(f"    Fuel: {systems['plumbing']['water_heater']['fuel_type']}")
            
            # Electrical
            if systems['electrical']['main_panel']['amperage']:
                print(f"  Electrical Panel: {systems['electrical']['main_panel']['amperage']} amp")
                if systems['electrical']['main_panel']['size']:
                    print(f"    Size: {systems['electrical']['main_panel']['size']} spaces")
                if systems['electrical']['wire']['linear_feet']:
                    print(f"    Wire: {systems['electrical']['wire']['linear_feet']} linear feet")
        
        # Show material specifications if available
        if 'material_specifications' in data:
            materials = data['material_specifications']
            print(f"\nMaterial Specifications:")
            
            # Insulation
            if materials['insulation']['wall_r_value'] or materials['insulation']['ceiling_r_value']:
                print(f"  Insulation:")
                if materials['insulation']['wall_r_value']:
                    print(f"    Wall: R-{materials['insulation']['wall_r_value']}")
                if materials['insulation']['ceiling_r_value']:
                    print(f"    Ceiling: R-{materials['insulation']['ceiling_r_value']}")
                if materials['insulation']['type']:
                    print(f"    Type: {materials['insulation']['type']}")
            
            # Siding
            if materials['siding']['type']:
                print(f"  Siding: {materials['siding']['type']}")
                if materials['siding']['area_sqft']:
                    print(f"    Area: {materials['siding']['area_sqft']} sqft")
            
            # Flooring
            if materials['flooring']['types']:
                print(f"  Flooring: {', '.join(materials['flooring']['types'])}")
            
            # Interior finishes
            if materials['interior_finishes']['drywall']['area_sqft'] or materials['interior_finishes']['paint']['area_sqft']:
                print(f"  Interior Finishes:")
                if materials['interior_finishes']['drywall']['area_sqft']:
                    print(f"    Drywall: {materials['interior_finishes']['drywall']['area_sqft']} sqft")
                if materials['interior_finishes']['paint']['area_sqft']:
                    print(f"    Paint: {materials['interior_finishes']['paint']['area_sqft']} sqft")
        
        if data['rooms']:
            print("\nRoom Details:")
            for room_type, room_data in data['rooms'].items():
                if isinstance(room_data, list):
                    for i, room in enumerate(room_data):
                        room_name = room['name']
                        # Clean up room names to avoid duplicates like "BEDROOM 1 1"
                        if room_name.endswith(f" {i+1}"):
                            room_name = room_name.replace(f" {i+1}", "")
                        print(f"  {room_name} {i+1}: {room['area']} sqft ({room['type']})")
                else:
                    print(f"  {room_data['name']}: {room_data['area']} sqft ({room_data['type']})")
        
        print("="*60)

def process_single_pdf(pdf_path: str) -> Dict:
    """Process a single PDF and return results"""
    try:
        print(f"\n🚀 Starting extraction for: {Path(pdf_path).name}")
        start_time = time.time()
        
        # Initialize extractor
        extractor = TesseractTakeoffExtractor(pdf_path)
        
        # Extract construction data
        construction_data = extractor.extract_construction_data()
        
        # Print summary
        extractor.print_summary(construction_data)
        
        # Save results
        output_file = extractor.save_results(construction_data)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        result = {
            'pdf_path': pdf_path,
            'output_file': str(output_file),
            'success': True,
            'processing_time': processing_time,
            'data': construction_data
        }
        
        print(f"\n✅ Completed {Path(pdf_path).name} in {processing_time:.1f} seconds")
        print(f"📄 Results saved to: {output_file}")
        
        return result
        
    except Exception as e:
        error_msg = f"❌ Error processing {Path(pdf_path).name}: {e}"
        print(error_msg)
        logging.error(f"Processing failed for {pdf_path}: {e}", exc_info=True)
        
        return {
            'pdf_path': pdf_path,
            'output_file': None,
            'success': False,
            'error': str(e),
            'processing_time': 0
        }

def process_all_pdfs():
    """Process all PDFs in the plans directory in parallel"""
    plans_dir = Path("plans")
    
    if not plans_dir.exists():
        print(f"Error: Plans directory not found: {plans_dir}")
        return False
    
    # Find all PDF files in the plans directory
    pdf_files = list(plans_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {plans_dir}")
        return False
    
    print(f"📁 Found {len(pdf_files)} PDF files to process:")
    for pdf_file in pdf_files:
        print(f"  - {pdf_file.name}")
    
    # Process PDFs in parallel (2 at a time)
    max_workers = 2
    results = []
    
    print(f"\n🚀 Starting parallel processing with {max_workers} workers...")
    overall_start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_pdf = {
            executor.submit(process_single_pdf, str(pdf_file)): pdf_file 
            for pdf_file in pdf_files
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_pdf):
            pdf_file = future_to_pdf[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"❌ Exception processing {pdf_file.name}: {e}")
                results.append({
                    'pdf_path': str(pdf_file),
                    'output_file': None,
                    'success': False,
                    'error': str(e),
                    'processing_time': 0
                })
    
    overall_end_time = time.time()
    total_time = overall_end_time - overall_start_time
    
    # Print summary
    print(f"\n{'='*80}")
    print("PARALLEL PROCESSING SUMMARY")
    print(f"{'='*80}")
    print(f"Total PDFs processed: {len(pdf_files)}")
    print(f"Successful extractions: {sum(1 for r in results if r['success'])}")
    print(f"Failed extractions: {sum(1 for r in results if not r['success'])}")
    print(f"Total processing time: {total_time:.1f} seconds")
    print(f"Average time per PDF: {total_time/len(pdf_files):.1f} seconds")
    
    print(f"\n📊 Individual Results:")
    for result in results:
        pdf_name = Path(result['pdf_path']).name
        if result['success']:
            print(f"  ✅ {pdf_name}: {result['processing_time']:.1f}s -> {Path(result['output_file']).name}")
        else:
            print(f"  ❌ {pdf_name}: FAILED - {result.get('error', 'Unknown error')}")
    
    print(f"\n🎉 Parallel processing completed!")
    
    # Save batch results summary
    batch_summary = {
        'batch_timestamp': datetime.now().isoformat(),
        'total_pdfs': len(pdf_files),
        'successful': sum(1 for r in results if r['success']),
        'failed': sum(1 for r in results if not r['success']),
        'total_time_seconds': total_time,
        'average_time_per_pdf': total_time/len(pdf_files),
        'results': results
    }
    
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    batch_file = output_dir / f"batch_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(batch_file, 'w') as f:
        json.dump(batch_summary, f, indent=2)
    
    print(f"📄 Batch summary saved to: {batch_file}")
    return True

def process_single_default_pdf():
    """Process the default PDF file"""
    default_pdf = "plans/9339_lavendar_approved_plans.pdf"
    
    if not Path(default_pdf).exists():
        print(f"Error: Default PDF file not found: {default_pdf}")
        return False
    
    print(f"📄 Processing default PDF: {Path(default_pdf).name}")
    
    result = process_single_pdf(default_pdf)
    
    if result['success']:
        print(f"\n🎉 Single PDF processing completed successfully!")
        return True
    else:
        print(f"\n❌ Single PDF processing failed: {result.get('error', 'Unknown error')}")
        return False

def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="Tesseract-Based Construction Plans Takeoff Estimator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tesseract_takeoff.py                    # Process all PDFs in plans/ directory
  python tesseract_takeoff.py --all             # Process all PDFs in plans/ directory
  python tesseract_takeoff.py --default         # Process only the default PDF
  python tesseract_takeoff.py --file my_plan.pdf # Process a specific PDF file
        """
    )
    
    # Create mutually exclusive group for processing options
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--all', 
        action='store_true', 
        help='Process all PDF files in the plans/ directory (default behavior)'
    )
    group.add_argument(
        '--default', 
        action='store_true', 
        help='Process only the default PDF (9339_lavendar_approved_plans.pdf)'
    )
    group.add_argument(
        '--file', 
        type=str, 
        metavar='PDF_PATH',
        help='Process a specific PDF file (provide full path)'
    )
    
    parser.add_argument(
        '--workers', 
        type=int, 
        default=2, 
        metavar='N',
        help='Number of parallel workers for processing multiple PDFs (default: 2)'
    )
    
    parser.add_argument(
        '--verbose', '-v', 
        action='store_true', 
        help='Enable verbose logging output'
    )
    
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine which processing mode to use
    if args.default:
        print("🔧 Mode: Processing default PDF only")
        success = process_single_default_pdf()
    elif args.file:
        print(f"🔧 Mode: Processing specific file: {args.file}")
        if not Path(args.file).exists():
            print(f"Error: File not found: {args.file}")
            sys.exit(1)
        result = process_single_pdf(args.file)
        success = result['success']
    else:
        print("🔧 Mode: Processing all PDFs in plans/ directory")
        success = process_all_pdfs()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
