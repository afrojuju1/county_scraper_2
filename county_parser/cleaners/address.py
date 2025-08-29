"""Address cleaning and standardization utilities."""

import polars as pl
import re
from typing import List


class AddressCleaner:
    """Utilities for cleaning and standardizing address data."""
    
    # Common address abbreviations
    STREET_TYPES = {
        'AVENUE': 'AVE', 'AVE.': 'AVE',
        'BOULEVARD': 'BLVD', 'BLVD.': 'BLVD',
        'CIRCLE': 'CIR', 'CIR.': 'CIR',
        'COURT': 'CT', 'CT.': 'CT',
        'DRIVE': 'DR', 'DR.': 'DR',
        'LANE': 'LN', 'LN.': 'LN',
        'PLACE': 'PL', 'PL.': 'PL',
        'ROAD': 'RD', 'RD.': 'RD',
        'STREET': 'ST', 'ST.': 'ST',
        'TRAIL': 'TRL', 'TRL.': 'TRL',
        'WAY': 'WAY'
    }
    
    DIRECTIONS = {
        'NORTH': 'N', 'N.': 'N',
        'SOUTH': 'S', 'S.': 'S',
        'EAST': 'E', 'E.': 'E',
        'WEST': 'W', 'W.': 'W',
        'NORTHEAST': 'NE', 'N.E.': 'NE',
        'NORTHWEST': 'NW', 'N.W.': 'NW',
        'SOUTHEAST': 'SE', 'S.E.': 'SE',
        'SOUTHWEST': 'SW', 'S.W.': 'SW'
    }
    
    @staticmethod
    def clean_address(df: pl.DataFrame, address_col: str) -> pl.DataFrame:
        """Clean and standardize address column."""
        
        if address_col not in df.columns:
            return df
        
        # Create a cleaned version of the address
        df = df.with_columns([
            # Remove extra whitespace and normalize case
            pl.col(address_col)
            .str.strip_chars()
            .str.to_uppercase()
            .alias(f"{address_col}_cleaned")
        ])
        
        # Apply street type standardization
        for full_name, abbrev in AddressCleaner.STREET_TYPES.items():
            df = df.with_columns(
                pl.col(f"{address_col}_cleaned")
                .str.replace_all(rf"\b{full_name}\b", abbrev)
                .alias(f"{address_col}_cleaned")
            )
        
        # Apply direction standardization
        for full_name, abbrev in AddressCleaner.DIRECTIONS.items():
            df = df.with_columns(
                pl.col(f"{address_col}_cleaned")
                .str.replace_all(rf"\b{full_name}\b", abbrev)
                .alias(f"{address_col}_cleaned")
            )
        
        # Remove common noise
        df = df.with_columns(
            pl.col(f"{address_col}_cleaned")
            .str.replace_all(r'\s+', ' ')  # Multiple spaces to single
            .str.replace_all(r'[^\w\s-]', '')  # Remove special chars except hyphens
            .str.strip_chars()
            .alias(f"{address_col}_cleaned")
        )
        
        return df
    
    @staticmethod
    def clean_zip_code(df: pl.DataFrame, zip_col: str) -> pl.DataFrame:
        """Clean and standardize ZIP code column."""
        
        if zip_col not in df.columns:
            return df
        
        df = df.with_columns([
            # Extract 5-digit ZIP or ZIP+4
            pl.col(zip_col)
            .str.extract(r'(\d{5}(?:-\d{4})?)')
            .alias(f"{zip_col}_cleaned"),
            
            # Extract just the 5-digit ZIP
            pl.col(zip_col)
            .str.extract(r'(\d{5})')
            .alias(f"{zip_col}_5digit")
        ])
        
        return df
    
    @staticmethod
    def geocode_addresses(df: pl.DataFrame, address_col: str, city_col: str = None, 
                         state_col: str = None, zip_col: str = None) -> pl.DataFrame:
        """Add geocoding information for addresses (placeholder for future enhancement)."""
        
        # This would integrate with a geocoding service like Google Maps API
        # For now, just add placeholder columns
        df = df.with_columns([
            pl.lit(None, dtype=pl.Float64).alias("latitude"),
            pl.lit(None, dtype=pl.Float64).alias("longitude"),
            pl.lit(None, dtype=pl.Utf8).alias("geocoded_address")
        ])
        
        return df
