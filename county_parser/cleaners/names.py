"""Name cleaning and standardization utilities."""

import polars as pl
import re
from typing import List, Set


class NameCleaner:
    """Utilities for cleaning and standardizing name data."""
    
    # Common business entity types
    ENTITY_TYPES = {
        'INCORPORATED', 'INC', 'INC.', 'CORPORATION', 'CORP', 'CORP.',
        'COMPANY', 'CO', 'CO.', 'LIMITED LIABILITY COMPANY', 'LLC',
        'LIMITED PARTNERSHIP', 'LP', 'L.P.', 'LIMITED', 'LTD', 'LTD.',
        'PARTNERSHIP', 'PARTNERS', 'TRUST', 'ESTATE', 'FOUNDATION'
    }
    
    # Common name prefixes/suffixes that indicate business vs individual
    BUSINESS_INDICATORS = {
        'PROPERTIES', 'INVESTMENTS', 'HOLDINGS', 'VENTURES', 'DEVELOPMENT',
        'REAL ESTATE', 'MANAGEMENT', 'ASSOCIATES', 'PARTNERS', 'GROUP'
    }
    
    # Common individual name prefixes
    INDIVIDUAL_PREFIXES = {
        'MR', 'MR.', 'MRS', 'MRS.', 'MS', 'MS.', 'DR', 'DR.', 'PROF', 'PROF.',
        'REV', 'REV.', 'HON', 'HON.', 'SIR', 'DAME'
    }
    
    @staticmethod
    def clean_owner_name(df: pl.DataFrame, name_col: str) -> pl.DataFrame:
        """Clean and standardize owner names."""
        
        if name_col not in df.columns:
            return df
        
        # Create cleaned version
        df = df.with_columns([
            pl.col(name_col)
            .str.strip_chars()
            .str.to_uppercase()
            .str.replace_all(r'\s+', ' ')  # Multiple spaces to single
            .alias(f"{name_col}_cleaned")
        ])
        
        # Identify entity types
        entity_pattern = '|'.join(NameCleaner.ENTITY_TYPES)
        df = df.with_columns([
            pl.col(f"{name_col}_cleaned")
            .str.contains(rf'\b({entity_pattern})\b')
            .alias("is_business_entity"),
            
            # Extract entity type
            pl.col(f"{name_col}_cleaned")
            .str.extract(rf'\b({entity_pattern})\b')
            .alias("entity_type")
        ])
        
        # Identify likely business names
        business_pattern = '|'.join(NameCleaner.BUSINESS_INDICATORS)
        df = df.with_columns([
            (pl.col("is_business_entity") | 
             pl.col(f"{name_col}_cleaned").str.contains(rf'\b({business_pattern})\b'))
            .alias("is_likely_business")
        ])
        
        return df
    
    @staticmethod
    def parse_individual_names(df: pl.DataFrame, name_col: str) -> pl.DataFrame:
        """Parse individual names into components."""
        
        if name_col not in df.columns:
            return df
        
        # Only process names that are likely individuals
        df = df.with_columns([
            # Last name, First name pattern
            pl.when(pl.col(f"{name_col}_cleaned").str.contains(r'^([A-Z\s]+),\s*([A-Z\s]+)'))
            .then(
                pl.col(f"{name_col}_cleaned").str.extract(r'^([A-Z\s]+),\s*([A-Z\s]+)', 1)
            )
            .otherwise(None)
            .alias("last_name_parsed"),
            
            pl.when(pl.col(f"{name_col}_cleaned").str.contains(r'^([A-Z\s]+),\s*([A-Z\s]+)'))
            .then(
                pl.col(f"{name_col}_cleaned").str.extract(r'^([A-Z\s]+),\s*([A-Z\s]+)', 2)
            )
            .otherwise(None)
            .alias("first_name_parsed")
        ])
        
        return df
    
    @staticmethod
    def identify_related_owners(df: pl.DataFrame, name_col: str) -> pl.DataFrame:
        """Identify potentially related owners (same last name, similar addresses)."""
        
        # This is a placeholder for more sophisticated matching
        # Could use fuzzy matching algorithms like Levenshtein distance
        
        df = df.with_columns([
            # Extract potential last names for matching
            pl.when(pl.col("last_name_parsed").is_not_null())
            .then(pl.col("last_name_parsed"))
            .otherwise(
                pl.col(f"{name_col}_cleaned").str.extract(r'^(\w+)')
            )
            .alias("matching_key")
        ])
        
        return df
    
    @staticmethod
    def standardize_trust_estate_names(df: pl.DataFrame, name_col: str) -> pl.DataFrame:
        """Standardize trust and estate name formats."""
        
        if name_col not in df.columns:
            return df
        
        # Common trust/estate patterns
        trust_patterns = [
            (r'(.+)\s+TRUST\b', r'\1 TRUST'),
            (r'(.+)\s+ESTATE\s+OF\b', r'ESTATE OF \1'),
            (r'ESTATE\s+OF\s+(.+)', r'ESTATE OF \1'),
            (r'(.+)\s+FAMILY\s+TRUST', r'\1 FAMILY TRUST'),
            (r'(.+)\s+REVOCABLE\s+TRUST', r'\1 REVOCABLE TRUST')
        ]
        
        cleaned_col = f"{name_col}_cleaned"
        for pattern, replacement in trust_patterns:
            df = df.with_columns(
                pl.col(cleaned_col)
                .str.replace_all(pattern, replacement)
                .alias(cleaned_col)
            )
        
        return df
