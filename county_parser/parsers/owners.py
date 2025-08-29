"""Parser for owner data files."""

import polars as pl
from pathlib import Path
from typing import Dict, Any

from .base import BaseParser


class OwnersParser(BaseParser):
    """Parser for owner data files (owners.txt)."""
    
    def get_schema(self) -> Dict[str, Any]:
        """Return the polars schema for owner data."""
        return {
            "acct": pl.Utf8,
            "ln_num": pl.Int32,
            "name": pl.Utf8,
            "aka": pl.Utf8,
            "pct_own": pl.Float64,
        }
    
    def get_file_path(self) -> Path:
        """Return the path to the owners file."""
        return self.config.get_file_path(self.config.owners_file)
    
    def preprocess_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply owners specific preprocessing."""
        
        # Clean up name field - remove extra whitespace
        if "name" in df.columns:
            df = df.with_columns(
                pl.col("name").str.strip_chars().alias("name")
            )
        
        # Clean up AKA field
        if "aka" in df.columns:
            df = df.with_columns(
                pl.col("aka").str.strip_chars().alias("aka")
            )
        
        # Convert ownership percentage - handle string values
        if "pct_own" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("pct_own") == "")
                .then(None)
                .otherwise(pl.col("pct_own"))
                .alias("pct_own")
            )
        
        # Add derived fields for analysis
        df = df.with_columns([
            # Flag for primary owner (first line number)
            (pl.col("ln_num") == 1).alias("is_primary_owner"),
            
            # Flag for full ownership
            (pl.col("pct_own") >= 1.0).alias("is_full_owner"),
            
            # Clean name variants for matching
            pl.col("name").str.to_uppercase().str.replace_all(r"[^A-Z0-9\s]", "").alias("name_normalized")
        ])
        
        return df
    
    def get_summary_stats(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Get summary statistics for the owners data."""
        
        return {
            "total_records": len(df),
            "unique_accounts": df["acct"].n_unique(),
            "unique_owners": df["name"].n_unique(),
            "ownership_stats": {
                "avg_ownership_pct": df["pct_own"].mean(),
                "full_ownership_count": df.filter(pl.col("pct_own") >= 1.0).shape[0],
                "partial_ownership_count": df.filter(pl.col("pct_own") < 1.0).shape[0],
            },
            "multiple_owners": df.group_by("acct").agg(
                pl.count().alias("owner_count")
            ).filter(pl.col("owner_count") > 1).shape[0],
            "common_owner_types": df.filter(
                pl.col("name").str.contains("LLC|INC|CORP|TRUST|ESTATE")
            ).group_by("name").agg(
                pl.count().alias("count")
            ).sort("count", descending=True).head(10).to_dicts(),
            "missing_data": {
                "name": (df["name"].is_null().sum() / len(df) * 100),
                "pct_own": (df["pct_own"].is_null().sum() / len(df) * 100),
            }
        }
