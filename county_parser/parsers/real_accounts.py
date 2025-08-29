"""Parser for real account data files."""

import polars as pl
from pathlib import Path
from typing import Dict, Any

from .base import BaseParser


class RealAccountsParser(BaseParser):
    """Parser for real account data files (real_acct.txt)."""
    
    def get_schema(self) -> Dict[str, Any]:
        """Return the polars schema for real account data."""
        return {
            "acct": pl.Utf8,
            "yr": pl.Int32,
            "mailto": pl.Utf8,
            "mail_addr_1": pl.Utf8,
            "mail_addr_2": pl.Utf8,
            "mail_city": pl.Utf8,
            "mail_state": pl.Utf8,
            "mail_zip": pl.Utf8,
            "mail_country": pl.Utf8,
            "str_pfx": pl.Utf8,
            "str_num": pl.Utf8,
            "str_num_sfx": pl.Utf8,
            "str": pl.Utf8,
            "str_sfx": pl.Utf8,
            "str_sfx_dir": pl.Utf8,
            "str_unit": pl.Utf8,
            "site_addr_1": pl.Utf8,
            "site_addr_2": pl.Utf8,
            "site_addr_3": pl.Utf8,
            "school_dist": pl.Utf8,
            "Market_Area_1": pl.Utf8,
            "Market_Area_1_Dscr": pl.Utf8,
            "Market_Area_2": pl.Utf8,
            "Market_Area_2_Dscr": pl.Utf8,
            "yr_impr": pl.Int32,
            "bld_ar": pl.Int32,
            "land_ar": pl.Int32,
            "acreage": pl.Float64,
            "land_val": pl.Float64,
            "bld_val": pl.Float64,
            "x_features_val": pl.Float64,
            "ag_val": pl.Float64,
            "assessed_val": pl.Float64,
            "tot_appr_val": pl.Float64,
            "tot_mkt_val": pl.Float64,
            "prior_land_val": pl.Float64,
            "prior_bld_val": pl.Float64,
            "prior_x_features_val": pl.Float64,
            "prior_ag_val": pl.Float64,
            "prior_tot_appr_val": pl.Float64,
            "prior_tot_mkt_val": pl.Float64,
            "value_status": pl.Utf8,
            "noticed": pl.Utf8,
            "notice_dt": pl.Utf8,
            "protested": pl.Utf8,
            "new_own_dt": pl.Utf8,
            "lgl_1": pl.Utf8,
            "lgl_2": pl.Utf8,
            "lgl_3": pl.Utf8,
            "lgl_4": pl.Utf8,
            "jurs": pl.Utf8,
        }
    
    def get_file_path(self) -> Path:
        """Return the path to the real accounts file."""
        return self.config.get_file_path(self.config.real_accounts_file)
    
    def preprocess_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply real accounts specific preprocessing."""
        
        # Remove quotes from string columns if present
        string_columns = [col for col, dtype in df.schema.items() if dtype == pl.Utf8]
        for col in string_columns:
            df = df.with_columns(
                pl.col(col).str.strip_chars('"').alias(col)
            )
        
        # Clean up numeric fields - convert empty strings to null
        numeric_columns = [
            "yr_impr", "bld_ar", "land_ar", "acreage",
            "land_val", "bld_val", "x_features_val", "ag_val", 
            "assessed_val", "tot_appr_val", "tot_mkt_val",
            "prior_land_val", "prior_bld_val", "prior_x_features_val", 
            "prior_ag_val", "prior_tot_appr_val", "prior_tot_mkt_val"
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col) == "")
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
        
        # Standardize boolean-like fields
        boolean_like_columns = ["noticed", "protested"]
        for col in boolean_like_columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).str.to_uppercase().alias(col)
                )
        
        # Clean zip codes - remove extra characters
        if "mail_zip" in df.columns:
            df = df.with_columns(
                pl.col("mail_zip").str.replace_all(r"[^0-9-]", "").alias("mail_zip")
            )
        
        # Create full address columns for easier searching
        df = df.with_columns([
            # Full mailing address
            pl.concat_str([
                pl.col("mail_addr_1"),
                pl.col("mail_addr_2")
            ], separator=", ", ignore_nulls=True).alias("full_mail_address"),
            
            # Full property address
            pl.concat_str([
                pl.col("str_pfx"),
                pl.col("str_num"),
                pl.col("str_num_sfx"),
                pl.col("str"),
                pl.col("str_sfx"),
                pl.col("str_sfx_dir"),
                pl.col("str_unit")
            ], separator=" ", ignore_nulls=True).alias("full_property_address"),
            
            # Full legal description
            pl.concat_str([
                pl.col("lgl_1"),
                pl.col("lgl_2"),
                pl.col("lgl_3"),
                pl.col("lgl_4")
            ], separator=" ", ignore_nulls=True).alias("full_legal_description")
        ])
        
        return df
    
    def get_summary_stats(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Get summary statistics for the real accounts data."""
        
        return {
            "total_records": len(df),
            "unique_accounts": df["acct"].n_unique(),
            "value_stats": {
                "avg_total_appraised_value": df["tot_appr_val"].mean(),
                "median_total_appraised_value": df["tot_appr_val"].median(),
                "max_total_appraised_value": df["tot_appr_val"].max(),
                "min_total_appraised_value": df["tot_appr_val"].min(),
            },
            "property_types": df.group_by("Market_Area_1_Dscr").agg(
                pl.count().alias("count")
            ).sort("count", descending=True).head(10).to_dicts(),
            "school_districts": df.group_by("school_dist").agg(
                pl.count().alias("count")
            ).sort("count", descending=True).head(10).to_dicts(),
            "year_improved_range": {
                "min": df["yr_impr"].min(),
                "max": df["yr_impr"].max()
            },
            "missing_data": {
                col: (df[col].is_null().sum() / len(df) * 100) 
                for col in ["mail_addr_1", "site_addr_1", "yr_impr", "tot_appr_val"]
                if col in df.columns
            }
        }
