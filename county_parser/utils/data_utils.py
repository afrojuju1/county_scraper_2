"""Data processing utilities."""

import polars as pl
from pathlib import Path
from typing import Union, Dict, Any, List


def detect_file_format(file_path: Union[str, Path]) -> str:
    """Detect the format of a data file."""
    
    path = Path(file_path)
    
    # First check by extension
    if path.suffix.lower() in ['.csv']:
        return 'csv'
    elif path.suffix.lower() in ['.parquet']:
        return 'parquet'
    elif path.suffix.lower() in ['.json']:
        return 'json'
    elif path.suffix.lower() in ['.txt']:
        # For .txt files, we need to examine the content
        return _detect_text_format(path)
    else:
        return _detect_text_format(path)


def _detect_text_format(file_path: Path) -> str:
    """Detect the format of a text file by examining its content."""
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sample = f.read(5000)  # Read first 5KB
        
        # Count potential delimiters
        delimiters = {
            'csv': sample.count(','),
            'tsv': sample.count('\t'),
            'pipe': sample.count('|')
        }
        
        # Return format with most delimiters
        if max(delimiters.values()) > 0:
            return max(delimiters, key=delimiters.get)
        else:
            return 'unknown'
            
    except Exception:
        return 'unknown'


def sample_data(file_path: Union[str, Path], n_rows: int = 1000) -> pl.DataFrame:
    """Get a sample of data from a file."""
    
    path = Path(file_path)
    file_format = detect_file_format(path)
    
    try:
        if file_format == 'csv':
            return pl.read_csv(path, n_rows=n_rows)
        elif file_format == 'tsv':
            return pl.read_csv(path, separator='\t', n_rows=n_rows)
        elif file_format == 'pipe':
            return pl.read_csv(path, separator='|', n_rows=n_rows)
        elif file_format == 'parquet':
            # Parquet doesn't support n_rows directly, so read all and take first n
            return pl.read_parquet(path).head(n_rows)
        else:
            # Try CSV as fallback
            return pl.read_csv(path, n_rows=n_rows)
            
    except Exception as e:
        raise ValueError(f"Could not sample data from {path}: {e}")


def compare_schemas(df1: pl.DataFrame, df2: pl.DataFrame) -> Dict[str, Any]:
    """Compare schemas of two DataFrames."""
    
    schema1 = df1.schema
    schema2 = df2.schema
    
    common_columns = set(schema1.keys()) & set(schema2.keys())
    only_in_df1 = set(schema1.keys()) - set(schema2.keys())
    only_in_df2 = set(schema2.keys()) - set(schema1.keys())
    
    type_mismatches = []
    for col in common_columns:
        if schema1[col] != schema2[col]:
            type_mismatches.append({
                'column': col,
                'df1_type': str(schema1[col]),
                'df2_type': str(schema2[col])
            })
    
    return {
        'common_columns': len(common_columns),
        'total_columns_df1': len(schema1),
        'total_columns_df2': len(schema2),
        'only_in_df1': list(only_in_df1),
        'only_in_df2': list(only_in_df2),
        'type_mismatches': type_mismatches
    }


def profile_dataframe(df: pl.DataFrame) -> Dict[str, Any]:
    """Generate a basic data profile for a DataFrame."""
    
    profile = {
        'shape': df.shape,
        'columns': list(df.columns),
        'dtypes': {col: str(dtype) for col, dtype in df.schema.items()},
        'memory_usage_mb': round(df.estimated_size("mb"), 2),
        'null_counts': {},
        'unique_counts': {}
    }
    
    # Calculate null and unique counts for each column
    for col in df.columns:
        try:
            profile['null_counts'][col] = df[col].null_count()
            profile['unique_counts'][col] = df[col].n_unique()
        except Exception:
            # Skip if there's an error (e.g., unsupported dtype)
            continue
    
    return profile
