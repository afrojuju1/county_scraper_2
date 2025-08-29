"""Base parser class for county data files."""

import polars as pl
from pathlib import Path
from typing import Dict, Any, Optional, Generator
from abc import ABC, abstractmethod
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from ..models import Config, ParsingOptions


class BaseParser(ABC):
    """Base class for parsing county data files."""
    
    def __init__(self, config: Config):
        self.config = config
        self.console = Console()
        
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Return the polars schema for the file type."""
        pass
        
    @abstractmethod
    def get_file_path(self) -> Path:
        """Return the path to the file to parse."""
        pass
        
    @abstractmethod
    def preprocess_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply file-specific preprocessing to the dataframe."""
        pass
        
    def parse_file(self, output_path: Optional[Path] = None) -> pl.DataFrame:
        """Parse the entire file and return a DataFrame."""
        
        file_path = self.get_file_path()
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        self.console.print(f"[bold green]Parsing {file_path.name}...[/bold green]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=self.console
        ) as progress:
            
            parse_task = progress.add_task("Loading data...", total=None)
            
            try:
                # Read file with polars for efficiency
                df = self._read_file(file_path)
                progress.update(parse_task, description=f"Loaded {len(df):,} rows")
                
                # Apply preprocessing
                df = self.preprocess_dataframe(df)
                progress.update(parse_task, description=f"Preprocessed {len(df):,} rows")
                
                # Save to output if specified
                if output_path:
                    self._save_dataframe(df, output_path)
                    progress.update(parse_task, description=f"Saved to {output_path}")
                
                progress.update(parse_task, description="✅ Complete", total=1, completed=1)
                
                return df
                
            except Exception as e:
                progress.update(parse_task, description=f"❌ Error: {e}")
                raise
    
    def parse_in_chunks(self, output_path: Optional[Path] = None) -> Generator[pl.DataFrame, None, None]:
        """Parse the file in chunks for memory efficiency."""
        
        file_path = self.get_file_path()
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        self.console.print(f"[bold green]Parsing {file_path.name} in chunks...[/bold green]")
        
        chunk_size = self.config.parsing.chunk_size
        
        # For CSV files, we can use polars' built-in chunking
        if file_path.suffix.lower() == '.csv' or file_path.name.endswith('.txt'):
            try:
                reader = pl.read_csv_batched(
                    file_path,
                    batch_size=chunk_size,
                    has_header=True,
                    infer_schema_length=1000,
                    try_parse_dates=True
                )
                
                chunk_num = 0
                for batch in reader:
                    chunk_num += 1
                    self.console.print(f"Processing chunk {chunk_num} ({len(batch):,} rows)")
                    
                    processed_chunk = self.preprocess_dataframe(batch)
                    
                    if output_path:
                        # Save chunk to separate file or append
                        chunk_output = output_path.with_suffix(f".chunk_{chunk_num:04d}{output_path.suffix}")
                        self._save_dataframe(processed_chunk, chunk_output)
                    
                    yield processed_chunk
                    
            except Exception as e:
                self.console.print(f"[red]Error processing chunks: {e}[/red]")
                raise
    
    def _read_file(self, file_path: Path) -> pl.DataFrame:
        """Read file using appropriate polars method based on file type."""
        
        if file_path.suffix.lower() == '.csv' or file_path.name.endswith('.txt'):
            # Try to detect delimiter
            delimiter = self._detect_delimiter(file_path)
            
            return pl.read_csv(
                file_path,
                separator=delimiter,
                has_header=True,
                infer_schema_length=10000,
                try_parse_dates=True,
                null_values=["", "NULL", "null", "N/A", "n/a"]
            )
        elif file_path.suffix.lower() == '.parquet':
            return pl.read_parquet(file_path)
        elif file_path.suffix.lower() == '.json':
            return pl.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")
    
    def _detect_delimiter(self, file_path: Path) -> str:
        """Detect the delimiter used in the file."""
        
        # Read first few lines to detect delimiter
        with open(file_path, 'r', encoding='utf-8') as f:
            sample = f.read(10000)  # Read first 10KB
            
        # Count common delimiters
        delimiters = {',': 0, '\t': 0, '|': 0, ';': 0}
        for line in sample.split('\n')[:10]:  # Check first 10 lines
            for delim in delimiters:
                delimiters[delim] += line.count(delim)
        
        # Return most common delimiter
        return max(delimiters, key=delimiters.get)
    
    def _save_dataframe(self, df: pl.DataFrame, output_path: Path):
        """Save DataFrame to specified output format."""
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        format_name = self.config.parsing.output_format.lower()
        
        if format_name == 'parquet':
            df.write_parquet(output_path.with_suffix('.parquet'))
        elif format_name == 'csv':
            df.write_csv(output_path.with_suffix('.csv'))
        elif format_name == 'json':
            df.write_json(output_path.with_suffix('.json'))
        else:
            raise ValueError(f"Unsupported output format: {format_name}")
    
    def get_file_info(self) -> Dict[str, Any]:
        """Get basic information about the file."""
        
        file_path = self.get_file_path()
        if not file_path.exists():
            return {"error": "File not found"}
            
        stat = file_path.stat()
        
        return {
            "file_name": file_path.name,
            "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
            "file_path": str(file_path),
            "exists": True
        }
