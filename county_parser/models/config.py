"""Configuration models using Pydantic."""

from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field


class ParsingOptions(BaseModel):
    """Options for parsing county data files."""
    
    chunk_size: int = Field(default=10000, description="Number of rows to process at once")
    max_workers: int = Field(default=4, description="Maximum number of worker processes")
    skip_errors: bool = Field(default=True, description="Skip rows with parsing errors")
    output_format: str = Field(default="parquet", description="Output format (parquet, csv, json)")
    clean_data: bool = Field(default=True, description="Apply data cleaning transformations")


class Config(BaseModel):
    """Main configuration for the county parser."""
    
    data_dir: Path = Field(default_factory=lambda: Path.home() / "Downloads" / "hcad_2025")
    output_dir: Path = Field(default_factory=lambda: Path.cwd() / "output")
    
    # File names
    real_accounts_file: str = "real_acct.txt"
    owners_file: str = "owners.txt"
    deeds_file: str = "deeds.txt"
    permits_file: str = "permits.txt"
    parcel_tieback_file: str = "parcel_tieback.txt"
    
    # Parsing options
    parsing: ParsingOptions = Field(default_factory=ParsingOptions)
    
    def get_file_path(self, filename: str) -> Path:
        """Get full path to a data file."""
        return self.data_dir / filename
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        config_dict = {}
        
        # Map environment variables to config fields
        if data_dir := os.getenv("DATA_DIR"):
            config_dict["data_dir"] = Path(data_dir).expanduser()
            
        if output_dir := os.getenv("OUTPUT_DIR"):
            config_dict["output_dir"] = Path(output_dir).expanduser()
            
        # File names
        for field in ["real_accounts_file", "owners_file", "deeds_file", "permits_file", "parcel_tieback_file"]:
            if value := os.getenv(field.upper()):
                config_dict[field] = value
        
        # Parsing options
        parsing_dict = {}
        if chunk_size := os.getenv("CHUNK_SIZE"):
            parsing_dict["chunk_size"] = int(chunk_size)
        if max_workers := os.getenv("MAX_WORKERS"):
            parsing_dict["max_workers"] = int(max_workers)
            
        if parsing_dict:
            config_dict["parsing"] = ParsingOptions(**parsing_dict)
        
        return cls(**config_dict)
