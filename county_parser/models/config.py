"""Configuration models using Pydantic."""

from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class ParsingOptions(BaseModel):
    """Options for parsing county data files."""
    
    chunk_size: int = Field(default=10000, description="Number of rows to process at once")
    max_workers: int = Field(default=4, description="Maximum number of worker processes")
    skip_errors: bool = Field(default=True, description="Skip rows with parsing errors")
    output_format: str = Field(default="parquet", description="Output format (parquet, csv, json)")
    clean_data: bool = Field(default=True, description="Apply data cleaning transformations")
    
    # Travis County specific options
    extract_improvements: bool = Field(default=True, description="Extract improvement details")
    extract_land_details: bool = Field(default=True, description="Extract land detail records")
    extract_tax_entities: bool = Field(default=True, description="Extract tax entity records")
    extract_agents: bool = Field(default=True, description="Extract agent records")
    extract_legal_proceedings: bool = Field(default=True, description="Extract lawsuit/arbitration records")


class TravisCountyConfig(BaseModel):
    """Configuration specific to Travis County data files."""
    
    # Core property files
    properties_file: str = "PROP.TXT"
    property_entities_file: str = "PROP_ENT.TXT"
    
    # Improvement files
    improvements_file: str = "IMP_DET.TXT"
    improvement_attributes_file: str = "IMP_ATR.TXT"
    improvement_info_file: str = "IMP_INFO.TXT"
    
    # Land and property detail files
    land_details_file: str = "LAND_DET.TXT"
    subdivisions_file: str = "ABS_SUBD.TXT"
    
    # Agent and entity files
    agents_file: str = "AGENT.TXT"
    entity_file: str = "ENTITY.TXT"
    
    # Legal and administrative files
    lawsuits_file: str = "LAWSUIT.TXT"
    arbitration_file: str = "ARB.TXT"
    mobile_homes_file: str = "MOBILE_HOME_INFO.TXT"
    
    # Reference files
    country_file: str = "COUNTRY.TXT"
    state_codes_file: str = "STATE_CD.TXT"
    totals_file: str = "TOTALS.TXT"
    tax_deferral_file: str = "TAX_DEFERRAL_INFO.TXT"
    appraisal_header_file: str = "APPR_HDR.TXT"
    
    # Field specifications
    property_line_length: int = Field(default=9247, description="Expected line length for PROP.TXT")
    entity_line_length: int = Field(default=2750, description="Expected line length for PROP_ENT.TXT")
    
    # Processing options
    max_records_per_file: Optional[int] = Field(default=None, description="Maximum records to process per file")
    validate_field_positions: bool = Field(default=True, description="Validate field positions during extraction")


class Config(BaseModel):
    """Main configuration for the county parser."""
    
    data_dir: Path = Field(default_factory=lambda: Path.home() / "Downloads" / "hcad_2025")
    output_dir: Path = Field(default_factory=lambda: Path.cwd() / "output")
    
    # County type
    county_type: str = Field(default="harris", description="Type of county (harris, travis, etc.)")
    
    # File names (Harris County specific)
    real_accounts_file: str = "real_acct.txt"
    owners_file: str = "owners.txt"
    deeds_file: str = "deeds.txt"
    permits_file: str = "permits.txt"
    parcel_tieback_file: str = "parcel_tieback.txt"
    
    # Travis County specific configuration
    travis_config: Optional[TravisCountyConfig] = Field(default_factory=TravisCountyConfig, description="Travis County specific settings")
    
    # Parsing options
    parsing: ParsingOptions = Field(default_factory=ParsingOptions)
    
    def get_file_path(self, filename: str) -> Path:
        """Get full path to a data file."""
        return self.data_dir / filename
    
    def get_travis_file_path(self, filename: str) -> Path:
        """Get full path to a Travis County data file."""
        if self.county_type == "travis":
            return self.data_dir / "travis_2025" / filename
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
        
        if county_type := os.getenv("COUNTY_TYPE"):
            config_dict["county_type"] = county_type
            
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
        if extract_improvements := os.getenv("EXTRACT_IMPROVEMENTS"):
            parsing_dict["extract_improvements"] = extract_improvements.lower() == "true"
        if extract_land_details := os.getenv("EXTRACT_LAND_DETAILS"):
            parsing_dict["extract_land_details"] = extract_land_details.lower() == "true"
        if extract_tax_entities := os.getenv("EXTRACT_TAX_ENTITIES"):
            parsing_dict["extract_tax_entities"] = extract_tax_entities.lower() == "true"
            
        if parsing_dict:
            config_dict["parsing"] = ParsingOptions(**parsing_dict)
        
        return cls(**config_dict)
