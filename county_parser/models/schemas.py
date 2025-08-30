"""Data schemas for different county file types."""

from datetime import date
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator
from decimal import Decimal


class RealAccountRecord(BaseModel):
    """Schema for real account records."""
    
    account_number: str = Field(alias="acct")
    year: int = Field(alias="yr")
    
    # Owner/mailing info
    owner_name: str = Field(alias="mailto")
    mail_address_1: Optional[str] = Field(default=None, alias="mail_addr_1")
    mail_address_2: Optional[str] = Field(default=None, alias="mail_addr_2")
    mail_city: Optional[str] = Field(default=None, alias="mail_city")
    mail_state: Optional[str] = Field(default=None, alias="mail_state")
    mail_zip: Optional[str] = Field(default=None, alias="mail_zip")
    mail_country: Optional[str] = Field(default=None, alias="mail_country")
    
    # Property address
    street_prefix: Optional[str] = Field(default=None, alias="str_pfx")
    street_number: Optional[str] = Field(default=None, alias="str_num")
    street_number_suffix: Optional[str] = Field(default=None, alias="str_num_sfx")
    street_name: Optional[str] = Field(default=None, alias="str")
    street_suffix: Optional[str] = Field(default=None, alias="str_sfx")
    street_suffix_direction: Optional[str] = Field(default=None, alias="str_sfx_dir")
    unit: Optional[str] = Field(default=None, alias="str_unit")
    
    site_address_1: Optional[str] = Field(default=None, alias="site_addr_1")
    site_address_2: Optional[str] = Field(default=None, alias="site_addr_2")
    site_address_3: Optional[str] = Field(default=None, alias="site_addr_3")
    
    # Property details
    school_district: Optional[str] = Field(default=None, alias="school_dist")
    market_area_1: Optional[str] = Field(default=None, alias="Market_Area_1")
    market_area_1_description: Optional[str] = Field(default=None, alias="Market_Area_1_Dscr")
    market_area_2: Optional[str] = Field(default=None, alias="Market_Area_2")
    market_area_2_description: Optional[str] = Field(default=None, alias="Market_Area_2_Dscr")
    
    year_improved: Optional[int] = Field(default=None, alias="yr_impr")
    building_area: Optional[int] = Field(default=None, alias="bld_ar")
    land_area: Optional[int] = Field(default=None, alias="land_ar")
    acreage: Optional[Decimal] = Field(default=None, alias="acreage")
    
    # Values
    land_value: Optional[Decimal] = Field(default=None, alias="land_val")
    building_value: Optional[Decimal] = Field(default=None, alias="bld_val")
    extra_features_value: Optional[Decimal] = Field(default=None, alias="x_features_val")
    agricultural_value: Optional[Decimal] = Field(default=None, alias="ag_val")
    assessed_value: Optional[Decimal] = Field(default=None, alias="assessed_val")
    total_appraised_value: Optional[Decimal] = Field(default=None, alias="tot_appr_val")
    total_market_value: Optional[Decimal] = Field(default=None, alias="tot_mkt_val")
    
    # Prior year values
    prior_land_value: Optional[Decimal] = Field(default=None, alias="prior_land_val")
    prior_building_value: Optional[Decimal] = Field(default=None, alias="prior_bld_val")
    prior_extra_features_value: Optional[Decimal] = Field(default=None, alias="prior_x_features_val")
    prior_agricultural_value: Optional[Decimal] = Field(default=None, alias="prior_ag_val")
    prior_total_appraised_value: Optional[Decimal] = Field(default=None, alias="prior_tot_appr_val")
    prior_total_market_value: Optional[Decimal] = Field(default=None, alias="prior_tot_mkt_val")
    
    # Status and legal
    value_status: Optional[str] = Field(default=None, alias="value_status")
    noticed: Optional[str] = Field(default=None, alias="noticed")
    notice_date: Optional[str] = Field(default=None, alias="notice_dt")
    protested: Optional[str] = Field(default=None, alias="protested")
    new_owner_date: Optional[str] = Field(default=None, alias="new_own_dt")
    
    legal_1: Optional[str] = Field(default=None, alias="lgl_1")
    legal_2: Optional[str] = Field(default=None, alias="lgl_2")
    legal_3: Optional[str] = Field(default=None, alias="lgl_3")
    legal_4: Optional[str] = Field(default=None, alias="lgl_4")
    
    jurisdictions: Optional[str] = Field(default=None, alias="jurs")
    
    class Config:
        populate_by_name = True


class OwnerRecord(BaseModel):
    """Schema for owner records."""
    
    account_number: str = Field(alias="acct")
    line_number: int = Field(alias="ln_num")
    name: str
    aka: Optional[str] = Field(default=None)
    ownership_percentage: Optional[Decimal] = Field(default=None, alias="pct_own")
    
    class Config:
        populate_by_name = True


class DeedRecord(BaseModel):
    """Schema for deed records."""
    
    account_number: str = Field(alias="acct")
    document_id: Optional[str] = Field(default=None)
    deed_date: Optional[date] = Field(default=None)
    grantor: Optional[str] = Field(default=None)
    grantee: Optional[str] = Field(default=None)
    
    class Config:
        populate_by_name = True


# NEW SCHEMAS FOR TRAVIS COUNTY AND ENHANCED DATA CAPTURE

class TaxEntityRecord(BaseModel):
    """Schema for tax entity/jurisdiction records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    tax_year: int = Field(description="Tax year")
    jurisdiction_id: str = Field(description="Tax jurisdiction identifier")
    entity_type: str = Field(description="Type of tax entity (e.g., TRAVIS CE, AUSTIN IS)")
    entity_name: str = Field(description="Name of the tax entity")
    entity_assessed_value: Optional[Decimal] = Field(default=None, description="Assessed value for this entity")
    entity_market_value: Optional[Decimal] = Field(default=None, description="Market value for this entity")
    entity_taxable_value: Optional[Decimal] = Field(default=None, description="Taxable value for this entity")
    prior_year_value: Optional[Decimal] = Field(default=None, description="Prior year value")
    tax_rate: Optional[Decimal] = Field(default=None, description="Tax rate for this entity")
    tax_amount: Optional[Decimal] = Field(default=None, description="Tax amount due")
    exemption_amount: Optional[Decimal] = Field(default=None, description="Exemption amount")
    
    class Config:
        populate_by_name = True


class ImprovementRecord(BaseModel):
    """Schema for improvement detail records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    tax_year: int = Field(description="Tax year")
    improvement_id: str = Field(description="Unique improvement identifier")
    improvement_type: str = Field(description="Type of improvement (e.g., 1st Floor, CANOPY)")
    improvement_class: str = Field(description="Improvement classification code")
    year_built: Optional[int] = Field(default=None, description="Year improvement was built")
    square_footage: Optional[Decimal] = Field(default=None, description="Square footage of improvement")
    value: Optional[Decimal] = Field(default=None, description="Value of the improvement")
    
    class Config:
        populate_by_name = True


class ImprovementAttributeRecord(BaseModel):
    """Schema for improvement attribute records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    tax_year: int = Field(description="Tax year")
    improvement_id: str = Field(description="Improvement identifier")
    attribute_type: str = Field(description="Type of attribute (e.g., Floor Factor, Foundation)")
    attribute_value: str = Field(description="Value of the attribute")
    
    class Config:
        populate_by_name = True


class LandDetailRecord(BaseModel):
    """Schema for land detail records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    tax_year: int = Field(description="Tax year")
    land_id: str = Field(description="Land identifier")
    land_type: str = Field(description="Type of land (e.g., LAND, COMM)")
    land_description: str = Field(description="Description of the land")
    land_class: str = Field(description="Land classification code")
    land_area: Optional[Decimal] = Field(default=None, description="Land area in square feet")
    land_value: Optional[Decimal] = Field(default=None, description="Land value")
    
    class Config:
        populate_by_name = True


class AgentRecord(BaseModel):
    """Schema for agent records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    agent_id: str = Field(description="Agent identifier")
    agent_name: str = Field(description="Name of the agent")
    agent_type: Optional[str] = Field(default=None, description="Type of agent")
    agent_address: Optional[str] = Field(default=None, description="Agent address")
    agent_city: Optional[str] = Field(default=None, description="Agent city")
    agent_state: Optional[str] = Field(default=None, description="Agent state")
    agent_zip: Optional[str] = Field(default=None, description="Agent ZIP code")
    
    class Config:
        populate_by_name = True


class SubdivisionRecord(BaseModel):
    """Schema for subdivision records (Travis County specific)."""
    
    subdivision_id: str = Field(description="Subdivision identifier")
    subdivision_name: str = Field(description="Name of the subdivision")
    subdivision_type: Optional[str] = Field(default=None, description="Type of subdivision")
    city: Optional[str] = Field(default=None, description="City where subdivision is located")
    county: Optional[str] = Field(default=None, description="County where subdivision is located")
    
    class Config:
        populate_by_name = True


class LawsuitRecord(BaseModel):
    """Schema for lawsuit records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    lawsuit_id: str = Field(description="Lawsuit identifier")
    lawsuit_type: Optional[str] = Field(default=None, description="Type of lawsuit")
    lawsuit_status: Optional[str] = Field(default=None, description="Status of the lawsuit")
    filing_date: Optional[str] = Field(default=None, description="Date lawsuit was filed")
    
    class Config:
        populate_by_name = True


class ArbitrationRecord(BaseModel):
    """Schema for arbitration records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    arbitration_id: str = Field(description="Arbitration identifier")
    arbitration_type: Optional[str] = Field(default=None, description="Type of arbitration")
    arbitration_status: Optional[str] = Field(default=None, description="Status of arbitration")
    filing_date: Optional[str] = Field(default=None, description="Date arbitration was filed")
    
    class Config:
        populate_by_name = True


class MobileHomeRecord(BaseModel):
    """Schema for mobile home records (Travis County specific)."""
    
    account_id: str = Field(description="Property account ID")
    mobile_home_id: str = Field(description="Mobile home identifier")
    mobile_home_type: Optional[str] = Field(default=None, description="Type of mobile home")
    year_built: Optional[int] = Field(default=None, description="Year mobile home was built")
    square_footage: Optional[Decimal] = Field(default=None, description="Square footage")
    value: Optional[Decimal] = Field(default=None, description="Value of mobile home")
    
    class Config:
        populate_by_name = True


class UnifiedPropertyRecord(BaseModel):
    """Unified schema that combines all property data sources."""
    
    # Core property information
    account_id: str = Field(description="Unique property account identifier")
    county: str = Field(description="County name")
    year: int = Field(description="Tax year")
    
    # Property address
    property_address: Dict[str, Any] = Field(description="Property physical address")
    mailing_address: Dict[str, Any] = Field(description="Owner mailing address")
    
    # Property details
    property_details: Dict[str, Any] = Field(description="Property characteristics and details")
    
    # Valuation
    valuation: Dict[str, Any] = Field(description="Property valuation information")
    
    # Legal status
    legal_status: Dict[str, Any] = Field(description="Legal and status information")
    
    # Tax entities (Travis County specific)
    tax_entities: List[TaxEntityRecord] = Field(default_factory=list, description="Tax jurisdiction records")
    
    # Improvements (Travis County specific)
    improvements: List[ImprovementRecord] = Field(default_factory=list, description="Improvement details")
    improvement_attributes: List[ImprovementAttributeRecord] = Field(default_factory=list, description="Improvement attributes")
    
    # Land details (Travis County specific)
    land_details: List[LandDetailRecord] = Field(default_factory=list, description="Land detail records")
    
    # Agents and representatives
    agents: List[AgentRecord] = Field(default_factory=list, description="Agent records")
    
    # Subdivisions
    subdivisions: List[SubdivisionRecord] = Field(default_factory=list, description="Subdivision information")
    
    # Legal proceedings
    lawsuits: List[LawsuitRecord] = Field(default_factory=list, description="Lawsuit records")
    arbitrations: List[ArbitrationRecord] = Field(default_factory=list, description="Arbitration records")
    
    # Mobile homes
    mobile_homes: List[MobileHomeRecord] = Field(default_factory=list, description="Mobile home records")
    
    # Owners
    owners: List[OwnerRecord] = Field(default_factory=list, description="Owner records")
    
    # Deeds
    deeds: List[DeedRecord] = Field(default_factory=list, description="Deed records")
    
    # Metadata
    metadata: Dict[str, Any] = Field(description="Processing metadata and timestamps")
    
    class Config:
        populate_by_name = True
