"""Data schemas for different county file types."""

from datetime import date
from typing import Optional, Dict, Any
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
