// MongoDB initialization script for county data
// This runs when the container starts for the first time

print('🏗️  Initializing County Data Database...');

// Switch to the county data database
db = db.getSiblingDB('county_data');

// Create collections with proper indexing
db.createCollection('properties');
db.createCollection('processing_logs');

print('📊 Creating indexes for optimal query performance...');

// Property collection indexes
db.properties.createIndex({ "account_id": 1 }, { unique: true, name: "idx_account_id" });
db.properties.createIndex({ "property_address.zip_code": 1, "property_details.school_district": 1 }, { name: "idx_location" });
db.properties.createIndex({ "property_details.market_area_1": 1 }, { name: "idx_market_area" });
db.properties.createIndex({ "valuation.total_market_value": 1 }, { name: "idx_market_value" });
db.properties.createIndex({ "metadata.created_at": 1 }, { name: "idx_created_date" });
db.properties.createIndex({ "metadata.source_files": 1 }, { name: "idx_source_files" });

// Compound indexes for common queries
db.properties.createIndex({ 
    "property_details.school_district": 1, 
    "valuation.total_market_value": 1 
}, { name: "idx_school_value" });

db.properties.createIndex({ 
    "property_address.city": 1, 
    "property_details.market_area_1": 1 
}, { name: "idx_city_market" });

// Text search index for property addresses and owner names
db.properties.createIndex({ 
    "property_address.full_address": "text",
    "owners.name": "text",
    "legal_status.legal_description": "text"
}, { name: "idx_text_search" });

// Processing logs indexes
db.processing_logs.createIndex({ "timestamp": 1 }, { name: "idx_log_timestamp" });
db.processing_logs.createIndex({ "batch_id": 1 }, { name: "idx_batch_id" });
db.processing_logs.createIndex({ "status": 1 }, { name: "idx_status" });

print('✅ County Data Database initialized successfully!');
print('📋 Collections created: properties, processing_logs');
print('🔍 Indexes created for optimal query performance');
print('🚀 Ready for county data ingestion!');
