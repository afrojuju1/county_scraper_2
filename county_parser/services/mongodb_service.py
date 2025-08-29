"""MongoDB service for storing county property data."""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
import os


class MongoDBService:
    """Service for managing MongoDB operations for county data."""
    
    def __init__(self, mongo_uri: str = None, database: str = None):
        """Initialize MongoDB service."""
        self.console = Console()
        
        # Use environment variables or defaults
        self.mongo_uri = mongo_uri or os.getenv(
            'MONGO_URI', 
            'mongodb://admin:county_admin_2024@localhost:27017/county_data?authSource=admin'
        )
        self.database_name = database or os.getenv('MONGO_DATABASE', 'county_data')
        
        self.client = None
        self.database = None
        self.properties_collection = None
        self.logs_collection = None
        
    def connect(self) -> bool:
        """Connect to MongoDB."""
        try:
            self.console.print("[yellow]🔌 Connecting to MongoDB...[/yellow]")
            self.client = MongoClient(self.mongo_uri)
            
            # Test connection
            self.client.admin.command('ping')
            
            self.database = self.client[self.database_name]
            self.properties_collection = self.database['properties']
            self.logs_collection = self.database['processing_logs']
            
            self.console.print("[green]✅ Connected to MongoDB successfully![/green]")
            return True
            
        except ConnectionFailure as e:
            self.console.print(f"[red]❌ Failed to connect to MongoDB: {e}[/red]")
            return False
        except Exception as e:
            self.console.print(f"[red]❌ MongoDB connection error: {e}[/red]")
            return False
    
    def disconnect(self):
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            self.console.print("[blue]🔌 Disconnected from MongoDB[/blue]")
    
    def create_processing_log(self, batch_id: str, total_properties: int, source_files: List[str]) -> str:
        """Create a processing log entry."""
        log_entry = {
            "batch_id": batch_id,
            "timestamp": datetime.now(timezone.utc),
            "status": "started",
            "total_properties": total_properties,
            "processed_properties": 0,
            "source_files": source_files,
            "metadata": {
                "version": "1.0",
                "processing_type": "full_normalization"
            }
        }
        
        result = self.logs_collection.insert_one(log_entry)
        self.console.print(f"[blue]📝 Created processing log: {batch_id}[/blue]")
        return str(result.inserted_id)
    
    def update_processing_log(self, batch_id: str, status: str, processed_count: int = None, 
                            error_message: str = None):
        """Update processing log status."""
        update_data = {
            "status": status,
            "updated_at": datetime.now(timezone.utc)
        }
        
        if processed_count is not None:
            update_data["processed_properties"] = processed_count
            
        if error_message:
            update_data["error_message"] = error_message
            
        if status == "completed":
            update_data["completed_at"] = datetime.now(timezone.utc)
            
        self.logs_collection.update_one(
            {"batch_id": batch_id},
            {"$set": update_data}
        )
    
    def save_properties(self, properties_data: List[Dict], batch_id: str = None, 
                       source_files: List[str] = None) -> Dict[str, Any]:
        """Save property data to MongoDB with proper timestamping."""
        
        if self.database is None:
            raise Exception("Not connected to MongoDB")
        
        if not batch_id:
            batch_id = f"batch_{uuid.uuid4().hex[:8]}"
        
        timestamp = datetime.now(timezone.utc)
        
        # Add metadata to each property record
        enhanced_properties = []
        for prop in properties_data:
            enhanced_prop = dict(prop)  # Copy the property data
            enhanced_prop["metadata"] = {
                "batch_id": batch_id,
                "created_at": timestamp,
                "updated_at": timestamp,
                "source_files": source_files or [],
                "version": "1.0",
                "ingestion_type": "normalized_bulk"
            }
            enhanced_properties.append(enhanced_prop)
        
        # Create processing log
        log_id = self.create_processing_log(batch_id, len(enhanced_properties), source_files or [])
        
        # Save properties with progress tracking
        saved_count = 0
        duplicate_count = 0
        error_count = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            task = progress.add_task(
                f"[cyan]💾 Saving {len(enhanced_properties):,} properties to MongoDB...", 
                total=len(enhanced_properties)
            )
            
            try:
                # Use bulk operations for efficiency
                from pymongo import ReplaceOne
                
                operations = []
                for prop in enhanced_properties:
                    operations.append(
                        ReplaceOne(
                            {"account_id": prop["account_id"]},
                            prop,
                            upsert=True
                        )
                    )
                    
                    # Execute in batches of 1000
                    if len(operations) >= 1000:
                        result = self.properties_collection.bulk_write(operations)
                        saved_count += result.upserted_count + result.modified_count
                        operations = []
                        progress.update(task, advance=1000)
                
                # Execute remaining operations
                if operations:
                    result = self.properties_collection.bulk_write(operations)
                    saved_count += result.upserted_count + result.modified_count
                    progress.update(task, advance=len(operations))
                
                # Update processing log
                self.update_processing_log(batch_id, "completed", saved_count)
                
                progress.update(task, description="[green]✅ Properties saved successfully!")
                
            except Exception as e:
                error_count = len(enhanced_properties) - saved_count
                self.update_processing_log(batch_id, "failed", saved_count, str(e))
                raise Exception(f"Failed to save properties: {e}")
        
        result = {
            "batch_id": batch_id,
            "saved_count": saved_count,
            "duplicate_count": duplicate_count,
            "error_count": error_count,
            "total_count": len(enhanced_properties),
            "timestamp": timestamp.isoformat(),
            "log_id": log_id
        }
        
        self.console.print(f"[green]🎉 Successfully saved {saved_count:,} properties to MongoDB![/green]")
        self.console.print(f"[blue]📊 Batch ID: {batch_id}[/blue]")
        
        return result
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        if self.database is None:
            return {}
        
        properties_count = self.properties_collection.count_documents({})
        logs_count = self.logs_collection.count_documents({})
        
        # Get latest batch info
        latest_batch = self.logs_collection.find_one(
            {}, 
            sort=[("timestamp", -1)]
        )
        
        # Sample property for data structure info
        sample_property = self.properties_collection.find_one({})
        
        return {
            "properties_count": properties_count,
            "logs_count": logs_count,
            "latest_batch": latest_batch,
            "sample_structure": list(sample_property.keys()) if sample_property else [],
            "database_name": self.database_name
        }
    
    def query_properties(self, filter_query: Dict = None, limit: int = 100) -> List[Dict]:
        """Query properties with optional filters."""
        if self.database is None:
            return []
        
        cursor = self.properties_collection.find(filter_query or {}).limit(limit)
        return list(cursor)
    
    def create_backup(self, backup_path: Path):
        """Create a JSON backup of the current data."""
        if self.database is None:
            raise Exception("Not connected to MongoDB")
            
        self.console.print("[yellow]📦 Creating backup...[/yellow]")
        
        properties = list(self.properties_collection.find({}))
        
        # Convert ObjectId and datetime objects to JSON-serializable formats
        for prop in properties:
            if '_id' in prop:
                prop['_id'] = str(prop['_id'])
            
            # Handle nested datetime objects
            if 'metadata' in prop:
                metadata = prop['metadata']
                for key, value in metadata.items():
                    if isinstance(value, datetime):
                        metadata[key] = value.isoformat()
        
        backup_data = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "properties_count": len(properties),
            "properties": properties
        }
        
        with open(backup_path, 'w') as f:
            json.dump(backup_data, f, indent=2, default=str)
        
        self.console.print(f"[green]✅ Backup created: {backup_path}[/green]")
        return backup_path
