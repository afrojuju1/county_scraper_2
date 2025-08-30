"""
Simple Flask web application to visualize property data from our multi-county database.
"""

from flask import Flask, render_template, jsonify, request
from county_parser.services.mongodb_service import MongoDBService
from county_parser.models.config import Config
import json
from datetime import datetime

app = Flask(__name__)

# Initialize MongoDB service
config = Config()
mongodb = MongoDBService()

@app.route('/')
def index():
    """Main dashboard page."""
    return render_template('index.html')

@app.route('/api/properties')
def get_properties():
    """API endpoint to get property data."""
    try:
        # Get query parameters
        sample_size = int(request.args.get('limit', 100))
        county_filter = request.args.get('county', 'all')
        
        if not mongodb.connect():
            return jsonify({'error': 'Failed to connect to database'}), 500
        
        try:
            # Build MongoDB query
            query = {}
            if county_filter != 'all':
                query['county'] = county_filter.lower()
            
            # Get random sample
            pipeline = [
                {'$match': query},
                {'$sample': {'size': min(sample_size, 1000)}}  # Limit to 1000 max
            ]
            
            properties = list(mongodb.properties_collection.aggregate(pipeline))
            
            # Convert ObjectId to string for JSON serialization
            for prop in properties:
                if '_id' in prop:
                    prop['_id'] = str(prop['_id'])
            
            return jsonify({
                'properties': properties,
                'count': len(properties),
                'query': query
            })
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Get database statistics."""
    try:
        if not mongodb.connect():
            return jsonify({'error': 'Failed to connect to database'}), 500
        
        try:
            # Get collection stats
            stats = mongodb.get_collection_stats()
            
            # Get county distribution
            pipeline = [
                {'$group': {'_id': '$county', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]
            county_stats = list(mongodb.properties_collection.aggregate(pipeline))
            
            # Get value statistics (sample for performance) - handle both market_value and total_market_value
            value_pipeline = [
                {'$sample': {'size': 1000}},
                {'$match': {
                    '$or': [
                        {'valuation.market_value': {'$exists': True, '$gt': 0, '$lt': 100000000}},
                        {'valuation.total_market_value': {'$exists': True, '$gt': 0, '$lt': 100000000}},
                        {'valuation.assessed_value': {'$exists': True, '$gt': 0, '$lt': 100000000}}
                    ]
                }},
                {'$addFields': {
                    'effective_value': {
                        '$cond': {
                            'if': {'$and': [
                                {'$gt': ['$valuation.market_value', 0]},
                                {'$lt': ['$valuation.market_value', 100000000]}
                            ]},
                            'then': '$valuation.market_value',
                            'else': {
                                '$cond': {
                                    'if': {'$and': [
                                        {'$gt': ['$valuation.total_market_value', 0]},
                                        {'$lt': ['$valuation.total_market_value', 100000000]}
                                    ]},
                                    'then': '$valuation.total_market_value',
                                    'else': '$valuation.assessed_value'
                                }
                            }
                        }
                    }
                }},
                {'$group': {
                    '_id': None,
                    'avg_value': {'$avg': '$effective_value'},
                    'min_value': {'$min': '$effective_value'},
                    'max_value': {'$max': '$effective_value'},
                    'count': {'$sum': 1}
                }}
            ]
            
            try:
                value_stats = list(mongodb.properties_collection.aggregate(value_pipeline))
            except Exception as e:
                print(f"Error in value stats aggregation: {e}")
                value_stats = [{'avg_value': 0, 'min_value': 0, 'max_value': 0, 'count': 0}]
            
            return jsonify({
                'total_properties': stats['properties_count'],
                'total_logs': stats['logs_count'],
                'counties': county_stats,
                'values': value_stats[0] if value_stats else {},
                'last_updated': datetime.now().isoformat()
            })
            
        finally:
            mongodb.disconnect()
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
