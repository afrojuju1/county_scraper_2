// Multi-County Property Data Viewer - Frontend JavaScript
class PropertyViewer {
    constructor() {
        this.properties = [];
        this.filteredProperties = [];
        this.stats = {};
        
        this.initEventListeners();
        this.loadInitialData();
    }
    
    initEventListeners() {
        // Control listeners
        document.getElementById('countyFilter').addEventListener('change', () => this.loadProperties());
        document.getElementById('sampleSize').addEventListener('change', () => this.loadProperties());
        document.getElementById('refreshBtn').addEventListener('click', () => this.loadProperties());
        document.getElementById('searchInput').addEventListener('input', (e) => this.filterTable(e.target.value));
    }
    
    async loadInitialData() {
        await Promise.all([
            this.loadStats(),
            this.loadProperties()
        ]);
    }
    
    async loadStats() {
        try {
            const response = await fetch('/api/stats');
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.stats = data;
            this.renderStats();
            this.renderCountyChart();
            
        } catch (error) {
            console.error('Error loading stats:', error);
            this.showError('Failed to load database statistics');
        }
    }
    
    async loadProperties() {
        try {
            // Show loading state
            this.showTableLoading();
            
            const countyFilter = document.getElementById('countyFilter').value;
            const sampleSize = document.getElementById('sampleSize').value;
            
            const params = new URLSearchParams({
                limit: sampleSize,
                county: countyFilter
            });
            
            const response = await fetch(`/api/properties?${params}`);
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.properties = data.properties;
            this.filteredProperties = [...this.properties];
            this.renderTable();
            this.updateTableInfo();
            
        } catch (error) {
            console.error('Error loading properties:', error);
            this.showError('Failed to load property data');
        }
    }
    
    renderStats() {
        const statsCards = document.getElementById('statsCards');
        if (!statsCards || !this.stats) return;
        
        const totalProperties = this.stats.total_properties || 0;
        const countiesCount = this.stats.counties ? this.stats.counties.length : 0;
        const avgValue = this.stats.values && this.stats.values.avg_value 
            ? this.formatCurrency(this.stats.values.avg_value) 
            : 'N/A';
        
        statsCards.innerHTML = `
            <div class="stat-card">
                <div class="stat-value">${totalProperties.toLocaleString()}</div>
                <div class="stat-label">Total Properties</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${countiesCount}</div>
                <div class="stat-label">Counties</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${avgValue}</div>
                <div class="stat-label">Avg Value</div>
            </div>
        `;
    }
    
    renderCountyChart() {
        const chartContainer = document.getElementById('countyChart');
        if (!chartContainer || !this.stats.counties) return;
        
        const totalProperties = this.stats.total_properties || 1;
        
        chartContainer.innerHTML = this.stats.counties.map(county => {
            const percentage = ((county.count / totalProperties) * 100).toFixed(1);
            const countyName = county._id ? county._id.charAt(0).toUpperCase() + county._id.slice(1) : 'Unknown';
            
            return `
                <div class="county-stat">
                    <div class="county-name">${countyName} County</div>
                    <div class="county-count">${county.count.toLocaleString()}</div>
                    <div class="county-percent">${percentage}% of total</div>
                </div>
            `;
        }).join('');
    }
    
    renderTable() {
        const tbody = document.querySelector('#propertiesTable tbody');
        if (!tbody) return;
        
        if (this.filteredProperties.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="loading-row">
                        No properties found matching current filters.
                    </td>
                </tr>
            `;
            return;
        }
        
        tbody.innerHTML = this.filteredProperties.map(property => {
            const county = property.county || 'unknown';
            const accountId = property.account_id || 'N/A';
            const owner = this.getOwnerName(property);
            const address = this.getPropertyAddress(property);
            const marketValue = this.getMarketValue(property);
            const taxEntities = this.getTaxEntitiesCount(property);
            
            return `
                <tr>
                    <td><span class="county-badge county-${county.toLowerCase()}">${county}</span></td>
                    <td class="account-id">${accountId}</td>
                    <td>${this.truncateText(owner, 30)}</td>
                    <td>${this.truncateText(address, 35)}</td>
                    <td class="value-amount">${marketValue}</td>
                    <td class="tax-entities">${taxEntities}</td>
                </tr>
            `;
        }).join('');
    }
    
    filterTable(searchTerm) {
        const term = searchTerm.toLowerCase().trim();
        
        if (!term) {
            this.filteredProperties = [...this.properties];
        } else {
            this.filteredProperties = this.properties.filter(property => {
                const county = (property.county || '').toLowerCase();
                const accountId = (property.account_id || '').toLowerCase();
                const owner = this.getOwnerName(property).toLowerCase();
                const address = this.getPropertyAddress(property).toLowerCase();
                
                return county.includes(term) ||
                       accountId.includes(term) ||
                       owner.includes(term) ||
                       address.includes(term);
            });
        }
        
        this.renderTable();
        this.updateTableInfo();
    }
    
    updateTableInfo() {
        const tableInfo = document.getElementById('tableInfo');
        if (!tableInfo) return;
        
        const totalCount = this.properties.length;
        const filteredCount = this.filteredProperties.length;
        
        if (filteredCount === totalCount) {
            tableInfo.textContent = `Showing ${totalCount} properties`;
        } else {
            tableInfo.textContent = `Showing ${filteredCount} of ${totalCount} properties`;
        }
    }
    
    showTableLoading() {
        const tbody = document.querySelector('#propertiesTable tbody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="loading-row">
                        <div class="loading-spinner">Loading property data...</div>
                    </td>
                </tr>
            `;
        }
    }
    
    showError(message) {
        const tbody = document.querySelector('#propertiesTable tbody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="loading-row" style="color: #e53e3e;">
                        ❌ ${message}
                    </td>
                </tr>
            `;
        }
    }
    
    // Helper methods for data extraction
    getOwnerName(property) {
        if (property.mailing_address && property.mailing_address.name) {
            return property.mailing_address.name;
        }
        if (property.owners && property.owners.length > 0) {
            return property.owners[0].name || 'Unknown Owner';
        }
        return 'Unknown Owner';
    }
    
    getPropertyAddress(property) {
        if (property.property_address) {
            const addr = property.property_address;
            if (addr.street_address && addr.city) {
                return `${addr.street_address}, ${addr.city}`;
            } else if (addr.street_address) {
                return addr.street_address;
            } else if (addr.full_address) {
                return addr.full_address;
            }
        }
        return 'No Address';
    }
    
    getMarketValue(property) {
        if (property.valuation) {
            const val = property.valuation;
            
            // Try different market value fields in order of preference
            let marketValue = val.market_value || val.total_market_value || val.assessed_value || val.total_value;
            
            if (marketValue !== undefined && marketValue !== null) {
                // Convert to number if it's a string
                const numValue = typeof marketValue === 'string' ? parseFloat(marketValue) : marketValue;
                
                // Handle extremely large values (likely parsing errors from Dallas data)
                if (numValue > 10000000000) { // > 10 billion suggests error
                    // Try to fix by dividing by 100000000 (common Dallas parsing issue)
                    const corrected = numValue / 100000000;
                    if (corrected > 0 && corrected < 100000000) { // Reasonable range
                        return this.formatCurrency(corrected);
                    }
                }
                
                if (numValue > 0 && numValue < 100000000) { // Reasonable property value range
                    return this.formatCurrency(numValue);
                }
            }
        }
        return '$0';
    }
    
    getTaxEntitiesCount(property) {
        if (property.tax_entities && Array.isArray(property.tax_entities)) {
            return property.tax_entities.length || '-';
        }
        return '-';
    }
    
    formatCurrency(value) {
        const num = parseFloat(value);
        if (isNaN(num)) return '$0';
        
        if (num >= 1e9) {
            return `$${(num / 1e9).toFixed(1)}B`;
        } else if (num >= 1e6) {
            return `$${(num / 1e6).toFixed(1)}M`;
        } else if (num >= 1e3) {
            return `$${(num / 1e3).toFixed(0)}K`;
        } else {
            return `$${num.toLocaleString()}`;
        }
    }
    
    truncateText(text, maxLength) {
        if (!text || text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    }
}

// Initialize the app when the page loads
document.addEventListener('DOMContentLoaded', () => {
    new PropertyViewer();
});
