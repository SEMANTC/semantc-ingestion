# SEMANTC Shopify Data Integration

## Problem Statement
E-commerce platforms like Shopify generate vast amounts of operational data that is challenging to digest and analyze effectively. While Shopify provides built-in analytics, businesses need a more comprehensive solution that can:

1. Extract complete operational data across orders, products, customers, and transactions
2. Maintain data in its raw form for flexible analysis
3. Enable integration with other e-commerce platforms and marketplaces
4. Support AI-powered insights and analytics

This repository focuses specifically on the Shopify data ingestion layer, using the Shopify Admin GraphQL API to extract operational data in a format suitable for downstream analytics.

## Solution Overview

### Core Components

1. **Data Extraction**
   - Leverages Shopify's Bulk Operations API for efficient data extraction
   - Handles API rate limits and pagination
   - Maintains data lineage and audit trails

2. **Storage Format**
   - Stores data in JSONL format for BigQuery compatibility
   - Preserves raw data structure from Shopify
   - Enables creation of external tables in BigQuery

3. **Entity Coverage**
   - Orders and transactions
   - Products and inventory
   - Customers and marketing data
   - Shop configurations

## Project Structure
```
src/
├── client/            # Shopify API client implementation
├── extractors/        # Data extraction modules
├── processors/        # JSONL processing utilities
├── queries/          # GraphQL query definitions
├── loaders/          # GCS upload functionality
└── main.py           # Main execution script

data/
├── raw/              # Raw JSONL files from Shopify
├── processed/        # Processed JSON files (if needed)
└── state/            # Sync state and metadata
```

## Key Features

1. **Robust Data Extraction**
   - Handles large datasets through bulk operations
   - Maintains extraction state for incremental updates
   - Validates data completeness and integrity

2. **Error Handling**
   - Retries failed operations
   - Logs extraction errors
   - Maintains partial data on failures

3. **Storage Integration**
   - Uploads data to GCS
   - Maintains folder structure for easy querying
   - Supports BigQuery external tables

## Getting Started

### Prerequisites
- Python 3.9+
- Google Cloud Storage access
- Shopify Admin API access

### Configuration
```bash
# Required environment variables
SHOPIFY_STORE_URL="your-store.myshopify.com"
SHOPIFY_ACCESS_TOKEN="your-access-token"
GCS_BUCKET_NAME="your-bucket"
```

### Installation
```bash
# Clone repository
git clone https://github.com/your-org/semantc-shopify-integration.git

# Install dependencies
pip install -r requirements.txt

# Run extraction
python src/main.py
```

## Data Model

### Core Entities
1. Orders
   - Order details
   - Line items
   - Fulfillments
   - Refunds

2. Products
   - Product details
   - Variants
   - Inventory levels
   - Metafields

3. Customers
   - Customer details
   - Addresses
   - Marketing preferences

4. Shop
   - Configuration
   - Policies
   - Shipping settings

## Development

### Adding New Queries
1. Define GraphQL query in `queries/bulk_queries.py`
2. Add processing logic in `processors/data_processor.py`
3. Update extraction handling in `extractors/bulk_operations.py`

### Running Tests
```bash
pytest tests/
```

## Next Steps

### Short Term
- [ ] Review and enhance current data extraction coverage
- [ ] Add missing fields needed for core analytics
- [ ] Implement data validation checks
- [ ] Add monitoring for extraction jobs

### Medium Term
- [ ] Optimize refresh cycles
- [ ] Add support for incremental updates
- [ ] Enhance error handling
- [ ] Implement automated testing

## Documentation
- [Implementation Details](docs/implementation.md)
- [Query Reference](docs/queries.md)
- [Field Mappings](docs/field_mappings.md)

## External References
- [Shopify Admin API Reference](https://shopify.dev/api/admin-graphql)
- [BigQuery External Tables](https://cloud.google.com/bigquery/docs/external-data-sources)