# Shopify Analytics Integration Project

## Project Structure
```
.
├── Dockerfile
├── requirements.txt
└── src/
    ├── main.py
    ├── client/
    │   ├── __init__.py
    │   └── shopify_client.py
    ├── extractors/
    │   ├── __init__.py
    │   ├── base.py
    │   └── bulk_operations.py
    ├── processors/
    │   ├── __init__.py
    │   ├── data_processor.py
    │   └── sync_state.py
    ├── queries/
    │   ├── __init__.py
    │   └── bulk_queries.py
    └── loaders/
        ├── __init__.py
        └── gcs_loader.py
```

## Component Overview

### Core Files

#### `Dockerfile`
- Base image: Python 3.9-slim
- Sets up working environment and dependencies
- Creates necessary directory structure for data processing
- Configures Python path and environment variables

#### `main.py`
- Entry point of the application
- Orchestrates the ETL process
- Manages sync tasks for different entities (orders, products, customers, etc.)
- Handles logging and error reporting

### Client Layer

#### `client/shopify_client.py`
- Handles authentication with Shopify's GraphQL API
- Manages API requests and response handling
- Provides methods for executing GraphQL queries
- Includes error handling and retry logic

### Extractors Layer

#### `extractors/base.py`
- Abstract base class for extractors
- Defines interface for extraction operations

#### `extractors/bulk_operations.py`
- Implements Shopify's Bulk Operations API
- Handles large data extractions
- Manages operation status monitoring
- Implements data download and verification
- Supports incremental extractions

### Processors Layer

#### `processors/data_processor.py`
- Processes raw JSON data into structured format
- Contains entity-specific processing logic for:
  - Orders
  - Products
  - Customers
  - Inventory
  - Collections
  - Product Metafields
  - Shop Information
- Handles data transformation and cleaning

#### `processors/sync_state.py`
- Tracks synchronization state
- Manages incremental sync timestamps
- Stores sync statistics and status
- Provides methods for state persistence

### Query Definitions

#### `queries/bulk_queries.py`
- Contains GraphQL query definitions for:
  - Orders
  - Products
  - Customers
  - Inventory
  - Collections
  - Product Metafields
  - Shop Information
- Supports pagination through cursor-based navigation

### Data Loading

#### `loaders/gcs_loader.py`
- Handles data loading to Google Cloud Storage
- Manages GCS authentication
- Implements file upload functionality

## Data Flow

1. **Extraction**
   - `ShopifyClient` authenticates with Shopify
   - `BulkOperationsExtractor` initiates data extraction
   - Bulk operation status is monitored
   - Data is downloaded when complete

2. **Processing**
   - Raw JSONL data is processed by `DataProcessor`
   - Entity-specific transformations are applied
   - Data is structured according to defined schemas

3. **State Management**
   - `SyncStateTracker` maintains sync state
   - Tracks successful/failed operations
   - Manages incremental sync timestamps

4. **Loading**
   - Processed data can be loaded to GCS
   - File paths and naming follow defined patterns

## Key Features

- Incremental syncs support
- Bulk operation handling
- Error handling and retries
- State persistence
- Data verification
- GCS integration
- Comprehensive logging

## Data Storage Structure

```
/app/data/
├── raw/          # Raw data from Shopify
├── processed/    # Transformed data
└── state/        # Sync state information
```

## Current Entity Support

- Orders
- Products
- Customers
- Inventory
- Collections
- Product Metafields
- Shop Information

Each entity supports full extraction with specific field selection and transformation logic.

## Current Limitations

1. Fixed batch size of 100 records per query
2. No parallel processing implementation
3. Single destination support (GCS)
4. Basic error recovery mechanism
5. Limited data validation