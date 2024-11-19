from src.client.shopify_client import ShopifyClient
from utils.helpers import setup_logging, format_error
import json
from datetime import datetime
import os

logger = setup_logging()

def fetch_products():
    """Fetch products using GraphQL"""
    try:
        client = ShopifyClient()
        query = """
        {
          products(first: 50) {
            edges {
              node {
                id
                title
                handle
                description
                productType
                vendor
                priceRangeV2 {
                  minVariantPrice {
                    amount
                    currencyCode
                  }
                  maxVariantPrice {
                    amount
                    currencyCode
                  }
                }
                variants(first: 10) {
                  edges {
                    node {
                      id
                      title
                      sku
                      price
                      inventoryQuantity
                    }
                  }
                }
                images(first: 1) {
                  edges {
                    node {
                      url
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        result = client.execute_query(query)
        return result['data']['products']['edges']
        
    except Exception as e:
        error_details = format_error(e, {'operation': 'fetch_products'})
        logger.error(f"Failed to fetch products: {error_details}")
        raise

def save_data(data, resource_type):
    """Save data to JSON file"""
    try:
        # Create data directory in current working directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, 'data')
        
        # Create directory if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            logger.info(f"Created directory: {data_dir}")
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(data_dir, f"{resource_type}_{timestamp}.json")
        
        # Save data
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Data saved to {filename}")
        return filename
        
    except Exception as e:
        error_details = format_error(e, {'operation': 'save_data', 'path': data_dir})
        logger.error(f"Failed to save data: {error_details}")
        raise

def main():
    try:
        # Test connection first
        client = ShopifyClient()
        shop_query = """
        {
          shop {
            name
            email
            myshopifyDomain
          }
        }
        """
        result = client.execute_query(shop_query)
        shop_info = result['data']['shop']
        logger.info(f"Connected to shop: {shop_info['name']}")
        
        # Fetch products
        logger.info("Fetching products...")
        products = fetch_products()
        logger.info(f"Found {len(products)} products")
        
        # Save products
        filename = save_data(products, 'products')
        logger.info(f"Products saved to {filename}")
        
        # Print sample of data
        if products:
            sample = products[0]['node']
            logger.info("\nSample product:")
            logger.info(f"Title: {sample['title']}")
            logger.info(f"ID: {sample['id']}")
            if sample['variants']['edges']:
                logger.info(f"First variant price: {sample['variants']['edges'][0]['node']['price']}")
        
    except Exception as e:
        error_details = format_error(e, {'operation': 'main'})
        logger.error(f"Script failed: {error_details}")
        return False
    
    return True

if __name__ == "__main__":
    main()