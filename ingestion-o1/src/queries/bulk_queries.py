# src/queries/bulk_queries.py

# query to fetch orders
GET_ORDERS_QUERY = """
{
  orders(query: "created_at:>2022-01-01", first: 250) {
    edges {
      node {
        id
        name
        createdAt
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        customer {
          id
          email
          firstName
          lastName
        }
        lineItems(first: 250) {
          edges {
            node {
              id
              title
              quantity
              price
              product {
                id
                title
              }
            }
          }
        }
      }
    }
  }
}
"""

# query to fetch products
GET_PRODUCTS_QUERY = """
{
  products(first: 250) {
    edges {
      node {
        id
        title
        createdAt
        updatedAt
        vendor
        productType
        tags
        variants(first: 250) {
          edges {
            node {
              id
              sku
              title
              price
              inventoryQuantity
            }
          }
        }
      }
    }
  }
}
"""

# query to fetch customers
GET_CUSTOMERS_QUERY = """
{
  customers(first: 250) {
    edges {
      node {
        id
        email
        firstName
        lastName
        createdAt
        ordersCount
        totalSpent
        lastOrder {
          id
          name
          createdAt
        }
      }
    }
  }
}
"""

# query to fetch inventory levels
GET_INVENTORY_LEVELS_QUERY = """
{
  inventoryLevels(first: 250) {
    edges {
      node {
        id
        available
        updatedAt
        location {
          id
          name
        }
        item {
          ... on InventoryItem {
            id
            sku
            tracked
            product {
              id
              title
            }
          }
        }
      }
    }
  }
}
"""

# query to fetch collections
GET_COLLECTIONS_QUERY = """
{
  collections(first: 250) {
    edges {
      node {
        id
        title
        updatedAt
        productsCount
        products(first: 250) {
          edges {
            node {
              id
              title
            }
          }
        }
      }
    }
  }
}
"""

# query to fetch metafields (example for products)
GET_PRODUCT_METAFIELDS_QUERY = """
{
  products(first: 250) {
    edges {
      node {
        id
        title
        metafields(first: 250) {
          edges {
            node {
              namespace
              key
              value
              type
            }
          }
        }
      }
    }
  }
}
"""

# query to fetch shop information
GET_SHOP_INFO_QUERY = """
{
  shop {
    id
    name
    email
    primaryDomain {
      url
    }
    currencyCode
    timezoneAbbreviation
  }
}
"""