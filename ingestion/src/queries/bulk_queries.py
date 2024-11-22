# src/queries/bulk_queries.py
GET_ORDERS_QUERY = """
{
  orders(first: 100) {
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
        lineItems {
          edges {
            node {
              id
              title
              quantity
              originalUnitPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
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

GET_PRODUCTS_QUERY = """
{
  products(first: 100) {
    edges {
      node {
        id
        title
        createdAt
        updatedAt
        vendor
        productType
        variants {
          edges {
            node {
              id
              sku
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

GET_CUSTOMERS_QUERY = """
{
  customers(first: 100) {
    edges {
      node {
        id
        email
        firstName
        lastName
        createdAt
        orders(first: 1) {
          edges {
            node {
              id
              createdAt
            }
          }
        }
      }
    }
  }
}
"""

GET_INVENTORY_QUERY = """
{
  products(first: 100) {
    edges {
      node {
        id
        title
        variants {
          edges {
            node {
              id
              inventoryItem {
                id
                tracked
                inventoryLevels(first: 10) {
                  edges {
                    node {
                      available
                      location {
                        id
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

GET_COLLECTIONS_QUERY = """
{
  collections(first: 100) {
    edges {
      node {
        id
        title
        updatedAt
        products(first: 100) {
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

GET_PRODUCT_METAFIELDS_QUERY = """
{
  products(first: 100) {
    edges {
      node {
        id
        title
        metafields(first: 100) {
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

GET_SHOP_INFO_QUERY = """
{
  products(first: 1) {
    edges {
      node {
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
    }
  }
}
"""