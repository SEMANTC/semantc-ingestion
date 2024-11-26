# src/queries/bulk_queries.py

GET_ORDERS_QUERY = """
{
  orders {
    edges {
      node {
        id
        name
        createdAt
        currencyCode
        email
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        customer {
          email
        }
        lineItems {
          edges {
            node {
              id
              name
              quantity
              sku
              variant {
                id
                title
                sku
              }
              originalUnitPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              discountedUnitPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              discountedTotalSet {
                shopMoney {
                  amount
                  currencyCode
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

GET_PRODUCTS_QUERY = """
{
  products {
    edges {
      node {
        id
        title
        vendor
        totalInventory
        variants {
          edges {
            node {
              id
              sku
              title
              inventoryQuantity
              price
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
  customers {
    edges {
      node {
        id
        email
        firstName
        lastName
        createdAt
        numberOfOrders
      }
    }
  }
}
"""

GET_COLLECTIONS_QUERY = """
{
  collections {
    edges {
      node {
        id
        title
        handle
        updatedAt
      }
    }
  }
}
"""

GET_PRODUCT_METAFIELDS_QUERY = """
{
  products {
    edges {
      node {
        id
        title
        metafields {
          edges {
            node {
              namespace
              key
              value
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
  shop {
    id
    name
    email
    primaryDomain {
      url
      host
    }
    currencyCode
    currencyFormats {
      moneyFormat
      moneyWithCurrencyFormat
    }
  }
}
"""