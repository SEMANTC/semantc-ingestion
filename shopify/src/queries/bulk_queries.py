GET_ORDERS_QUERY = """
{
  orders {
    edges {
      node {
        id
        name
        createdAt
        processedAt
        currencyCode
        email
        displayFinancialStatus
        displayFulfillmentStatus
        cancelledAt
        closedAt
        
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        
        currentTotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        
        subtotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        
        totalShippingPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        
        totalDiscountsSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        
        totalTaxSet {
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
          numberOfOrders
          defaultAddress {
            address1
            city
            province
            country
          }
        }
        
        lineItems {
          edges {
            node {
              id
              name
              quantity
              sku
              refundableQuantity
              currentQuantity
              variant {
                id
                title
                sku
                inventoryQuantity
                price
              }
            }
          }
        }
        
        transactions {
          id
          processedAt
          status
          kind
          gateway
          amountSet {
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
"""

GET_PRODUCTS_QUERY = """
{
  products {
    edges {
      node {
        id
        title
        handle
        productType
        vendor
        createdAt
        updatedAt
        publishedAt
        status
        totalInventory
        tracksInventory
        
        variants {
          edges {
            node {
              id
              title
              sku
              price
              compareAtPrice
              inventoryQuantity
              sellableOnlineQuantity
              
              inventoryItem {
                id
                tracked
                unitCost {
                  amount
                  currencyCode
                }
              }
              
              selectedOptions {
                name
                value
              }
            }
          }
        }
        
        options {
          id
          name
          position
          values
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
        firstName
        lastName
        email
        phone
        createdAt
        updatedAt
        numberOfOrders
        
        amountSpent {
          amount
          currencyCode
        }
        
        addresses {
          address1
          city
          province
          country
          phone
        }
        
        defaultAddress {
          address1
          city
          province
          country
          phone
        }
        
        emailMarketingConsent {
          marketingState
          marketingOptInLevel
          consentUpdatedAt
        }
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
        productsCount
        sortOrder
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
              type
            }
          }
        }
      }
    }
  }
}
"""