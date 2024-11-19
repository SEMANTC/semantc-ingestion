class BulkQueries:
    """Collection of GraphQL queries for Shopify Bulk Operations"""
    
    PRODUCTS = """
    {
      products {
        edges {
          node {
            id
            handle
            title
            description
            productType
            status
            vendor
            createdAt
            updatedAt
            publishedAt
            variants {
              edges {
                node {
                  id
                  title
                  sku
                  price
                  compareAtPrice
                  inventoryQuantity
                  inventoryPolicy
                  fulfillmentService
                  weight
                  weightUnit
                  requiresShipping
                  taxable
                }
              }
            }
            options {
              id
              name
              position
              values
            }
            images {
              edges {
                node {
                  id
                  src
                  altText
                  width
                  height
                }
              }
            }
            metafields {
              edges {
                node {
                  id
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

    ORDERS = """
    {
      orders {
        edges {
          node {
            id
            name
            email
            phone
            totalPrice
            subtotalPrice
            totalTax
            totalDiscounts
            createdAt
            updatedAt
            processedAt
            cancelledAt
            cancelReason
            fulfillmentStatus
            financialStatus
            tags
            note
            customer {
              id
              email
              firstName
              lastName
              ordersCount
              totalSpent
              phone
            }
            shippingAddress {
              address1
              address2
              city
              province
              provinceCode
              country
              countryCode
              zip
              phone
            }
            lineItems {
              edges {
                node {
                  id
                  title
                  quantity
                  sku
                  vendor
                  fulfillmentStatus
                  price
                  totalPrice
                  variant {
                    id
                    title
                    sku
                    price
                  }
                }
              }
            }
            fulfillments {
              id
              status
              createdAt
              updatedAt
              trackingCompany
              trackingNumber
              trackingUrl
            }
            transactions {
              id
              status
              kind
              amount
              createdAt
            }
          }
        }
      }
    }
    """

    CUSTOMERS = """
    {
      customers {
        edges {
          node {
            id
            email
            firstName
            lastName
            phone
            state
            createdAt
            updatedAt
            ordersCount
            totalSpent
            lastOrderId
            note
            verifiedEmail
            tags
            addresses {
              edges {
                node {
                  id
                  address1
                  address2
                  city
                  province
                  provinceCode
                  country
                  countryCode
                  zip
                  phone
                }
              }
            }
            defaultAddress {
              id
              address1
              address2
              city
              province
              zip
              country
            }
            metafields {
              edges {
                node {
                  id
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

    INVENTORY = """
    {
      inventoryItems {
        edges {
          node {
            id
            sku
            createdAt
            updatedAt
            tracked
            unitCost {
              amount
              currencyCode
            }
            inventoryLevels {
              edges {
                node {
                  id
                  available
                  location {
                    id
                    name
                    address {
                      address1
                      city
                      province
                      country
                      zip
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