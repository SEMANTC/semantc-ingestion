# src/queries/bulk_queries.py

GET_ORDERS_QUERY = """
{
  orders {
    edges {
      node {
        id
        name
        createdAt
        displayFulfillmentStatus
        displayFinancialStatus
        closed
        closedAt
        currentSubtotalPriceSet {
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
        currentTotalTaxSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        currentTotalDiscountsSet {
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
          state
          taxExempt
          phone
          numberOfOrders
          defaultAddress {
            address1
            address2
            city
            provinceCode
            province
            zip
            country
            countryCode
          }
        }
        shippingAddress {
          address1
          address2
          city
          provinceCode
          province
          zip
          country
          countryCode
          phone
        }
        taxLines {
          priceSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          rate
          title
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
        createdAt
        updatedAt
        publishedAt
        status
        vendor
        productType
        description
        descriptionHtml
        totalInventory
        tracksInventory
        hasOnlyDefaultVariant
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
        tags
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
        phone
        state
        createdAt
        updatedAt
        numberOfOrders
        verifiedEmail
        taxExempt
        tags
        defaultAddress {
          address1
          address2
          city
          provinceCode
          province
          zip
          country
          countryCode
          phone
        }
        note
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
        description
        descriptionHtml
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
      }
    }
  }
}
"""