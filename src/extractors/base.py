from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class BaseExtractor(ABC):
    """Base class for all extractors"""
    
    @abstractmethod
    def extract(self, query: str) -> Optional[str]:
        """
        Extract data using the provided query
        
        Args:
            query (str): The query to execute
            
        Returns:
            Optional[str]: URL to the extracted data or None if extraction failed
        """
        pass

    def validate_response(self, response: Dict[str, Any]) -> bool:
        """
        Validate the response from the API
        
        Args:
            response (Dict[str, Any]): The API response to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        return 'data' in response and response.get('data') is not None

    def format_error(self, error: Exception) -> Dict[str, Any]:
        """
        Format error information
        
        Args:
            error (Exception): The error to format
            
        Returns:
            Dict[str, Any]: Formatted error information
        """
        return {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_details': getattr(error, 'details', None)
        }