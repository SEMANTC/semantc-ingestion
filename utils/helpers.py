import logging
from typing import Dict, Any
from datetime import datetime

def setup_logging() -> logging.Logger:
    """Configure and return logger"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def format_error(error: Exception, context: Dict[str, Any]) -> Dict[str, Any]:
    """Format error with context for logging"""
    return {
        'error': str(error),
        'error_type': type(error).__name__,
        'context': context,
        'timestamp': datetime.now().isoformat()
    }

def ensure_directory(file_path: str) -> None:
    """Ensure directory exists for file path"""
    import os
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)