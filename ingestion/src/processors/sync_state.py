# src/processors/sync_state.py

from typing import Optional, Dict, Any
from datetime import datetime
import json
import os
from google.cloud import firestore

class SyncStateTracker:
    def __init__(self):
        self.db = firestore.Client()
        self.collection = self.db.collection('shopify_sync_state')
    
    def get_last_sync(self, entity: str) -> Optional[datetime]:
        """Get the last successful sync timestamp for an entity"""
        doc = self.collection.document(entity).get()
        if doc.exists:
            data = doc.to_dict()
            return datetime.fromisoformat(data['last_success'])
        return None
    
    def update_sync_state(self, entity: str, status: Dict[str, Any]) -> None:
        """Update sync state with results"""
        doc_ref = self.collection.document(entity)
        doc_ref.set({
            'last_attempt': datetime.utcnow().isoformat(),
            'last_success': datetime.utcnow().isoformat() if status['success'] else None,
            'records_count': status.get('records_count', 0),
            'file_size': status.get('file_size', 0),
            'error': status.get('error'),
            'operation_id': status.get('operation_id')
        }, merge=True)
    
    def get_sync_stats(self) -> Dict[str, Any]:
        """Get sync statistics for all entities"""
        stats = {}
        for doc in self.collection.stream():
            stats[doc.id] = doc.to_dict()
        return stats