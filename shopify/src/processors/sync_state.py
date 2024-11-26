# src/processors/sync_state.py
import os
import json
from typing import Optional, Dict, Any
from datetime import datetime

class SyncStateTracker:
    def __init__(self):
        self.state_dir = '/app/data/state'
        os.makedirs(self.state_dir, exist_ok=True)
        self.state_file = os.path.join(self.state_dir, 'sync_state.json')
        self._ensure_state_file()

    def _ensure_state_file(self):
        if not os.path.exists(self.state_file):
            with open(self.state_file, 'w') as f:
                json.dump({}, f)

    def _read_state(self) -> Dict:
        try:
            with open(self.state_file, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def _write_state(self, state: Dict):
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2, default=str)

    def get_last_sync(self, entity: str) -> Optional[datetime]:
        """Get the last successful sync timestamp for an entity"""
        state = self._read_state()
        if entity in state and state[entity].get('last_success'):
            return datetime.fromisoformat(state[entity]['last_success'])
        return None

    def update_sync_state(self, entity: str, status: Dict[str, Any]) -> None:
        """Update sync state with results"""
        state = self._read_state()
        state[entity] = {
            'last_attempt': datetime.utcnow().isoformat(),
            'last_success': datetime.utcnow().isoformat() if status['success'] else None,
            'records_count': status.get('records_count', 0),
            'file_size': status.get('file_size', 0),
            'error': status.get('error'),
            'operation_id': status.get('operation_id')
        }
        self._write_state(state)

    def get_sync_stats(self) -> Dict[str, Any]:
        """Get sync statistics for all entities"""
        return self._read_state()