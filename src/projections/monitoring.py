"""Lag monitoring for projections."""

from typing import Dict
from src.projections.daemon import ProjectionDaemon


class LagMonitor:
    """Exposes projection lag metrics."""
    
    def __init__(self, daemon: ProjectionDaemon):
        self.daemon = daemon
    
    def get_all_lags(self) -> Dict[str, float]:
        """Get lag for all projections in milliseconds."""
        return {
            name: self.daemon.get_lag(name)
            for name in self.daemon.projections.keys()
        }
    
    def check_slos(self) -> Dict[str, str]:
        """Check if projections meet their SLOs."""
        lags = self.get_all_lags()
        
        results = {}
        for name, lag in lags.items():
            if name == "application_summary":
                results[name] = "✅ OK" if lag < 500 else "❌ VIOLATION"
            elif name == "compliance_audit":
                results[name] = "✅ OK" if lag < 2000 else "❌ VIOLATION"
            else:
                results[name] = "✅ OK" if lag < 5000 else "⚠️ WARNING"
        
        return results