"""Event upcasters for schema evolution."""

import logging
from datetime import datetime
from typing import Dict, Any, List
from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)

# Create global registry instance
registry = UpcasterRegistry()


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upcast CreditAnalysisCompleted from v1 to v2.
    
    Adds: model_version, confidence_score, regulatory_basis
    """
    # Start with existing v1 fields
    new_payload = {
        "application_id": payload.get("application_id", ""),
        "agent_id": payload.get("agent_id", ""),
        "session_id": payload.get("session_id", ""),
        "risk_tier": payload.get("risk_tier", ""),
        "recommended_limit_usd": payload.get("recommended_limit_usd", 0),
        "analysis_duration_ms": payload.get("analysis_duration_ms", 0),
        "input_data_hash": payload.get("input_data_hash", ""),
    }
    
    # Add v2 fields with inference
    recorded_at = payload.get("_recorded_at")
    new_payload["model_version"] = _infer_model_version(recorded_at)
    new_payload["confidence_score"] = None  # Genuinely unknown
    new_payload["regulatory_basis"] = _infer_regulatory_basis(recorded_at)
    
    return new_payload


def _infer_model_version(recorded_at: Any) -> str:
    """Infer model version from recorded_at timestamp."""
    if not recorded_at:
        return "legacy-unknown"
    
    try:
        if isinstance(recorded_at, str):
            dt = datetime.fromisoformat(recorded_at.replace('Z', '+00:00'))
        else:
            dt = recorded_at
        year = dt.year
    except:
        return "legacy-unknown"
    
    if year < 2026:
        return "legacy-pre-2026"
    elif year == 2026:
        return "v1.0-initial"
    else:
        return f"v{year-2025}.0"


def _infer_regulatory_basis(recorded_at: Any) -> List[str]:
    """Infer active regulations at the time of the event."""
    if not recorded_at:
        return ["BASEL_III"]
    
    try:
        if isinstance(recorded_at, str):
            dt = datetime.fromisoformat(recorded_at.replace('Z', '+00:00'))
        else:
            dt = recorded_at
        year = dt.year
    except:
        return ["BASEL_III"]
    
    if year < 2025:
        return ["BASEL_III_2024", "CCAR_2024"]
    elif year < 2026:
        return ["BASEL_III_2025", "CCAR_2025", "DODD_FRANK_2025"]
    else:
        return ["BASEL_IV_2026", "CCAR_2026", "DODD_FRANK_2026"]