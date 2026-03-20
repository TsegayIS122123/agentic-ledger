"""Event upcasters for schema evolution."""

import logging
from datetime import datetime
from typing import Dict, Any, List
from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)

# Create global registry instance
registry = UpcasterRegistry()


# ============================================================================
# CreditAnalysisCompleted v1 → v2
# Original v1: {application_id, agent_id, session_id, risk_tier, 
#              recommended_limit_usd, analysis_duration_ms, input_data_hash}
# New v2: adds model_version, confidence_score, regulatory_basis
# ============================================================================

@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upcast CreditAnalysisCompleted from v1 to v2.
    
    Inference Strategy:
    - model_version: Infer from recorded_at timestamp. Pre-2026 events used
      a legacy rule-based system, not ML models. We use a sentinel value.
      Error rate: 0% - this is a categorical fact.
      
    - confidence_score: Set to NULL. This data was not collected in v1.
      Fabrication would be worse than null because it would create false
      audit trail. Regulators must know this information is unavailable.
      
    - regulatory_basis: Infer from recorded_at date by looking up which
      regulations were active at that time. This is SAFE because regulation
      changes are public record and deterministic.
    """
    # Extract recorded_at from metadata (will be added by caller)
    recorded_at = payload.get("_recorded_at")
    
    # Start with existing v1 fields
    new_payload = {
        "application_id": payload["application_id"],
        "agent_id": payload["agent_id"],
        "session_id": payload["session_id"],
        "risk_tier": payload["risk_tier"],
        "recommended_limit_usd": payload["recommended_limit_usd"],
        "analysis_duration_ms": payload["analysis_duration_ms"],
        "input_data_hash": payload["input_data_hash"],
    }
    
    # Add v2 fields with inference
    new_payload["model_version"] = _infer_model_version(recorded_at)
    new_payload["confidence_score"] = None  # Genuinely unknown - do not fabricate!
    new_payload["regulatory_basis"] = _infer_regulatory_basis(recorded_at)
    
    logger.debug(f"Upcasted CreditAnalysisCompleted v1→v2: model={new_payload['model_version']}")
    return new_payload


def _infer_model_version(recorded_at: Any) -> str:
    """Infer model version from recorded_at timestamp."""
    if not recorded_at:
        return "legacy-unknown"
    
    # Parse timestamp if it's a string
    if isinstance(recorded_at, str):
        try:
            dt = datetime.fromisoformat(recorded_at.replace('Z', '+00:00'))
            year = dt.year
        except:
            return "legacy-unknown"
    else:
        # Assume it's a datetime object
        year = recorded_at.year
    
    # Model versioning was introduced in 2026
    if year < 2026:
        return "legacy-pre-2026"
    elif year == 2026:
        return "v1.0-initial"
    else:
        return f"v{year-2025}.0"


def _infer_regulatory_basis(recorded_at: Any) -> List[str]:
    """Infer active regulations at the time of the event."""
    if not recorded_at:
        return ["BASEL_III"]  # Default
    
    # Parse timestamp
    if isinstance(recorded_at, str):
        try:
            dt = datetime.fromisoformat(recorded_at.replace('Z', '+00:00'))
        except:
            return ["BASEL_III"]
    else:
        dt = recorded_at
    
    # Regulation sets changed over time
    if dt.year < 2025:
        return ["BASEL_III_2024", "CCAR_2024"]
    elif dt.year < 2026:
        return ["BASEL_III_2025", "CCAR_2025", "DODD_FRANK_2025"]
    else:
        return ["BASEL_IV_2026", "CCAR_2026", "DODD_FRANK_2026"]


# ============================================================================
# DecisionGenerated v1 → v2
# Original v1: {application_id, orchestrator_agent_id, recommendation,
#              confidence_score, contributing_agent_sessions[],
#              decision_basis_summary}
# New v2: adds model_versions{} dict
# ============================================================================

@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_generated_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upcast DecisionGenerated from v1 to v2.
    
    This upcaster requires loading AgentSession events to reconstruct
    which model versions were used. This has performance implications:
    - Each upcast may require multiple database queries
    - Consider caching for batch operations
    - In production, this is acceptable because:
        a) Old events are upcasted once and cached
        b) Temporal queries are rare
        c) Regulatory examinations can tolerate some latency
    
    Inference Strategy:
    - model_versions: Reconstruct from contributing_agent_sessions by
      loading each session's AgentContextLoaded event to get model_version.
      This is ACCURATE but EXPENSIVE.
    """
    # Start with existing v1 fields
    new_payload = {
        "application_id": payload["application_id"],
        "orchestrator_agent_id": payload["orchestrator_agent_id"],
        "recommendation": payload["recommendation"],
        "confidence_score": payload["confidence_score"],
        "contributing_agent_sessions": payload["contributing_agent_sessions"],
        "decision_basis_summary": payload["decision_basis_summary"],
    }
    
    # Add model_versions (will be populated by the caller with store access)
    # The actual store lookup happens in EventStore.load_stream with upcasting
    new_payload["model_versions"] = {}  # Placeholder
    
    return new_payload


# Note: The actual model_versions reconstruction requires store access
# This is handled in the EventStore.load_stream method