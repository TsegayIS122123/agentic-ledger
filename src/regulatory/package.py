"""Regulatory Examination Package Generator."""

import json
import hashlib
from datetime import datetime
from typing import Dict, Any, List
from dataclasses import dataclass, asdict

from src.core.event_store import EventStore
from src.projections.compliance_audit import ComplianceAuditProjection
from src.integrity.audit_chain import AuditChain


@dataclass
class RegulatoryPackage:
    """Complete regulatory examination package."""
    application_id: str
    examination_date: str
    event_stream: List[Dict[str, Any]]
    projection_state: Dict[str, Any]
    integrity_verification: Dict[str, Any]
    human_readable_narrative: str
    agent_metadata: List[Dict[str, Any]]
    package_hash: str


class RegulatoryPackageGenerator:
    """
    Generate self-contained regulatory examination packages.
    
    The package can be verified independently without trusting the system.
    """
    
    def __init__(self, store: EventStore, pool):
        self.store = store
        self.pool = pool
        self.compliance = ComplianceAuditProjection(pool)
        self.audit = AuditChain(store)
    
    async def generate_package(
        self,
        application_id: str,
        examination_date: datetime
    ) -> RegulatoryPackage:
        """Generate complete examination package."""
        
        # 1. Get complete event stream
        events = await self.store.load_stream(f"loan-{application_id}")
        event_stream = []
        
        for e in events:
            event_stream.append({
                "event_type": e.event_type,
                "event_version": e.event_version,
                "stream_position": e.stream_position,
                "global_position": e.global_position,
                "payload": e.payload,
                "recorded_at": e.recorded_at.isoformat()
            })
        
        # 2. Get projection state at examination date
        projection_state = await self.compliance.get_compliance_at(
            application_id, examination_date
        )
        
        # 3. Run integrity check
        integrity_result = await self.audit.run_integrity_check("loan", application_id)
        integrity_verification = {
            "chain_valid": integrity_result.chain_valid,
            "events_verified": integrity_result.events_verified,
            "tamper_detected": integrity_result.tamper_detected,
            "checked_at": integrity_result.checked_at.isoformat(),
            "current_hash": integrity_result.current_hash
        }
        
        # 4. Generate human-readable narrative
        narrative = self._generate_narrative(events)
        
        # 5. Extract agent metadata
        agent_metadata = []
        for event in events:
            if event.event_type == "CreditAnalysisCompleted":
                agent_metadata.append({
                    "agent_id": event.payload.get("agent_id"),
                    "model_version": event.payload.get("model_version"),
                    "confidence_score": event.payload.get("confidence_score"),
                    "input_data_hash": event.payload.get("input_data_hash"),
                    "event_type": "CreditAnalysisCompleted",
                    "timestamp": event.recorded_at.isoformat()
                })
            elif event.event_type == "FraudScreeningCompleted":
                agent_metadata.append({
                    "agent_id": event.payload.get("agent_id"),
                    "model_version": event.payload.get("screening_model_version"),
                    "fraud_score": event.payload.get("fraud_score"),
                    "input_data_hash": event.payload.get("input_data_hash"),
                    "event_type": "FraudScreeningCompleted",
                    "timestamp": event.recorded_at.isoformat()
                })
        
        # 6. Calculate package hash for independent verification
        package_data = {
            "application_id": application_id,
            "examination_date": examination_date.isoformat(),
            "event_stream": event_stream,
            "projection_state": projection_state,
            "integrity_verification": integrity_verification,
            "agent_metadata": agent_metadata
        }
        package_hash = hashlib.sha256(
            json.dumps(package_data, sort_keys=True).encode()
        ).hexdigest()
        
        return RegulatoryPackage(
            application_id=application_id,
            examination_date=examination_date.isoformat(),
            event_stream=event_stream,
            projection_state=projection_state,
            integrity_verification=integrity_verification,
            human_readable_narrative=narrative,
            agent_metadata=agent_metadata,
            package_hash=package_hash
        )
    
    def _generate_narrative(self, events: List) -> str:
        """Generate human-readable narrative from events."""
        narrative_lines = []
        
        for event in events:
            if event.event_type == "ApplicationSubmitted":
                narrative_lines.append(
                    f"• Application {event.payload.get('application_id')} submitted "
                    f"for ${event.payload.get('requested_amount_usd'):,} on {event.recorded_at.strftime('%Y-%m-%d %H:%M')}"
                )
            elif event.event_type == "CreditAnalysisCompleted":
                narrative_lines.append(
                    f"• Credit analysis completed with {event.payload.get('risk_tier')} risk tier, "
                    f"confidence {event.payload.get('confidence_score'):.0%}"
                )
            elif event.event_type == "FraudScreeningCompleted":
                narrative_lines.append(
                    f"• Fraud screening completed with score {event.payload.get('fraud_score'):.0%}"
                )
            elif event.event_type == "ComplianceRulePassed":
                narrative_lines.append(
                    f"• Compliance rule {event.payload.get('rule_id')} passed"
                )
            elif event.event_type == "DecisionGenerated":
                narrative_lines.append(
                    f"• AI recommendation: {event.payload.get('recommendation')} "
                    f"(confidence {event.payload.get('confidence_score'):.0%})"
                )
            elif event.event_type == "HumanReviewCompleted":
                narrative_lines.append(
                    f"• Human review completed: {event.payload.get('final_decision')}"
                )
            elif event.event_type == "ApplicationApproved":
                narrative_lines.append(
                    f"• Application APPROVED on {event.recorded_at.strftime('%Y-%m-%d %H:%M')}"
                )
            elif event.event_type == "ApplicationDeclined":
                narrative_lines.append(
                    f"• Application DECLINED on {event.recorded_at.strftime('%Y-%m-%d %H:%M')}"
                )
        
        return "\n".join(narrative_lines)