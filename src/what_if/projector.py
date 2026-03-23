"""What-If Projector - Counterfactual event simulation."""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from copy import deepcopy

from src.core.event_store import EventStore
from src.models.events import BaseEvent, StoredEvent
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditProjection

logger = logging.getLogger(__name__)


@dataclass
class WhatIfResult:
    """Result of a what-if analysis."""
    real_outcome: Dict[str, Any]
    counterfactual_outcome: Dict[str, Any]
    divergence_events: List[str]
    branch_point: str
    counterfactual_applied: bool


class WhatIfProjector:
    """
    What-If Projector - Run counterfactual scenarios without modifying real data.
    
    Allows compliance teams to ask questions like:
    "What would have happened if we used a different risk model?"
    """
    
    def __init__(self, store: EventStore, pool):
        self.store = store
        self.pool = pool
        self.app_summary = ApplicationSummaryProjection(pool)
        self.compliance = ComplianceAuditProjection(pool)
    
    async def run_what_if(
        self,
        application_id: str,
        branch_at_event_type: str,
        counterfactual_events: List[BaseEvent],
        projections: List[str] = None
    ) -> WhatIfResult:
        """
        Run a counterfactual scenario.
        
        1. Load all events up to branch point
        2. Inject counterfactual events instead of real ones
        3. Continue with causally independent events
        4. Apply to projections
        """
        # Load all real events
        real_events = await self.store.load_stream(f"loan-{application_id}")
        
        # Find branch point
        branch_index = None
        for i, event in enumerate(real_events):
            if event.event_type == branch_at_event_type:
                branch_index = i
                break
        
        if branch_index is None:
            return WhatIfResult(
                real_outcome={},
                counterfactual_outcome={},
                divergence_events=[],
                branch_point=branch_at_event_type,
                counterfactual_applied=False
            )
        
        # Split events
        pre_branch = real_events[:branch_index]
        real_branch_event = real_events[branch_index]
        post_branch = real_events[branch_index + 1:]
        
        # Create counterfactual event stream
        counterfactual_stream = []
        
        # Add pre-branch events
        counterfactual_stream.extend(pre_branch)
        
        # Add counterfactual events
        for cf_event in counterfactual_events:
            # Create a StoredEvent wrapper for counterfactual
            cf_stored = StoredEvent(
                event_id=None,
                stream_id=f"loan-{application_id}",
                stream_position=0,
                global_position=0,
                event_type=cf_event.event_type,
                event_version=cf_event.event_version,
                payload=cf_event.to_dict(),
                metadata={"counterfactual": True, "replaces": real_branch_event.event_id},
                recorded_at=real_branch_event.recorded_at
            )
            counterfactual_stream.append(cf_stored)
        
        # Determine causally dependent events (events that reference the branch)
        dependent_events = []
        independent_events = []
        
        for event in post_branch:
            # Check if causation_id traces back to branch event
            causation_id = event.metadata.get("causation_id")
            if causation_id and str(real_branch_event.event_id) == causation_id:
                dependent_events.append(event)
            else:
                independent_events.append(event)
        
        # Add independent events to counterfactual stream
        counterfactual_stream.extend(independent_events)
        
        # Build real outcome from actual projection
        real_outcome = await self.app_summary.get_current(application_id)
        
        # Build counterfactual outcome by processing events
        counterfactual_outcome = await self._apply_events_to_projection(
            counterfactual_stream, application_id
        )
        
        divergence_events = [e.event_type for e in dependent_events]
        
        return WhatIfResult(
            real_outcome=real_outcome,
            counterfactual_outcome=counterfactual_outcome,
            divergence_events=divergence_events,
            branch_point=branch_at_event_type,
            counterfactual_applied=True
        )
    
    async def _apply_events_to_projection(
        self, 
        events: List[StoredEvent], 
        application_id: str
    ) -> Dict[str, Any]:
        """Apply a list of events to the application summary projection."""
        # Start with empty state
        state = {}
        
        for event in events:
            if event.event_type == "ApplicationSubmitted":
                state = {
                    "application_id": application_id,
                    "state": "SUBMITTED",
                    "applicant_id": event.payload.get("applicant_id"),
                    "requested_amount_usd": event.payload.get("requested_amount_usd")
                }
            elif event.event_type == "CreditAnalysisCompleted":
                state["risk_tier"] = event.payload.get("risk_tier")
                state["state"] = "ANALYSIS_COMPLETE"
            elif event.event_type == "DecisionGenerated":
                state["decision"] = event.payload.get("recommendation")
                state["state"] = "PENDING_DECISION"
            elif event.event_type == "ApplicationApproved":
                state["state"] = "FINAL_APPROVED"
            elif event.event_type == "ApplicationDeclined":
                state["state"] = "FINAL_DECLINED"
        
        return state