"""MCP Tools - Command side for AI agents."""

import json
import hashlib
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from src.core.event_store import EventStore
from src.core.exceptions import (
    OptimisticConcurrencyError, 
    DomainError, 
    StreamNotFoundError
)
from src.commands.handlers import (
    SubmitApplicationCommand,
    CreditAnalysisCompletedCommand,
    StartAgentSessionCommand,
    FraudScreeningCompletedCommand,
    ComplianceCheckCommand,
    DecisionGeneratedCommand,
    HumanReviewCompletedCommand,
    handle_submit_application,
    handle_credit_analysis_completed,
    handle_start_agent_session,
    handle_fraud_screening_completed,
    handle_compliance_check,
    handle_generate_decision,
    handle_human_review_completed
)

logger = logging.getLogger(__name__)


class MCPTools:
    """
    MCP Tools - Command interface for AI agents.
    
    Each tool has:
    - Clear description with preconditions for LLM consumption
    - Structured error types with suggested_action
    - Schema validation
    """
    
    def __init__(self, store: EventStore):
        self.store = store
    
    # ========== TOOL 1: start_agent_session ==========
    async def start_agent_session(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Start a new AI agent session.
        
        PRECONDITIONS:
        - This MUST be called before any other agent tools
        - Each session_id must be unique per agent
        - Gas Town pattern: context must be loaded before decisions
        
        Returns: session_id, context_position
        """
        try:
            cmd = StartAgentSessionCommand(
                agent_id=args["agent_id"],
                session_id=args["session_id"],
                context_source=args["context_source"],
                event_replay_from_position=0,
                context_token_count=args.get("token_budget", 8000),
                model_version=args["model_version"]
            )
            await handle_start_agent_session(cmd, self.store)
            
            return {
                "success": True,
                "session_id": args["session_id"],
                "context_position": 1,
                "message": f"Session {args['session_id']} started"
            }
        except DomainError as e:
            return {
                "error_type": "PreconditionFailed",
                "message": str(e),
                "suggested_action": "Use a unique session_id"
            }
    
    # ========== TOOL 2: submit_application ==========
    async def submit_application(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Submit a new loan application.
        
        PRECONDITIONS:
        - application_id must be unique
        - requested_amount_usd must be > 0
        
        Returns: stream_id, initial_version
        """
        try:
            cmd = SubmitApplicationCommand(
                application_id=args["application_id"],
                applicant_id=args["applicant_id"],
                requested_amount_usd=args["requested_amount_usd"],
                loan_purpose=args.get("loan_purpose", "unknown"),
                submission_channel=args.get("submission_channel", "api"),
                submitted_at=datetime.utcnow().isoformat()
            )
            await handle_submit_application(cmd, self.store)
            
            return {
                "success": True,
                "stream_id": f"loan-{args['application_id']}",
                "initial_version": 1
            }
        except DomainError as e:
            return {
                "error_type": "DuplicateApplication",
                "message": str(e),
                "suggested_action": "Use a different application_id"
            }
    
    # ========== TOOL 3: record_credit_analysis ==========
    async def record_credit_analysis(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Record credit analysis results.
        
        PRECONDITIONS:
        - Requires active agent session (call start_agent_session first)
        - Loan application must be in AWAITING_ANALYSIS state
        - Agent must have context loaded
        - confidence_score must be between 0.0 and 1.0
        
        Returns: event_id, new_stream_version
        """
        try:
            input_hash = hashlib.sha256(
                json.dumps(args["input_data"], sort_keys=True).encode()
            ).hexdigest()
            
            cmd = CreditAnalysisCompletedCommand(
                application_id=args["application_id"],
                agent_id=args["agent_id"],
                session_id=args["session_id"],
                model_version=args["model_version"],
                confidence_score=args["confidence_score"],
                risk_tier=args["risk_tier"],
                recommended_limit_usd=args["recommended_limit_usd"],
                duration_ms=args["analysis_duration_ms"],
                input_data=args["input_data"]
            )
            version = await handle_credit_analysis_completed(cmd, self.store)
            
            return {
                "success": True,
                "event_id": f"credit-{args['application_id']}",
                "new_stream_version": version
            }
        except OptimisticConcurrencyError as e:
            return {
                "error_type": "OptimisticConcurrencyError",
                "stream_id": e.stream_id,
                "expected_version": e.expected_version,
                "actual_version": e.actual_version,
                "message": str(e),
                "suggested_action": "reload_stream_and_retry"
            }
        except DomainError as e:
            return {
                "error_type": "PreconditionFailed",
                "message": str(e),
                "suggested_action": "Verify agent session is active and loan is in correct state"
            }
    
    # ========== TOOL 4: record_fraud_screening ==========
    async def record_fraud_screening(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Record fraud screening results.
        
        PRECONDITIONS:
        - Requires active agent session
        - fraud_score must be between 0.0 and 1.0
        
        Returns: event_id, new_stream_version
        """
        try:
            if not 0.0 <= args["fraud_score"] <= 1.0:
                return {
                    "error_type": "ValidationError",
                    "message": "fraud_score must be between 0.0 and 1.0",
                    "suggested_action": "Normalize fraud_score to 0-1 range"
                }
            
            cmd = FraudScreeningCompletedCommand(
                application_id=args["application_id"],
                agent_id=args["agent_id"],
                fraud_score=args["fraud_score"],
                anomaly_flags=args.get("anomaly_flags", []),
                model_version=args["model_version"],
                input_data=args["input_data"]
            )
            version = await handle_fraud_screening_completed(cmd, self.store)
            
            return {
                "success": True,
                "event_id": f"fraud-{args['application_id']}",
                "new_stream_version": version
            }
        except OptimisticConcurrencyError as e:
            return {
                "error_type": "OptimisticConcurrencyError",
                "stream_id": e.stream_id,
                "expected_version": e.expected_version,
                "actual_version": e.actual_version,
                "message": str(e),
                "suggested_action": "reload_stream_and_retry"
            }
        except DomainError as e:
            return {
                "error_type": "PreconditionFailed",
                "message": str(e),
                "suggested_action": "Verify agent session is active"
            }
    
    # ========== TOOL 5: record_compliance_check ==========
    async def record_compliance_check(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Record a compliance rule check result.
        
        PRECONDITIONS:
        - rule_id must exist in active regulation_set_version
        
        Returns: check_id, compliance_status
        """
        try:
            cmd = ComplianceCheckCommand(
                application_id=args["application_id"],
                rule_id=args["rule_id"],
                rule_version=args["rule_version"],
                passed=args["passed"],
                evidence_hash=args.get("evidence_hash", ""),
                regulation_set=args.get("regulation_set", "BASEL_III")
            )
            check_id = await handle_compliance_check(cmd, self.store)
            
            return {
                "success": True,
                "check_id": check_id,
                "compliance_status": "PASSED" if args["passed"] else "FAILED"
            }
        except DomainError as e:
            return {
                "error_type": "ValidationError",
                "message": str(e),
                "suggested_action": "Verify rule_id exists in regulation set"
            }
    
    # ========== TOOL 6: generate_decision ==========
    async def generate_decision(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate final decision.
        
        PRECONDITIONS:
        - All required analyses must be present
        - If confidence_score < 0.6, recommendation forced to REFER
        
        Returns: decision_id, recommendation
        """
        try:
            # Confidence floor enforcement
            recommendation = args["recommendation"]
            if args["confidence_score"] < 0.6 and recommendation != "REFER":
                recommendation = "REFER"
                logger.info(f"Confidence floor triggered: forced REFER")
            
            cmd = DecisionGeneratedCommand(
                application_id=args["application_id"],
                orchestrator_agent_id=args["orchestrator_agent_id"],
                recommendation=recommendation,
                confidence_score=args["confidence_score"],
                contributing_agent_sessions=args["contributing_agent_sessions"],
                decision_basis_summary=args.get("decision_basis_summary", ""),
                model_versions=args.get("model_versions", {})
            )
            version = await handle_generate_decision(cmd, self.store)
            
            return {
                "success": True,
                "decision_id": f"decision-{args['application_id']}",
                "recommendation": recommendation
            }
        except OptimisticConcurrencyError as e:
            return {
                "error_type": "OptimisticConcurrencyError",
                "stream_id": e.stream_id,
                "expected_version": e.expected_version,
                "actual_version": e.actual_version,
                "message": str(e),
                "suggested_action": "reload_stream_and_retry"
            }
        except DomainError as e:
            return {
                "error_type": "PreconditionFailed",
                "message": str(e),
                "suggested_action": "Verify all required analyses are complete"
            }
    
    # ========== TOOL 7: record_human_review ==========
    async def record_human_review(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Record human reviewer decision.
        
        PRECONDITIONS:
        - reviewer_id must be authenticated
        - If override=True, override_reason is required
        
        Returns: final_decision, application_state
        """
        try:
            if args.get("override", False) and not args.get("override_reason"):
                return {
                    "error_type": "ValidationError",
                    "message": "override_reason required when override=True",
                    "suggested_action": "Provide override_reason"
                }
            
            cmd = HumanReviewCompletedCommand(
                application_id=args["application_id"],
                reviewer_id=args["reviewer_id"],
                override=args.get("override", False),
                final_decision=args["final_decision"],
                override_reason=args.get("override_reason")
            )
            version = await handle_human_review_completed(cmd, self.store)
            
            return {
                "success": True,
                "final_decision": args["final_decision"],
                "application_state": "FINAL_APPROVED" if args["final_decision"] == "APPROVE" else "FINAL_DECLINED"
            }
        except OptimisticConcurrencyError as e:
            return {
                "error_type": "OptimisticConcurrencyError",
                "stream_id": e.stream_id,
                "expected_version": e.expected_version,
                "actual_version": e.actual_version,
                "message": str(e),
                "suggested_action": "reload_stream_and_retry"
            }
        except DomainError as e:
            return {
                "error_type": "PreconditionFailed",
                "message": str(e),
                "suggested_action": "Verify loan is in pending decision state"
            }
    
    # ========== TOOL 8: run_integrity_check ==========
    async def run_integrity_check(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run cryptographic integrity check.
        
        PRECONDITIONS:
        - Compliance role required
        - Rate limited to 1/minute per entity
        
        Returns: check_result, chain_valid
        """
        from src.integrity.audit_chain import AuditChain
        
        audit = AuditChain(self.store)
        result = await audit.run_integrity_check(
            entity_type=args["entity_type"],
            entity_id=args["entity_id"]
        )
        
        return {
            "success": True,
            "check_result": {
                "events_verified": result.events_verified,
                "chain_valid": result.chain_valid,
                "tamper_detected": result.tamper_detected,
                "checked_at": result.checked_at.isoformat()
            },
            "chain_valid": result.chain_valid
        }