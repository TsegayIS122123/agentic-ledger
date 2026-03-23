"""MCP Server for Agentic Ledger."""

import asyncio
import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent, Resource

from src.core.event_store import EventStore
from src.mcp.tools import MCPTools
from src.mcp.resources import MCPResources
from src.utils.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class AgenticLedgerServer:
    """MCP Server for Agentic Ledger."""
    
    def __init__(self, event_store: EventStore, pool):
        self.store = event_store
        self.tools = MCPTools(event_store)
        self.resources = MCPResources(event_store, pool)
        self.server = Server("agentic-ledger")
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup all MCP handlers."""
        
        # ========== LIST TOOLS ==========
        @self.server.list_tools()
        async def handle_list_tools() -> list[Tool]:
            return [
                Tool(
                    name="start_agent_session",
                    description="Start a new AI agent session. PRECONDITION: Must be called before any other tools.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "agent_id": {"type": "string"},
                            "session_id": {"type": "string"},
                            "context_source": {"type": "string"},
                            "model_version": {"type": "string"},
                            "token_budget": {"type": "integer", "default": 8000}
                        },
                        "required": ["agent_id", "session_id", "context_source", "model_version"]
                    }
                ),
                Tool(
                    name="submit_application",
                    description="Submit a new loan application. PRECONDITION: application_id must be unique.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "application_id": {"type": "string"},
                            "applicant_id": {"type": "string"},
                            "requested_amount_usd": {"type": "number"},
                            "loan_purpose": {"type": "string"},
                            "submission_channel": {"type": "string"}
                        },
                        "required": ["application_id", "applicant_id", "requested_amount_usd"]
                    }
                ),
                Tool(
                    name="record_credit_analysis",
                    description="Record credit analysis. PRECONDITION: Requires active agent session, loan must be in AWAITING_ANALYSIS.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "application_id": {"type": "string"},
                            "agent_id": {"type": "string"},
                            "session_id": {"type": "string"},
                            "model_version": {"type": "string"},
                            "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                            "risk_tier": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
                            "recommended_limit_usd": {"type": "number"},
                            "analysis_duration_ms": {"type": "integer"},
                            "input_data": {"type": "object"}
                        },
                        "required": ["application_id", "agent_id", "session_id", "model_version", 
                                    "confidence_score", "risk_tier", "recommended_limit_usd", "input_data"]
                    }
                ),
                Tool(
                    name="record_fraud_screening",
                    description="Record fraud screening. PRECONDITION: fraud_score between 0.0-1.0.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "application_id": {"type": "string"},
                            "agent_id": {"type": "string"},
                            "fraud_score": {"type": "number", "minimum": 0, "maximum": 1},
                            "anomaly_flags": {"type": "array", "items": {"type": "string"}},
                            "model_version": {"type": "string"},
                            "input_data": {"type": "object"}
                        },
                        "required": ["application_id", "agent_id", "fraud_score", "model_version", "input_data"]
                    }
                ),
                Tool(
                    name="record_compliance_check",
                    description="Record compliance rule check.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "application_id": {"type": "string"},
                            "rule_id": {"type": "string"},
                            "rule_version": {"type": "string"},
                            "passed": {"type": "boolean"},
                            "evidence_hash": {"type": "string"}
                        },
                        "required": ["application_id", "rule_id", "rule_version", "passed"]
                    }
                ),
                Tool(
                    name="generate_decision",
                    description="Generate final decision. If confidence < 0.6, forces REFER.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "application_id": {"type": "string"},
                            "orchestrator_agent_id": {"type": "string"},
                            "recommendation": {"type": "string", "enum": ["APPROVE", "DECLINE", "REFER"]},
                            "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                            "contributing_agent_sessions": {"type": "array", "items": {"type": "string"}},
                            "decision_basis_summary": {"type": "string"}
                        },
                        "required": ["application_id", "orchestrator_agent_id", "recommendation", 
                                    "confidence_score", "contributing_agent_sessions"]
                    }
                ),
                Tool(
                    name="record_human_review",
                    description="Record human review. If override=True, override_reason required.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "application_id": {"type": "string"},
                            "reviewer_id": {"type": "string"},
                            "override": {"type": "boolean"},
                            "final_decision": {"type": "string", "enum": ["APPROVE", "DECLINE"]},
                            "override_reason": {"type": "string"}
                        },
                        "required": ["application_id", "reviewer_id", "override", "final_decision"]
                    }
                ),
                Tool(
                    name="run_integrity_check",
                    description="Run cryptographic integrity check. Rate limited to 1/minute.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "entity_type": {"type": "string"},
                            "entity_id": {"type": "string"}
                        },
                        "required": ["entity_type", "entity_id"]
                    }
                )
            ]
        
        # ========== CALL TOOL ==========
        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict) -> list[TextContent]:
            tool_map = {
                "start_agent_session": self.tools.start_agent_session,
                "submit_application": self.tools.submit_application,
                "record_credit_analysis": self.tools.record_credit_analysis,
                "record_fraud_screening": self.tools.record_fraud_screening,
                "record_compliance_check": self.tools.record_compliance_check,
                "generate_decision": self.tools.generate_decision,
                "record_human_review": self.tools.record_human_review,
                "run_integrity_check": self.tools.run_integrity_check,
            }
            
            handler = tool_map.get(name)
            if not handler:
                return [TextContent(type="text", text=json.dumps({
                    "error_type": "UnknownTool",
                    "message": f"Unknown tool: {name}"
                }))]
            
            result = await handler(arguments or {})
            return [TextContent(type="text", text=json.dumps(result))]
        
        # ========== LIST RESOURCES ==========
        @self.server.list_resources()
        async def handle_list_resources() -> list[Resource]:
            return [
                Resource(uri="ledger://applications/{id}", name="Application Summary", 
                        description="Current state of a loan application", mimeType="application/json"),
                Resource(uri="ledger://applications/{id}/compliance", name="Compliance Audit", 
                        description="Compliance history with temporal queries", mimeType="application/json"),
                Resource(uri="ledger://applications/{id}/audit-trail", name="Audit Trail", 
                        description="Complete event history", mimeType="application/json"),
                Resource(uri="ledger://agents/{id}/performance", name="Agent Performance", 
                        description="Metrics per agent model version", mimeType="application/json"),
                Resource(uri="ledger://agents/{id}/sessions/{session_id}", name="Agent Session", 
                        description="Full agent session history", mimeType="application/json"),
                Resource(uri="ledger://ledger/health", name="Ledger Health", 
                        description="System health metrics", mimeType="application/json")
            ]
        
        # ========== READ RESOURCE ==========
        @self.server.read_resource()
        async def handle_read_resource(uri: str) -> str:
            parts = uri.split("/")
            
            if "applications" in uri and "compliance" not in uri and "audit-trail" not in uri:
                app_id = parts[2] if len(parts) > 2 else None
                if app_id:
                    return json.dumps(await self.resources.get_application_summary(app_id))
            
            elif "compliance" in uri:
                app_id = parts[2] if len(parts) > 2 else None
                as_of = None
                if "?" in uri:
                    query = uri.split("?")[1]
                    if "as_of=" in query:
                        as_of = query.split("as_of=")[1].split("&")[0]
                if app_id:
                    return json.dumps(await self.resources.get_compliance_view(app_id, as_of))
            
            elif "audit-trail" in uri:
                app_id = parts[2] if len(parts) > 2 else None
                if app_id:
                    return json.dumps(await self.resources.get_audit_trail(app_id))
            
            elif "performance" in uri:
                agent_id = parts[2] if len(parts) > 2 else None
                if agent_id:
                    return json.dumps(await self.resources.get_agent_performance(agent_id))
            
            elif "sessions" in uri:
                agent_id = parts[2] if len(parts) > 2 else None
                session_id = parts[4] if len(parts) > 4 else None
                if agent_id and session_id:
                    return json.dumps(await self.resources.get_agent_session(agent_id, session_id))
            
            elif uri == "ledger://ledger/health":
                return json.dumps(await self.resources.get_health())
            
            return json.dumps({"error": "Resource not found"})


async def main():
    """Run the MCP server."""
    import asyncpg
    
    pool = await asyncpg.create_pool(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
        min_size=1,
        max_size=5
    )
    
    store = EventStore(pool)
    server = AgenticLedgerServer(store, pool)
    
    async with stdio_server() as (read_stream, write_stream):
        await server.server.run(
            read_stream,
            write_stream,
            server.server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())