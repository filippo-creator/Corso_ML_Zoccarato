from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, TypedDict
from argparse import ArgumentParser
from loguru import logger

from dotenv import find_dotenv, load_dotenv

# Load environment variables early
load_dotenv(find_dotenv())

# Optional LangChain imports - gracefully degrade if not available
LANGCHAIN_AVAILABLE = False
try:
    from langchain.agents import create_agent
    from langchain_core.language_models import BaseChatModel
    from langchain_core.messages import AIMessage, HumanMessage
    from langchain_core.tools import StructuredTool
    from langchain_google_genai import ChatGoogleGenerativeAI
    from pydantic import BaseModel, Field
    
    LANGCHAIN_AVAILABLE = True
    logger.info("LangChain dependencies loaded successfully")
except ImportError as e:
    logger.warning(f"LangChain not available: {e}")


# ============================================================================
# PART 1: Environment Verification
# ============================================================================


class EnvironmentCheckResult(TypedDict):
    """Result of an environment verification check."""
    name: str
    status: str  # "PASS", "FAIL", "WARN"
    message: str
    details: dict[str, Any]


def verify_imports() -> EnvironmentCheckResult:
    """
    Verify that all critical imports are available.
    
    Returns:
        EnvironmentCheckResult with import verification status
    """
    logger.info("Verifying imports...")
    
    required_modules = {
        "langchain": "langchain",
        "langchain_core": "langchain_core",
        "langgraph": "langgraph",
        "langchain_google_genai": "langchain_google_genai",
        "pydantic": "pydantic",
        "dotenv": "dotenv",
    }
    
    failed_modules = []
    available_modules = []
    
    for display_name, module_name in required_modules.items():
        try:
            __import__(module_name)
            available_modules.append(display_name)
            logger.debug(f"  [OK] {display_name}")
        except ImportError as e:
            failed_modules.append((display_name, str(e)))
            logger.debug(f"  [FAIL] {display_name}: {e}")
    
    status = "PASS" if not failed_modules else "FAIL"
    message = "All imports available" if status == "PASS" else f"{len(failed_modules)} import(s) failed"
    
    result: EnvironmentCheckResult = {
        "name": "imports",
        "status": status,
        "message": message,
        "details": {
            "available": available_modules,
            "failed": failed_modules,
        }
    }
    
    logger.info(f"Import verification: {status} - {message}")
    return result


def verify_api_keys() -> EnvironmentCheckResult:
    """
    Verify that required API keys are configured.
    
    Checks:
    - GOOGLE_API_KEY or GEMINI_API_KEY (required for Gemini)
    - LANGSMITH_API_KEY (optional for tracing)
    
    Returns:
        EnvironmentCheckResult with API key verification status
    """
    logger.info("Verifying API keys...")
    
    has_gemini_key = bool(os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"))
    has_langsmith_key = bool(os.getenv("LANGSMITH_API_KEY"))
    
    if not has_gemini_key:
        status = "FAIL"
        message = "Gemini API key not found (set GOOGLE_API_KEY or GEMINI_API_KEY)"
    elif not has_langsmith_key:
        status = "WARN"
        message = "Gemini key found; LangSmith key missing (tracing disabled)"
    else:
        status = "PASS"
        message = "Both Gemini and LangSmith keys configured"
    
    logger.info(f"API key verification: {status} - {message}")
    
    return {
        "name": "api_keys",
        "status": status,
        "message": message,
        "details": {
            "gemini_key_present": has_gemini_key,
            "langsmith_key_present": has_langsmith_key,
        }
    }


def verify_campaign_paths() -> EnvironmentCheckResult:
    """
    Verify that Campaign 1 directory structure exists.
    
    Checks:
    - Starter Kit directory
    - Campaign 1 subdirectories
    - Key fixture files
    
    Returns:
        EnvironmentCheckResult with path verification status
    """
    logger.info("Verifying Campaign 1 paths...")
    
    base_path = Path("Starter Kit – Agentic Models/Campaign 1")
    logger.info(f"Base path: {base_path}")
    required_dirs = {
        "fixtures": base_path / "01_pre_postprocessing" / "input_from_api",
        "config": base_path / "02_postprocessing",
        "output": base_path / "03_post_postprocessing" / "output_json",
    }
    
    missing_dirs = []
    existing_dirs = []
    
    for dir_name, dir_path in required_dirs.items():
        if dir_path.exists():
            existing_dirs.append(dir_name)
            logger.debug(f"  [OK] {dir_name}: {dir_path}")
        else:
            missing_dirs.append((dir_name, str(dir_path)))
            logger.debug(f"  [FAIL] {dir_name} missing: {dir_path}")
    
    # Check for key fixture files
    fixtures_dir = required_dirs["fixtures"]
    required_fixtures = [
        "impacts_in_target.json",
        "tv_spot_schedule.json",
        "target_universe.json",
    ]
    
    present_fixtures = []
    missing_fixtures = []
    
    if fixtures_dir.exists():
        for fixture in required_fixtures:
            fixture_path = fixtures_dir / fixture
            if fixture_path.exists():
                present_fixtures.append(fixture)
            else:
                missing_fixtures.append(fixture)
    
    status = "PASS" if not missing_dirs and not missing_fixtures else "FAIL"
    message = "Campaign 1 structure verified" if status == "PASS" else "Some paths missing"
    
    logger.info(f"Path verification: {status} - {message}")
    
    return {
        "name": "campaign_paths",
        "status": status,
        "message": message,
        "details": {
            "existing_dirs": existing_dirs,
            "missing_dirs": missing_dirs,
            "present_fixtures": present_fixtures,
            "missing_fixtures": missing_fixtures,
        }
    }


def run_environment_checks() -> dict[str, EnvironmentCheckResult]:
    """
    Run all environment verification checks.
    
    This is the main entry point for environment validation.
    
    Returns:
        Dictionary mapping check names to results
    """
    logger.info("=" * 70)
    logger.info("RUNNING ENVIRONMENT VERIFICATION CHECKS")
    logger.info("=" * 70)
    
    results = {
        "imports": verify_imports(),
        "api_keys": verify_api_keys(),
        "campaign_paths": verify_campaign_paths(),
    }
    
    # Summary
    logger.info("=" * 70)
    logger.info("VERIFICATION SUMMARY")
    logger.info("=" * 70)
    for check_name, result in results.items():
        status_symbol = "✓" if result["status"] == "PASS" else "⚠" if result["status"] == "WARN" else "✗"
        logger.info(f"{status_symbol} {check_name.upper()}: {result['message']}")
    
    return results


# ============================================================================
# PART 2: Campaign Data Tools
# ============================================================================


def list_available_fixtures(campaign_dir: Path) -> dict[str, Any]:
    """
    List all available fixture files from a campaign directory.
    
    Args:
        campaign_dir: Path to campaign root directory
        
    Returns:
        Dictionary with available fixtures and their paths
    """
    logger.info(f"Listing fixtures for: {campaign_dir.name}")
    
    if not campaign_dir.exists():
        logger.error(f"Campaign directory not found: {campaign_dir}")
        raise OSError(f"Campaign directory not found: {campaign_dir}")
    
    # Direct path construction (no CampaignPaths import)
    input_dir = campaign_dir / "01_pre_postprocessing" / "input_from_api"
    
    if not input_dir.exists():
        logger.warning(f"Fixture directory not found: {input_dir}")
        return {
            "campaign": campaign_dir.name,
            "available_fixtures": [],
            "fixture_paths": {},
            "count": 0,
        }
    
    available_files = {}
    for file_path in sorted(input_dir.glob("*.json")):
        available_files[file_path.stem] = str(file_path)
        logger.debug(f"  Found: {file_path.stem}")
    
    result = {
        "campaign": campaign_dir.name,
        "available_fixtures": list(available_files.keys()),
        "fixture_paths": available_files,
        "count": len(available_files),
    }
    
    logger.info(f"Found {len(available_files)} fixtures")
    return result


def filter_tables_by_allowlist(table_names: list[str]) -> dict[str, Any]:
    """

    Allowlist rules:
    - INCLUDE: TRP tables (keys containing 'trp')
    - INCLUDE: TabSummary tables (keys containing 'tabsummary')
    - EXCLUDE: Plot elements (keys containing 'plot')
    - EXCLUDE: 30-second equivalents (keys containing '30eq')
    
    Args:
        table_names: List of table element keys to filter
        
    Returns:
        Dictionary with allowed/rejected tables and statistics
    """
    logger.info(f"Filtering {len(table_names)} tables...")
    
    allowed_tables = []
    rejected_tables = []
    
    for table_name in table_names:
        table_lower = table_name.lower()
        rejection_reason = None
        
        # Check exclusion rules FIRST
        if "plot" in table_lower or ".30eq" in table_lower or "30eq" in table_lower:
            if "plot" in table_lower:
                rejection_reason = "excluded: contains 'plot'"
            else:
                rejection_reason = "excluded: contains '30eq'"
        
        # Check inclusion rules if not rejected
        elif "trp" in table_lower or "tabsummary" in table_lower:
            allowed_tables.append(table_name)
            logger.debug(f"  [ALLOWED] {table_name}")
        else:
            # Other table types allowed by default
            allowed_tables.append(table_name)
            logger.debug(f"  [ALLOWED] {table_name}")
        
        # Record rejections
        if rejection_reason:
            rejected_tables.append({
                "name": table_name,
                "reason": rejection_reason,
            })
            logger.debug(f"  [REJECTED] {table_name}: {rejection_reason}")
    
    result = {
        "allowed_tables": allowed_tables,
        "rejected_tables": rejected_tables,
        "allowlist_rules": [
            "INCLUDE: TRP tables (contain 'trp')",
            "INCLUDE: TabSummary tables (contain 'tabsummary')",
            "EXCLUDE: Plot elements (contain 'plot')",
            "EXCLUDE: 30eq elements (contain '30eq')",
        ],
        "statistics": {
            "total_input": len(table_names),
            "allowed_count": len(allowed_tables),
            "rejected_count": len(rejected_tables),
            "allowed_percentage": round(100 * len(allowed_tables) / len(table_names), 1) if table_names else 0,
        }
    }
    
    logger.info(f"Filtering complete: {len(allowed_tables)} allowed, {len(rejected_tables)} rejected")
    return result


# ============================================================================
# PART 3: Agent Creation (Optional - requires LangChain)
# ============================================================================


# dataBreeders API Knowledge Base (injected into agent prompt)
DATABREEDERS_API_KNOWLEDGE = """
# dataBreeders API Knowledge Base

## API Request Structure
The dataBreeders API uses a JSON request format with the following structure:

```json
{
  "name_campaign": "Campaign 1",
  "target": [
    {
      "name_target": "W25-54",
      "filter": [
        {"age_break": "25_34", "sex": "F"},
        {"age_break": "35_44", "sex": "F"},
        {"age_break": "45_54", "sex": "F"}
      ]
    }
  ],
  "sg_code": [
    {"id": "sg412233", "period_start": "2024-08-11", "period_end": "2024-08-25"}
  ],
  "filter": {
    "broadcaster": [],
    "channel": [],
    "device_type": [],
    "online_video": ["is_platform"]
  }
}
```

## Available Data Types (API responses)
1. **json_request.json** - Contains campaign request metadata
2. **impacts_by_sex_age.json** - Campaign results by sex and age_break
3. **impacts_in_target.json** - Campaign results by target audience
4. **r1plus_in_target_buildup.json** - Cumulative reach build-up over time
5. **rf_in_target_overall.json** - Reach and frequency distribution
6. **target_universe.json** - Total target population data
7. **tv_spot_schedule.json** - Spot scheduling information
8. **universe_by_sex_age.json** - Universe breakdown by demographics

## Target Audience Specifications
- **Age breaks**: 03_14, 15_17, 18_24, 25_34, 35_44, 45_54, 55_64, 65_plus
- **Sex**: M (Men), F (Women), or both
- **Common targets**:
  - W25-54 (Women 25-54): age_break 25_34, 35_44, 45_54 with sex=F
  - M25-54 (Men 25-54): age_break 25_34, 35_44, 45_54 with sex=M
  - A3+ (All 3+): all age breaks

## Key Metrics
- **Impacts**: Impression-related metric (one impression = one view)
- **Reach**: Percentage of individuals reached during a period
- **Frequency**: How many times target watched campaign (usually up to 20)
- **TRP (Target Rating Point)**: Metric for first level display
- **Contacts**: Similar to impacts metric

## User Request Mapping Examples

User says: "I need TRP data for women 25-54"
API call should request:
- data_types: ["impacts_in_target"]
- target_audience: "W25-54"
- This returns tables with TRP values

User says: "Get reach and frequency for Campaign 1"
API call should request:
- data_types: ["rf_in_target_overall", "r1plus_in_target_buildup"]
- campaign: "Campaign 1"

User says: "I need contact information by demographics"
API call should request:
- data_types: ["impacts_by_sex_age"]
- Returns breakdown by sex and age
"""


if LANGCHAIN_AVAILABLE:
    
    class DataRequestInput(BaseModel):
        """Input schema for data request analysis."""
        campaign_name: str = Field(description="Name of the campaign (e.g., 'Campaign 1')")
        data_types: list[str] = Field(
            description="List of data types requested (e.g., ['impacts_by_sex_age', 'tv_spot_schedule'])"
        )
        target_audience: str | None = Field(
            default=None, description="Target audience specification if mentioned"
        )
    
    class TableFilterInput(BaseModel):
        """Input schema for table filtering."""
        table_names: list[str] | None = Field(
            default=None,
            description="List of table element keys to filter. If not provided, uses default sample tables."
        )

    
    def generate_mock_api_call(
        campaign_dir: Path,
        campaign_name: str,
        data_types: list[str],
        target_audience: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate an API call request body for dataBreeders API.
        
        This creates the actual JSON request body that would be sent to the API,
        plus a curl command example.
        """
        logger.info(f"Generating API call for: {data_types}")
        logger.info(f"Target audience: {target_audience}")
        
        # Build target filter based on target_audience
        targets = []
        if target_audience:
            if target_audience.upper() == "W25-54":
                targets.append({
                    "name_target": "W25-54",
                    "filter": [
                        {"age_break": "25_34", "sex": "F"},
                        {"age_break": "35_44", "sex": "F"},
                        {"age_break": "45_54", "sex": "F"}
                    ]
                })
            elif target_audience.upper() == "M25-54":
                targets.append({
                    "name_target": "M25-54",
                    "filter": [
                        {"age_break": "25_34", "sex": "M"},
                        {"age_break": "35_44", "sex": "M"},
                        {"age_break": "45_54", "sex": "M"}
                    ]
                })
            elif target_audience.upper() == "A3+":
                targets.append({
                    "name_target": "A3+",
                    "filter": [
                        {"age_break": "03_14"},
                        {"age_break": "15_17"},
                        {"age_break": "18_24"},
                        {"age_break": "25_34"},
                        {"age_break": "35_44"},
                        {"age_break": "45_54"},
                        {"age_break": "55_64"},
                        {"age_break": "65_plus"}
                    ]
                })
            else:
                # Default: use the raw target name
                targets.append({
                    "name_target": target_audience,
                    "filter": [{"age_break": "25_34"}]  # Placeholder
                })
        else:
            # Default target if none specified
            targets.append({
                "name_target": "A3+",
                "filter": [
                    {"age_break": "03_14"},
                    {"age_break": "15_17"},
                    {"age_break": "18_24"},
                    {"age_break": "25_34"},
                    {"age_break": "35_44"},
                    {"age_break": "45_54"},
                    {"age_break": "55_64"},
                    {"age_break": "65_plus"}
                ]
            })
        
        # Build API request body following dataBreeders format
        api_request_body = {
            "name_campaign": campaign_name,
            "target": targets,
            "sg_code": [
                {"id": "sg412233", "period_start": "2024-08-11", "period_end": "2024-08-25"},
                {"id": "sg412234", "period_start": "2024-08-11", "period_end": "2024-08-25"}
            ],
            "filter": {
                "broadcaster": [],
                "channel": [],
                "device_type": [],
                "online_video": ["is_platform"]
            }
        }
        
        # Generate curl command
        endpoint = "https://api.databreeders.com/v1/campaign/data"
        curl_command = f"""curl -X POST {endpoint} \\
  -H "Content-Type: application/json" \\
  -H "Authorization: Bearer YOUR_API_KEY" \\
  -d '{json.dumps(api_request_body, indent=2)}'"""
        
        result = {
            "api_request": {
                "endpoint": endpoint,
                "method": "POST",
                "headers": {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer YOUR_API_KEY"
                },
                "body": api_request_body
            },
            "curl_command": curl_command,
            "requested_data_types": data_types,
            "explanation": f"This API call will fetch {', '.join(data_types)} for {campaign_name}"
        }
        
        logger.info("API call generated successfully")
        return result
    
    
    def create_simple_agent(campaign_dir: Path, table_names: list[str], enable_tracing: bool = True):
        """
        Create a simple LangChain agent demonstrating Lesson 1 concepts.
        
        Args:
            campaign_dir: Path to campaign directory
            table_names: List of table names to use for filtering
            enable_tracing: Enable LangSmith tracing
            
        Returns:
            Compiled agent graph ready to invoke
        """
        logger.info("Creating Lesson 1 demonstration agent...")
        
        # Enable LangSmith tracing if configured
        if enable_tracing and os.getenv("LANGSMITH_API_KEY"):
            os.environ["LANGCHAIN_TRACING_V2"] = "true"
            os.environ["LANGCHAIN_PROJECT"] = f"lesson-01-{campaign_dir.name}"
            logger.info("LangSmith tracing enabled")
        
        # Check for API key
        if not os.getenv("GOOGLE_API_KEY") and not os.getenv("GEMINI_API_KEY"):
            raise ValueError(
                "GOOGLE_API_KEY or GEMINI_API_KEY required. "
                "Get your key from https://aistudio.google.com/apikey"
            )
        
        # Initialize Gemini LLM
        llm: BaseChatModel = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            #model="gemini-2.5-flash-lite",
            #model="gemini-2.5-pro",
            #model="gemini-3-pro-preview",
            temperature=0.0,  # Deterministic
        )
        
        # Create tools
        list_fixtures_tool = StructuredTool.from_function(
            func=lambda **_: list_available_fixtures(campaign_dir),
            name="list_available_fixtures",
            description="List all available Campaign 1 fixture files.",
        )
        
        generate_api_call_tool = StructuredTool.from_function(
            func=lambda campaign_name, data_types, target_audience=None, **_: generate_mock_api_call(
                campaign_dir,
                campaign_name=campaign_name,
                data_types=data_types,
                target_audience=target_audience,
            ),
            name="generate_api_call",
            description="Generate a mock API call for downloading campaign data. Returns local fixture paths as mock response.",
            args_schema=DataRequestInput,
        )
        
        #only for demostration
        filter_tables_tool = StructuredTool.from_function(
            func=lambda table_names=None, **_: filter_tables_by_allowlist(
                table_names
            ),
            name="filter_tables_by_allowlist",
            description="Filter table names based on allowlist rules (TRP/TabSummary only, exclude plots/30eq). If no table_names provided, uses default sample tables.",
            args_schema=TableFilterInput,
        )
        
        tools = [list_fixtures_tool, generate_api_call_tool, filter_tables_tool]
        
        # System prompt with injected API knowledge
        system_prompt = f"""You are an expert API analyzer for dataBreeders campaign data.

Your role is to:
1. Understand user requests for campaign data downloads
2. Transform natural language requests into proper API calls with correct data_types
3. Generate appropriate API calls (currently mock responses using local fixtures)
4. Apply allowlist rules for table filtering
5. Guide users to available data sources

IMPORTANT GUIDELINES:
- This is a local-first system using Campaign 1 fixtures
- All outputs must be deterministic and traceable
- Table allowlist must be strictly enforced (TRP/TabSummary only, exclude plots/.30eq)
- Never invent or compute numeric data

{DATABREEDERS_API_KNOWLEDGE}

When a user asks for data:
1. ANALYZE the request to identify what data types are needed (use the knowledge base above)
2. Map natural language to specific data_types (e.g., "TRP data" → "impacts_in_target")
3. Identify target audience from the request (e.g., "women 25-54" → "W25-54")
4. Use list_available_fixtures every time, then use generate_api_call tool to CREATE THE ACTUAL API REQUEST
5. Return the curl command and request body - DO NOT just search for local files
6. Explain what the API call will fetch

CRITICAL: You are NOT a search assistant. You are an API call generator.
Your output should be an actual API request (curl command + JSON body), not file paths.

Be precise, follow the allowlist rules strictly, and always use the tools provided.
Most importantly: GENERATE API CALLS, don't search local files."""  
        
        # Create agent
        agent_graph = create_agent(llm, tools=tools, system_prompt=system_prompt)
        
        logger.info("Agent created successfully")
        return agent_graph


# ============================================================================
# PART 4: Main Execution Examples
# ============================================================================


def main(query: str | None = None) -> int:
    """
    Main execution function demonstrating Lesson 1 concepts.
    
    Runs through:
    1. Environment verification
    2. Campaign fixture discovery
    3. Table allowlist filtering
    4. (Optional) Agent creation and execution
    """

    # STEP 1: Environment Verification
    logger.info("\n[STEP 1] Environment Verification")
    logger.info("-" * 70)
    
    check_results = run_environment_checks()
    
    failed_checks = sum(1 for r in check_results.values() if r["status"] == "FAIL")
    
    if failed_checks > 0:
        logger.error("\nEnvironment checks failed. Please fix issues above.")
        return 1
    
    # STEP 2: Campaign Fixture Discovery
    logger.info("\n[STEP 2] Campaign Fixture Discovery")
    logger.info("-" * 70)
    
    campaign_path = Path("Starter Kit – Agentic Models/Campaign 1")
    fixtures = list_available_fixtures(campaign_path)
    
    logger.info(f"\nCampaign: {fixtures['campaign']}")
    logger.info(f"Available fixtures ({fixtures['count']}):")
    for fixture_name in fixtures["available_fixtures"]:
        logger.info(f"  - {fixture_name}")
    
    # STEP 3: Table Allowlist Filtering
    logger.info("\n[STEP 3] Table Allowlist Filtering")
    logger.info("-" * 70)
    
    sample_tables = [
        "standard_tabcontacts_table_contact_sexage_trp_raw",
        "standard_tabcontacts_plot_contact_sexage_abs_raw",
        "standard_tabsummary_table_contactreach_target_perc",
        "standard_tabr1_table_reach_target_perc_30eq",
        "standard_tabrf_table_reach_target_abs",
    ]
    
    logger.info(f"\nSample tables ({len(sample_tables)}):")
    for table in sample_tables:
        logger.info(f"  - {table}")
    
    filter_result = filter_tables_by_allowlist(sample_tables)
    
    logger.info(f"\nAllowlist rules:")
    for rule in filter_result["allowlist_rules"]:
        logger.info(f"  - {rule}")
    
    logger.info(f"\nResults:")
    logger.info(f"  Allowed: {filter_result['statistics']['allowed_count']}")
    logger.info(f"  Rejected: {filter_result['statistics']['rejected_count']}")
    
    if filter_result["rejected_tables"]:
        logger.info(f"\nRejected tables:")
        for rejected in filter_result["rejected_tables"]:
            logger.info(f"  - {rejected['name']}: {rejected['reason']}")
    

    print(sample_tables)
    # STEP 4: Agent Creation (if LangChain available)
    if LANGCHAIN_AVAILABLE:
        logger.info("\n[STEP 4] Agent Creation (Optional)")
        logger.info("-" * 70)
        
        try:
            agent = create_simple_agent(campaign_path, sample_tables, enable_tracing=True)
            logger.info("Agent created successfully with LangSmith tracing")

            # Example query - user asks for data, agent should generate API call
            if query:
                user_query = query
            else:
                user_query = "I need TRP data and reach metrics for Campaign 1 targeting women 25-54"
            
            logger.info(f"\nQuery: '{user_query}'")

            state = agent.invoke({"messages": [HumanMessage(content=user_query)]})
            messages = state.get("messages", []) if isinstance(state, dict) else []
            
            # Extract response
            for msg in reversed(messages):
                if isinstance(msg, AIMessage):
                    logger.info(f"\nAgent response:\n{msg.content}")
                    break
        
        except Exception as e:
            logger.warning(f"Agent creation skipped: {e}")
    else:
        logger.info("\n[STEP 4] Agent Creation (Skipped - LangChain not available)")
    

    return 0



if __name__ == "__main__":
    # ===========================================================
    # Parse command-line arguments
    # ===========================================================
    parser = ArgumentParser(description="Lesson 1: Working Code Demonstration")
    parser.add_argument("--query", type=str, help="Optional user query for the agent")
    args = parser.parse_args()
    query_input = args.query if args.query else None
    exit_code = main(query_input)
    sys.exit(exit_code)
