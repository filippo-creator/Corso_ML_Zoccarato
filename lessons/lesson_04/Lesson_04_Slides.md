---
marp: true
theme: default
paginate: true
backgroundColor: #fff
style: |
  section {
    font-size: 26px;
  }
  h1 {
    color: #1a1a1a;
    font-weight: 600;
  }
  h2 {
    color: #333333;
    font-weight: 500;
  }
  h3 {
    color: #555555;
    font-weight: 500;
  }
  .columns {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 1.5rem;
  }
  strong {
    color: #0066cc;
  }
  table {
    font-size: 22px;
  }
---

<!-- _class: lead -->

# Lesson 4
## Overview of Frameworks for Agentic AI

Course: Development of Agentic AI Systems for Advertising Campaign Analysis using Langchain Framework

Duration: 2 hours

---

## Learning Objectives

By the end of this lesson, students will be able to:

- Understand the role and value of frameworks in building agentic AI systems
- Compare the main characteristics of Langchain, CrewAI, and other emerging frameworks
- Evaluate frameworks based on practical criteria: ease of implementation, documentation, community support, and scalability
- Justify the selection of Langchain for the TTVAM project
- Understand the core architecture and components of Langchain
- Implement basic agentic workflows using Langchain

---

## Lesson Agenda

**Part 1: Introduction to Agentic AI Frameworks**
- The role of frameworks in agentic systems
- Building from scratch vs using frameworks
- Key components of agentic frameworks

**Part 2: Framework Landscape**
- Langchain: comprehensive toolkit
- CrewAI: multi-agent collaboration
- AutoGPT and autonomous agents
- Emerging frameworks (LangGraph, AutoGen, Haystack)

**Part 3: Comparative Analysis**
- Evaluation criteria
- Strengths and limitations
- Use case mapping

**Part 4: Deep Dive into Langchain**
- Architecture overview
- Core components
- Why Langchain for TTVAM

---

<!-- _class: lead -->

# Part 1
## Introduction to Agentic AI Frameworks

---

## What is an Agentic AI Framework?

**Definition**
An agentic AI framework is a software library that provides abstractions, tools, and patterns to simplify the development of autonomous AI agents capable of planning, tool usage, and goal-oriented behavior.

**Core Value Proposition**

Abstraction of complexity: handles low-level details of LLM interaction, tool integration, and state management.

Reusable components: pre-built modules for common agentic patterns (memory, planning, tool execution).

Standardized interfaces: consistent APIs for working with different LLMs and tools.

Best practices built-in: incorporates proven patterns for reliability and performance.

**Analogy**
Just as web frameworks (Django, React) simplify web development, agentic frameworks simplify building intelligent agents.

---

## Building from Scratch vs Using Frameworks

<div class="columns">
<div>

### Building from Scratch

**Advantages**
- Complete control over architecture
- No framework dependencies
- Optimized for specific use case
- Learning experience

**Disadvantages**
- Time-consuming development
- Need to handle edge cases
- Reinventing solved problems
- Maintenance burden
- Lack of community support

</div>
<div>

### Using a Framework

**Advantages**
- Faster development
- Battle-tested components
- Active community support
- Regular updates and improvements
- Focus on business logic

**Disadvantages**
- Learning curve
- Framework limitations
- Potential overhead
- Dependency on maintainers
- Less flexibility in some areas

</div>
</div>

**Recommendation**: For production systems like TTVAM, frameworks provide significant advantages in development speed and reliability.

---

## Key Components of Agentic Frameworks

All major agentic AI frameworks provide similar fundamental components:

**LLM Abstraction Layer**
Unified interface to interact with different LLM providers (OpenAI, Anthropic, Google, open-source models).

**Agent Orchestration**
Core logic for the agent execution cycle: reasoning, action selection, tool invocation, and observation.

**Tool/Function System**
Framework for defining, registering, and executing external tools that agents can use.

**Memory Management**
Components for maintaining conversation history, working memory, and long-term storage.

**Prompt Templates**
Reusable prompt structures with variable interpolation and formatting.

**Chain Composition**
Ability to connect multiple operations into sequential or conditional workflows.

---

<!-- _class: lead -->

# Part 2
## Framework Landscape

---

## Langchain: Comprehensive Toolkit

**Overview**
Langchain is the most mature and widely adopted framework for building LLM-powered applications, with strong support for agentic workflows.

**Key Characteristics**

Modular architecture: extensive library of composable components.

Multi-language support: robust implementations in Python and JavaScript/TypeScript.

Extensive integrations: 100+ integrations with LLM providers, vector stores, tools, and services.

Active development: frequent updates and new features.

**Core Components**
- LLMs and Chat Models
- Prompts and Prompt Templates
- Chains and LangChain Expression Language (LCEL)
- Agents and Tools
- Memory systems
- Retrievers and Vector Stores

**Website**: https://python.langchain.com

---

## Langchain: Strengths and Use Cases

**Strengths**

Maturity: most battle-tested framework with extensive production usage.

Documentation: comprehensive docs with examples and tutorials.

Flexibility: supports wide range of architectures from simple chains to complex agents.

Ecosystem: large collection of pre-built integrations and tools.

Community: active Discord community and extensive Stack Overflow presence.

**Ideal Use Cases**
- Production-ready applications requiring reliability
- Complex multi-step workflows
- Applications requiring multiple tool integrations
- Projects benefiting from extensive documentation
- Systems needing both simple chains and complex agents

**When to Use Langchain**
Projects requiring mature, well-documented solutions with extensive ecosystem support.

---

## CrewAI: Multi-Agent Collaboration

**Overview**
CrewAI specializes in orchestrating multiple AI agents working together towards common goals, with built-in role-based collaboration patterns.

**Key Characteristics**

Multi-agent focus: designed specifically for agent collaboration.

Role-based design: agents have specific roles, goals, and backstories.

Task delegation: agents can delegate sub-tasks to other specialized agents.

Built-in workflows: pre-defined patterns for common collaborative scenarios.

**Core Concepts**
- Agents with roles and goals
- Tasks with descriptions and expected outputs
- Crews (teams of agents)
- Processes (sequential, hierarchical)

**Strengths**
- Intuitive mental model for multi-agent systems
- Good for complex workflows requiring specialization
- Built-in collaboration patterns

**Website**: https://www.crewai.com

---

## CrewAI: Use Cases and Considerations

**Ideal Use Cases**

Complex research tasks requiring multiple specialized agents.

Content generation workflows with distinct roles (researcher, writer, editor).

Business process automation with handoffs between specialized functions.

Projects where agent collaboration brings clear value.

**Limitations**

Less mature than Langchain (newer framework).

Smaller ecosystem of integrations.

Less flexible for non-collaborative agent patterns.

Steeper learning curve for role-based abstractions.

**When to Choose CrewAI**
Projects explicitly requiring multiple specialized agents working collaboratively, where the role-based paradigm provides clear architectural benefits.

**Comparison with Langchain**
Langchain can also implement multi-agent systems, but CrewAI provides higher-level abstractions specifically optimized for this pattern.

---

## AutoGPT and Autonomous Agents

**Overview**
AutoGPT represents a class of frameworks focused on fully autonomous agents capable of pursuing goals with minimal human intervention.

**Key Characteristics**

Goal-oriented: user defines high-level goals, agent determines execution strategy.

Self-directed: agent autonomously plans, executes, and adjusts approach.

Memory system: maintains context across extended execution sessions.

Web interaction: built-in capabilities for web browsing and research.

**Related Projects**
- AutoGPT: original autonomous agent framework
- BabyAGI: task-driven autonomous agent
- AgentGPT: web-based autonomous agent platform

**Strengths**
- Impressive autonomy for exploratory tasks
- Good for open-ended research and problem-solving
- Minimal user guidance required

---

## AutoGPT: Considerations for Production

**Limitations**

Unpredictability: fully autonomous behavior can be difficult to control.

Cost concerns: extensive autonomous exploration can consume significant API credits.

Reliability: may go off-track without proper constraints.

Production readiness: better suited for experimentation than production systems.

**Appropriate Use Cases**

Research and exploration tasks.

Personal productivity assistants.

Prototyping autonomous agent capabilities.

Educational purposes to understand agentic behavior.

**When NOT to Use**

Business-critical applications requiring predictability.

Cost-sensitive deployments.

Systems requiring strict compliance or governance.

Applications needing fine-grained control over agent actions.

**For TTVAM Project**: Not suitable due to need for predictable, controlled behavior in production environment.

---

## Emerging Frameworks Overview

**LangGraph (by LangChain)**

Graph-based agent workflow orchestration with explicit state management.

Built on top of Langchain, provides more control over agent execution flow.

Ideal for: complex workflows requiring precise control and debugging.

**AutoGen (by Microsoft)**

Multi-agent conversation framework with emphasis on conversational patterns.

Strong support for code generation and execution.

Ideal for: collaborative coding assistants and conversational AI systems.

**Haystack (by deepset)**

Focus on semantic search and question-answering systems.

Strong NLP and document processing capabilities.

Ideal for: document-heavy applications, search systems, RAG implementations.

---

<!-- _class: lead -->

# Part 3
## Comparative Analysis

---

## Evaluation Criteria for Framework Selection

When selecting a framework for a production project, consider these key dimensions:

**Maturity and Stability**
- Production readiness
- Breaking changes frequency
- Version stability

**Documentation Quality**
- Comprehensiveness
- Examples and tutorials
- API reference clarity

**Community and Support**
- Community size and activity
- Response time for issues
- Available learning resources

**Ease of Implementation**
- Learning curve
- Development velocity
- Debugging capabilities

**Scalability**
- Performance characteristics
- Resource requirements
- Production deployment patterns

**Ecosystem**
- Available integrations
- Pre-built components
- Extension capabilities

---

## Framework Comparison Matrix

| Criterion | Langchain | CrewAI | AutoGPT | LangGraph |
|-----------|-----------|--------|---------|-----------|
| **Maturity** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| **Documentation** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Community** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Ease of Use** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Flexibility** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Production Ready** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| **Tool Ecosystem** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |

**Key Insight**: Langchain offers the most comprehensive and production-ready solution, while specialized frameworks excel in specific use cases.

---

## Use Case Mapping

**Choose Langchain when:**
- Building production systems requiring reliability
- Need extensive tool and service integrations
- Require both simple and complex workflows
- Want comprehensive documentation and community support
- Need flexibility to evolve architecture over time

**Choose CrewAI when:**
- Multi-agent collaboration is core requirement
- Clear role specialization benefits the architecture
- Task delegation between agents is natural fit
- Team-based mental model aids development

**Choose AutoGPT/BabyAGI when:**
- Exploring autonomous agent capabilities
- Research or experimental projects
- Personal productivity tools
- Cost is not primary constraint

**Choose LangGraph when:**
- Need explicit state management and control
- Complex conditional workflows
- Debugging and observability are priorities
- Building on Langchain ecosystem

---

<!-- _class: lead -->

# Part 4
## Deep Dive into Langchain

---

## Why Langchain for the TTVAM Project?

**Project Requirements Analysis**

Production deployment: requires stability and reliability.

API integration: need to interact with TTVAM REST API.

Tool usage: agents must use external tools (API calls, calculations).

Flexible architecture: may need to evolve from simple to complex patterns.

Maintainability: code must be maintainable by team members.

**Langchain Advantages for TTVAM**

Mature and production-ready: extensive battle-testing in similar applications.

Excellent documentation: reduces onboarding time for team members.

Flexible tool system: easy to create custom tools for TTVAM API.

Active community: problems likely already solved by others.

Gradual complexity: start simple, add complexity as needed.

Clear migration path: from basic chains to sophisticated agents.

---

## Langchain Architecture Overview

**Layered Architecture**

```
┌─────────────────────────────────────────────────┐
│          Application Layer                      │
│     (Your TTVAM Agentic System)                │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│         Langchain Components                    │
│  Agents | Chains | Tools | Memory | Prompts   │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│        LLM Providers & Integrations             │
│  OpenAI | Anthropic | Google | Vector Stores   │
└─────────────────────────────────────────────────┘
```

**Key Principle**: Composability - combine small, focused components into complex systems.

---

## Langchain Core Components

**1. Language Models**
- LLMs (completion models)
- Chat Models (conversation-optimized)
- Unified interface across providers

**2. Prompts**
- PromptTemplate: string formatting with variables
- ChatPromptTemplate: structured messages
- Few-shot examples

**3. Chains**
- Sequential workflows
- Conditional logic
- LCEL (LangChain Expression Language)

**4. Agents**
- ReAct agent: reasoning + acting
- Structured chat agent
- OpenAI functions agent
- Custom agent implementations

---

## Langchain Core Components (continued)

**5. Tools**
- Function definitions for agents
- Input/output schemas
- Built-in tools and custom implementations

**6. Memory**
- ConversationBufferMemory: stores full history
- ConversationSummaryMemory: summarizes old conversations
- VectorStore-backed memory: semantic retrieval

**7. Retrievers**
- Document loading and chunking
- Vector store integration
- Semantic search capabilities

**8. Callbacks**
- Logging and monitoring
- Token usage tracking
- Custom handlers for observability

---

## First Langchain Example: Simple Agent

**Conceptual Flow for TTVAM Agent**

```python
from langchain.agents import create_react_agent, AgentExecutor
from langchain.tools import Tool
from langchain_groq import ChatGroq

# 1. Initialize LLM
llm = ChatGroq(model="llama-3.1-70b-versatile")

# 2. Define Tools
def get_campaign_reach(spotgate_code: str) -> str:
    """Retrieve reach for a campaign."""
    # Call TTVAM API (simplified)
    return "Reach: 45%"

tools = [
    Tool(
        name="get_campaign_reach",
        func=get_campaign_reach,
        description="Get reach data for a campaign by Spotgate code"
    )
]

# 3. Create Agent
agent = create_react_agent(llm, tools, prompt_template)

# 4. Create Executor
agent_executor = AgentExecutor(agent=agent, tools=tools)

# 5. Run
result = agent_executor.invoke({
    "input": "What is the reach of campaign 1234?"
})
```

---



## Resources for Further Learning

**Official Documentation**

Langchain Python: https://python.langchain.com/docs/introduction/

CrewAI: https://docs.crewai.com/

LangGraph: https://langchain-ai.github.io/langgraph/

**Tutorials and Courses**

Langchain Course: https://learn.deeplearning.ai/langchain

Agent Development with Langchain: https://www.deeplearning.ai/short-courses/

**Community Resources**

Langchain Discord: Active community for questions and discussions

GitHub Examples: https://github.com/langchain-ai/langchain/tree/master/cookbook

Stack Overflow: Extensive Q&A on Langchain implementation

**Blog Posts and Articles**

Comparing Agentic AI Frameworks (various sources)

Production deployment patterns for Langchain agents

Best practices for tool design in Langchain

