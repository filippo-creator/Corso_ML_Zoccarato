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
  strong {
    color: #0066cc;
  }
---

<!-- _class: lead -->

# Lesson 3
## Fundamental Concepts of Agentic AI

Course: Development of Agentic AI Systems for Advertising Campaign Analysis using Langchain Framework

Duration: 2 hours

---

## Learning Objectives

By the end of this lesson, students will be able to:

- Understand the fundamental difference between traditional conversational systems and autonomous agents
- Identify the distinctive characteristics of an agentic system
- Recognize application scenarios where an agentic approach is appropriate
- Understand the planning and execution cycle of an agent
- Analyze agentic architectures and their main components
- Evaluate when to use an autonomous agent versus a deterministic pipeline

---

## Lesson Agenda

**Part 1: From Chatbot to Autonomous Agent**
- Evolution of conversational systems
- Definition of Agentic AI
- Fundamental differences

**Part 2: Characteristics of Agents**
- Planning and reasoning capabilities
- Use of external tools
- Memory management

**Part 3: Agentic Architectures**
- ReAct pattern
- Agent execution cycle
- Practical applications in advertising

---

<!-- _class: lead -->

# Part 1
## From Chatbot to Autonomous Agent

---

## Traditional Chatbot: Characteristics

**Classic conversational paradigm**

**Reactive architecture**: responds to user requests without its own initiative

**Linear flow**: predefined question-answer sequence without significant deviations

**Limited context**: maintains memory of current conversation but with limited strategic use capabilities

**No external interaction**: processes exclusively information present in the prompt or conversational memory

**Example**
```
User: "What is the reach of campaign X?"
Chatbot: "The reach of campaign X is 45%."
```

The chatbot provides information but cannot acquire it autonomously from external sources or plan complex actions.

---

## Autonomous Agent: Definition

**Definition**
An autonomous agent is an AI system capable of pursuing complex goals through planning and executing a sequence of actions, using external tools and adapting its behavior based on obtained results.

**Distinctive characteristics**

**Decisional autonomy**: autonomously determines necessary actions to achieve the objective

**Goal-oriented**: works to reach a specific purpose, not just to respond

**Tool usage**: interacts with external systems (APIs, databases, calculators) to acquire information or execute operations

**Dynamic planning**: develops and modifies action plans based on intermediate results

**Strategic memory management**: maintains and uses relevant information for future decisions

---

## Comparison: Chatbot vs Autonomous Agent

| Aspect | Traditional Chatbot | Autonomous Agent |
|---------|---------------------|-----------------|
| **Behavior** | Reactive | Proactive and goal-oriented |
| **Planning** | Absent or minimal | Multi-step planning |
| **Tools** | Not used | Autonomous tool usage |
| **Iterations** | Single response | Action-observation cycles |
| **Adaptation** | Limited | Dynamic based on results |
| **Task complexity** | Simple and direct tasks | Articulated and complex tasks |

**Practical implication**
The choice between chatbot and agent depends on task complexity. For direct responses, a chatbot is sufficient. For tasks requiring planning and interaction with external systems, an agent is necessary.

---

## Concrete Example: TTVAM Campaign Analysis

**Scenario: "Analyze autumn campaign performance and compare with budget"**

**Chatbot Approach**
```
User: "Analyze autumn campaign performance and compare with budget"

Chatbot: "Sorry, I don't have access to campaign data. 
         Can you provide the necessary information?"
```

**Autonomous Agent Approach**
```
Agent: "Proceeding with analysis:"
        
Step 1: Identify autumn campaign parameters
Step 2: Call TTVAM API to obtain performance data
Step 3: Retrieve planned budget from database
Step 4: Calculate KPIs (reach, frequency, cost per impact)
Step 5: Compare performance vs budget
Step 6: Generate report with analysis and recommendations

[Agent autonomously executes all these steps]
        
"Analysis completed: the campaign reached..."
```

---

<!-- _class: lead -->

# Part 2
## Distinctive Characteristics of Agents

---

## Characteristic 1: Planning Capability

**Definition**
Planning capability is the agent's ability to decompose a complex objective into an ordered sequence of sub-objectives and concrete actions.

**Planning process**

1. **Objective understanding**: interpretation of user request and identification of final goal
2. **Decomposition**: subdivision of goal into manageable sub-tasks
3. **Sequencing**: determination of optimal execution order
4. **Dependency identification**: recognition of which tasks depend on results of others

**Example in TTVAM context**
```
Objective: "Generate comparative report of three campaigns"

Agent's plan:
1. Identify Spotgate codes of three campaigns
2. For each campaign: call API to obtain data
3. Calculate standardized KPIs for each campaign
4. Compare results
5. Generate comparative visualizations
6. Compile narrative report with insights
```

---

## Planning Strategy: ReAct (Reasoning + Acting)

**ReAct Pattern**
Paradigm that alternates explicit reasoning and concrete action.

```
Thought: "I need to obtain reach data for campaign X"
Action: Call TTVAM API with appropriate parameters
Observation: "API returns reach = 45%"
Thought: "Data acquired, now I can proceed with comparison"
Action: Compare with target objective
```

**Advantages**
- Greater traceability of decisions
- Early identification of planning errors
- Possibility of plan revision based on results

**Practical implementation**
Add instructions like "Reason step-by-step", "Show your reasoning" or "Explain how you arrive at the conclusion" in prompts.

---

## Characteristic 2: Tool Usage

**Definition**
Tools are functions or interfaces that allow the agent to interact with external systems to acquire information or execute operations.

**Tool typologies**

**Information Retrieval**: APIs to obtain data, database queries, web searches

**Computation**: calculators, data processors, validators

**Action Execution**: sending emails, creating documents, updating databases

**Communication**: interaction with other systems or agents

**Fundamental characteristics of a tool**
- Descriptive name clearly indicating functionality
- Detailed description of what it does and when to use it
- Input parameter schema required
- Output format returned

---

## Example: Tool Definition for TTVAM

```python
{
  "name": "get_campaign_performance",
  "description": """Retrieves performance data for an advertising 
                    campaign from TTVAM system. Use this tool when 
                    you need metrics like reach, frequency or impacts 
                    for a specific campaign.""",
  "parameters": {
    "type": "object",
    "properties": {
      "spotgate_code": {
        "type": "string",
        "description": "Campaign Spotgate code"
      },
      "target": {
        "type": "object",
        "description": "Demographic target of interest"
      },
      "period_start": {
        "type": "string",
        "description": "Period start date (YYYY-MM-DD)"
      },
      "period_end": {
        "type": "string",
        "description": "Period end date (YYYY-MM-DD)"
      }
    },
    "required": ["spotgate_code", "period_start", "period_end"]
  }
}
```

---

## Characteristic 3: Memory Management

**Memory types in an agent**

**Short-term Memory**
- Contains current conversation context
- Includes history of recent actions and their results
- Limited by model's context window

**Long-term Memory**
- Persistence of relevant information beyond current session
- Typically implemented with vector or relational databases
- Allows retrieval of knowledge acquired in the past

**Working Memory**
- Temporary information necessary for current task
- Intermediate results of calculations or operations
- Execution plan state

---

<!-- _class: lead -->

# Part 3
## Agentic Architectures

---

## Agent Execution Cycle

**Standard agentic cycle phases**

```
1. PERCEIVE (Perception)
   → Agent receives input from user or environment
   
2. REASON (Reasoning)
   → Analyzes current situation
   → Evaluates plan state and objectives
   → Decides next action
   
3. ACT (Action)
   → Executes chosen action (tool call, response, etc.)
   
4. OBSERVE (Observation)
   → Receives action result
   → Updates state understanding
   
5. EVALUATE (Evaluation)
   → Verifies if objective is reached
   → If no, return to REASON
   → If yes, proceed to final response
```

**Fundamental characteristic**
The cycle is iterative: agent continues until reaching objective or maximum iteration limit.

---

## Architectural Pattern: ReAct Agent

**ReAct pattern structure**

ReAct (Reasoning and Acting) explicitly alternates reasoning and action phases.

**Components**
```
Thought: Agent's explicit reasoning
Action: Invocation of specific tool
Observation: Action result
[Repetition until completion]
Final Answer: Conclusive response to user
```

**Concrete example**
```
Thought: "I need to retrieve reach data for campaign 
         with Spotgate code 1234 for March 2024"
         
Action: get_campaign_performance(
  spotgate_code="1234", 
  period_start="2024-03-01",
  period_end="2024-03-31"
)

Observation: {"reach": "48%", "frequency": 3.8}

Thought: "I obtained data. Now I can respond to user"

Final Answer: "Campaign reached 48% reach..."
```

---

## Flow Control: Decision Strategies

**Max iterations**
Maximum limit of agentic cycle iterations to prevent infinite loops

**Early stopping**
Anticipated termination when:
- Objective is reached
- No further useful actions available
- Critical unrecoverable error occurs

**Timeouts**
Time limits for:
- Single tool operations
- Entire agent execution cycle
- Waiting for responses from external systems

**Error handling strategies**
- **Retry with backoff**: repeat failed operations with increasing intervals
- **Fallback actions**: alternative actions in case of failure
- **Graceful degradation**: provide partial result if total completion not possible

---

<!-- _class: lead -->

# Part 4
## Practical Applications

---

## Use Case 1: Campaign Analysis Assistant

**Scenario**
A marketing analyst wants to quickly obtain insights on multiple campaigns without writing code or manually composing complex queries.

**Agentic implementation**

Available tools:
- `get_campaign_data`: retrieve data from TTVAM API
- `calculate_kpi`: calculate derived metrics
- `compare_campaigns`: compare performance
- `generate_visualization`: create charts

**Agent behavior**:
```
User: "Compare spring and summer 2024 campaigns performance 
       on female target 25-44"

Agent:
1. Identifies Spotgate codes of two campaigns
2. Defines target filter for F 25-44
3. Calls get_campaign_data for both campaigns
4. Calculates comparative KPIs with calculate_kpi
5. Generates visualization with compare_campaigns
6. Presents narrative analysis with insights
```

---

## Benefits of Agentic Approach in TTVAM Project

**Data access democratization**
Non-technical users can execute complex analyses without programming skills or deep API knowledge

**Workload reduction**
Automation of repetitive tasks that would require hours of manual work from analysts

**Consistency and standardization**
Uniform application of calculation methodologies and report formatting

**Execution speed**
Reduction of analysis times from hours to minutes for complex tasks

**Scalability**
Capacity to handle growing request volumes without proportional increase in human resources

**Decision quality**
Rapid access to data-driven insights to support strategic decisions

---
