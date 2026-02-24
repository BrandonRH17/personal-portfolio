# Logic Apps Integration

<p align="center">
  <img src="https://azure.microsoft.com/svghandler/logic-apps/?width=600&height=315" alt="Azure Logic Apps" width="600" height="315">
</p>

We use **Azure Logic Apps** to orchestrate our AI workflows and integrate seamlessly with our MySQL database and Azure OpenAI services. Currently, we have three dedicated Logic Apps, each designed for a specific part of our AI-driven architecture.

---
# Logic Apps List

## 1. `db-vector-searcher`

This Logic App is responsible for powering our AI agent with the contextual information it needs to assist real estate advisors or trigger other internal tools.
---

## 🧠 Single-Agent Architecture vs Multi-Agent

Rather than using a multi-agent setup where each agent manages a specific tool, we chose to implement a **single agent with a unified toolset**.

### ✅ Why a Single Agent?

- **Prompt efficiency**:  
  Having a single agent reduces the amount of repeated context and communication overhead across agents. This significantly **optimizes both prompt and completion token usage**, especially when using models like GPT-4.1 mini.

- **Proven model capability**:  
  Through testing, we've confirmed that GPT-4.1 mini can handle all three integrated tools (vector search, quoting, and content generation) **reliably and without hallucination**, making multi-agent coordination unnecessary.

- **Improved stability**:  
  A single-agent setup avoids the risk of **delegated-agent errors** that can disrupt the flow of execution — a common issue when relying on multiple agents that must communicate and trust one another's output.

---
---

This modular, cost-efficient, and agent-optimized architecture allows our AI system to operate with both **scalability and precision**, without unnecessary complexity or token waste.
