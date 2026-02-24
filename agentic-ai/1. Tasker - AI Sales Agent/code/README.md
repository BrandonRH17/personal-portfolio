# Tasker — Technical Setup

This folder contains the full source code for the Tasker AI Sales Agent. The code is mirrored from the [original hackathon repository](https://github.com/BrandonRH17/AIAgentsHackathon2025-Neutrino).

---

## Prerequisites

- Python 3.10+
- Azure subscription with the following services provisioned:
  - Azure AI Foundry (for AI Agent deployment)
  - Azure AI Search (for vector indexes)
  - Azure SQL Database
  - Azure Logic Apps (3 workflows)
- Azure CLI authenticated (`az login`)

---

## Components

| Folder | Description |
|--------|-------------|
| `ai-agent/` | FastAPI web app serving the agent chat interface + agent creation script with OpenAPI tool definitions |
| `ai-search/` | Azure AI Search configuration — vector indexes for clients, properties, and projects using text-embedding-3-large |
| `logic-apps/` | Three Azure Logic App workflow definitions: vector searcher, instant quotes, and video builder |
| `mysql/` | Azure SQL DDL scripts for the relational schema (clients, properties, projects) |

---

## Quick Start

```bash
# Clone and setup
cd ai-agent
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run the agent web app
uvicorn app:app --reload
```

> **Note:** The agent requires active Azure services (AI Foundry, Logic Apps, AI Search, SQL Database) to function. The connection strings and agent IDs in the code reference the hackathon deployment environment.

---

## Key Files

| File | Purpose |
|------|---------|
| `ai-agent/createAgent.py` | Creates the Azure AI Agent with full system prompt and OpenAPI tool bindings |
| `ai-agent/logicAppsSchema.json` | OpenAPI 3.0 schema defining all 3 Logic App endpoints (VectorSearcher, Quotation, VideoBuilder) |
| `ai-agent/app.py` | FastAPI app that manages conversation threads and proxies messages to the agent |
| `logic-apps/db-vector-searcher/` | Embeds queries with OpenAI, performs vector search by entity type, returns cleaned results |
| `logic-apps/instant-quotes/` | Copies a Google Sheets template, inserts property data, returns a shareable quote link |
| `logic-apps/video-builder/` | Calls BannerBear API to generate personalized intro/outro videos with the client's name |

---

**Author:** Brandon Rodriguez
