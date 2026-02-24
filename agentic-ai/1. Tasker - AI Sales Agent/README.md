# Tasker — AI Sales Agent
### Reinventing High-Ticket Sales with AI-Powered Lead Response

---

## 📋 Project Overview (STAR Format)

### 🎯 Situation

High-ticket sales teams — real estate developers, automobile dealerships, luxury services — lose qualified leads every day because of slow response times. According to a [MIT study](https://hbr.org/2011/03/the-short-life-of-online-sales-leads), companies that respond to leads within **5 minutes** are **100x more likely** to connect with them than those that wait 30 minutes or more. Most sales teams respond hours or even days later.

The typical workflow looks like this: a customer asks about a property, the agent sends the same generic PDF they send to everyone else, manually builds a quote in Google Sheets, and follows up via WhatsApp — if they remember. Every step is manual, slow, and impersonal.

### 📝 Task

Build an AI agent that transforms this reactive, manual sales process into a proactive, automated system capable of:

- **Instant lead response** (< 5 minutes) via the customer's preferred channel (WhatsApp or email)
- **Lead pre-qualification** with relevant content sent immediately
- **Advisor notification** with full lead details and status updates
- **Automated CRM management** — client creation and deal tracking without manual input
- **Personalized content generation** — custom videos based on the lead's persona delivered in under 3 minutes
- **Instant quote creation** — property-specific quotes generated from live data in under 30 seconds

### ⚙️ Action

**Architecture: Single Agent with Unified Toolset**

Rather than a multi-agent system, we chose a **single AI agent with OpenAPI tool-use** — one agent orchestrating three specialized tools through Azure Logic Apps. This decision was driven by:

- **Prompt efficiency**: Reduces repeated context and optimizes token usage with GPT-4o-mini
- **Proven model capability**: GPT-4o-mini reliably handles all three tools without hallucination
- **Improved stability**: Avoids delegation errors common in multi-agent setups

**Technologies:**

| Component | Technology | Role |
|-----------|-----------|------|
| AI Agent | Azure AI Agents (GPT-4o-mini) | Core conversational agent with OpenAPI tool-use |
| Vector Search | Azure AI Search + text-embedding-3-large | Semantic search across clients, properties, and projects |
| Workflow Orchestration | Azure Logic Apps (3 workflows) | db-vector-searcher, instant-quotes, video-builder |
| Database | Azure SQL | Relational storage for clients, properties, projects |
| CRM | Retool | Custom-built CRM for advisors |
| Video Generation | BannerBear | Programmatic personalized videos per lead persona |
| Web App | FastAPI + Static Web Apps | Agent chat interface for advisors |
| Quote Delivery | Google Sheets + Google Drive | Templated quote generation and sharing |

**Key Design Decisions:**

1. **Separated vector indexes per entity type** (clients, properties, projects) — because each entity has different dimensionality (2-3 projects vs. 400+ properties), separate indexes ensure higher precision and lower noise
2. **OpenAPI schema as the tool interface** — the agent calls Logic Apps through a unified OpenAPI 3.0 spec, making tool definitions declarative and maintainable
3. **Persona-based video selection** — the agent analyzes the lead profile (family mom vs. young single) and selects appropriate video assets (amenities/family vs. luxury/zone) before generating

### 🎯 Result

**Microsoft AI Agents Hackathon 2025** — Team Neutrino

**Key Achievements:**

✅ **Instant Lead Response**
- Responds within the first 5 minutes of receiving a new lead via Facebook Ads or Instagram Forms
- Sends pre-qualification content immediately via the customer's preferred channel

✅ **Personalized Quote Generation (< 30 seconds)**
- Creates property-specific quotes based on live database data
- Includes property details, pricing, floor plans, parking, and financing terms
- Delivered as a shareable Google Sheets document

✅ **Personalized Video Generation (< 3 minutes)**
- Generates custom intro and outro videos per lead
- Adapts content to the lead persona — family-oriented videos for families, luxury/zone videos for young professionals
- Uses the client's first name directly in the video

✅ **Full CRM Automation**
- Automatically creates clients in the CRM upon lead capture
- Updates deal status and advisor assignments in real-time
- Eliminates manual data entry for the sales team

✅ **Semantic Search Across the Business**
- Vector-based search over clients, properties, and projects
- Advisors can ask natural language questions: "give me information about Oscar" → returns full client profile
- Property lookups by features, price range, or availability

---

## 📂 Project Structure

```
1. Tasker - AI Sales Agent/
├── code/
│   ├── README.md                          # Technical setup and component guide
│   ├── HACKATHON_README.md                # Original hackathon repo README
│   ├── assets/                            # Images, GIFs, and architecture diagrams
│   ├── ai-agent/
│   │   ├── app.py                         # FastAPI web app — agent chat interface
│   │   ├── createAgent.py                 # Azure AI Agent creation with OpenAPI tools
│   │   ├── logicAppsSchema.json           # OpenAPI 3.0 spec for all 3 Logic Apps
│   │   ├── requirements.txt              # Python dependencies
│   │   ├── static/                        # Frontend static assets
│   │   └── templates/                     # Jinja2 HTML templates
│   ├── ai-search/
│   │   └── readme.md                      # AI Search integration and embedding strategy
│   ├── logic-apps/
│   │   ├── readme.md                      # Logic Apps architecture rationale
│   │   ├── db-vector-searcher/            # Vector search workflow (JSON + docs)
│   │   ├── instant-quotes/                # Quote generation workflow (JSON)
│   │   └── video-builder/                 # Personalized video workflow (JSON)
│   └── mysql/
│       ├── readme.md                      # Azure SQL usage and CRM integration
│       └── tables/                        # DDL scripts (clients, properties, projects)
├── presentations/
│   └── README.md                          # Video links (Business Case + Tutorial) and live demo
└── README.md                              # This file
```

---

## 🔧 Architecture

```
                          ┌──────────────────────┐
                          │     Facebook Ads /    │
                          │   Instagram Forms     │
                          └──────────┬───────────┘
                                     │ New Lead
                                     ▼
┌─────────────┐          ┌──────────────────────┐
│   Retool    │◄────────►│   Azure AI Agent     │
│    CRM      │          │   (GPT-4o-mini)      │
└─────────────┘          │                      │
                         │   OpenAPI Tool-Use   │
                         └──┬────────┬────────┬─┘
                            │        │        │
              ┌─────────────┘        │        └─────────────┐
              ▼                      ▼                      ▼
   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
   │  Logic App #1    │  │  Logic App #2    │  │  Logic App #3    │
   │  Vector Searcher │  │  Instant Quotes  │  │  Video Builder   │
   └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
            │                     │                      │
            ▼                     ▼                      ▼
   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
   │ Azure AI Search  │  │  Google Sheets   │  │   BannerBear     │
   │ (Embeddings)     │  │  + Google Drive  │  │   (Video API)    │
   └────────┬─────────┘  └──────────────────┘  └──────────────────┘
            │
            ▼
   ┌──────────────────┐
   │   Azure SQL      │
   │ (Clients, Props, │
   │  Projects)       │
   └──────────────────┘
```

**Flow:**
1. A new lead arrives from Facebook Ads or Instagram
2. The AI agent receives the lead and responds instantly via WhatsApp or email
3. The agent uses **Vector Searcher** to look up relevant clients, properties, or projects using semantic embeddings
4. When the advisor requests a quote, the agent calls **Instant Quotes** to generate a personalized document in < 30 seconds
5. Based on the lead profile, the agent calls **Video Builder** to create persona-matched videos with the client's name
6. All interactions are logged in **Azure SQL** and surfaced through the **Retool CRM**

---

## 🔗 Resources

- [Original Repository](https://github.com/BrandonRH17/AIAgentsHackathon2025-Neutrino)
- [Video — Business Case](https://youtu.be/iKjZrwZJBUE)
- [Video — Tutorial](https://www.youtube.com/watch?v=0n6wq0PEYI4)

---

## 🚀 Next Steps

- User validation through Redis for session management
- Deployment across channels: WhatsApp, Telegram, and SMS
- Multi-tenant support for multiple real estate companies
- Analytics dashboard for lead conversion tracking

---

**Author:** Brandon Rodriguez
**Last Updated:** 2026-02-23
