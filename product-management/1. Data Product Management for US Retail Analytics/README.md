# Data Product Management for US Retail Analytics
### Leading Data Platform Migration and Self-Service Reporting for Post-Purchase Analytics

---

## 📋 Project Overview (STAR Format)

### 🎯 Situation

A US-based analytics company specialized in **post-purchase customer satisfaction** for the restaurant, retail, and grocery industries. The company provided actionable insights to clients based on customer feedback data — but the underlying data infrastructure was fragile and expensive.

The existing architecture relied heavily on **Denodo** (a third-party data virtualization layer) to serve data to internal teams and client-facing reports. This created three critical problems:

- **High licensing costs** — Denodo represented a significant recurring expense with limited flexibility
- **Slow response times** — data virtualization added latency to queries, making it harder to deliver timely insights to clients
- **Limited scalability** — the existing setup could not support the growing demand for new reporting products and analytics offerings

### 📝 Task

As **Data Product Manager**, I was responsible for:

- **Owning the data product roadmap** — defining what data products the company would build, in what order, and why
- **Managing an 8-person offshore engineering team** — coordinating daily standups, sprint planning, backlog grooming, and delivery across time zones
- **Collaborating with a solutions architect** — aligning on technical decisions for the new data platform architecture
- **Eliminating the Denodo dependency** — migrating data access patterns to an in-house solution without disrupting existing client deliverables
- **Building self-service reporting tools** — enabling client executives to generate their own reports without relying on the analytics team

### ⚙️ Action

**Team & Process:**

| Aspect | Detail |
|--------|--------|
| Team | 8 offshore engineers + 1 solutions architect |
| Methodology | Agile (2-week sprints) |
| Project Management | Jira (backlog, sprints, epics) |
| Documentation | Confluence (technical specs, ADRs, runbooks) |
| Duration | 12 months |

**Key Initiatives:**

**1. Data Lakehouse Migration (Denodo → Databricks)**
- Designed and led the migration from Denodo's data virtualization layer to a **Databricks lakehouse** with **medallion architecture** (Bronze → Silver → Gold)
- Coordinated with the solutions architect on schema design, data modeling, and access patterns
- Managed the engineering team through the phased migration — ensuring zero downtime for existing client reports
- Used **Spark SQL** for large-scale data transformations across the pipeline layers

**2. Internal Tooling for Data Source Independence**
- Led the development of internal tools to replace Denodo's data source abstraction
- Built custom data access layers that connected directly to the lakehouse, eliminating the virtualization middleman
- Prioritized and sequenced the tooling roadmap based on which data sources had the highest query volume and cost impact

**3. Self-Service Reporting Suite**
- Defined requirements and led development of **auto-reporting tools** for client executives
- Designed the product so that non-technical users could generate satisfaction analytics reports on demand
- Expanded the company's analytics offering — clients could now access a broader suite of reporting solutions without custom development per request

### 🎯 Result

**Key Achievements:**

✅ **Six-Figure Annual Cost Reduction**
- Eliminated the Denodo licensing dependency entirely
- Reduced infrastructure costs in the six-figure range annually by consolidating onto the Databricks lakehouse

✅ **Faster Response Times**
- Removed the data virtualization latency layer
- Internal teams and client reports now query directly from the optimized lakehouse, significantly reducing time-to-insight

✅ **Expanded Analytics Offering**
- Delivered self-service reporting tools that enabled client executives to generate reports independently
- Broadened the company's product suite — from custom one-off reports to a scalable, self-serve analytics platform

✅ **Successful Offshore Team Delivery**
- Managed 8 offshore engineers across time zones for 12 months
- Maintained consistent sprint velocity and on-time delivery throughout the migration
- Established clear documentation practices in Confluence for knowledge transfer and onboarding

✅ **Clean Data Architecture**
- Delivered a production-ready medallion architecture on Databricks
- Bronze (raw ingestion) → Silver (cleaned, standardized) → Gold (business-ready aggregations)
- Provided a scalable foundation for future data products and analytics use cases

---

## 👥 User Research & Stakeholder Discovery

### Target Audiences

Three distinct user groups consumed the data products, each with different needs, technical literacy, and decision-making authority:

| Audience | Who They Are | What They Need | How They Use Data |
|----------|-------------|----------------|-------------------|
| **Marketing Teams** | Internal analysts building campaign performance reports | Fast access to post-purchase satisfaction scores segmented by region, store, and time period | Query the lakehouse directly via SQL or BI tools; need granular, filterable data |
| **Executive Leadership** | C-suite and VPs making strategic decisions | High-level KPIs, trend lines, and competitive benchmarks — delivered on-demand without analyst dependency | Self-service dashboards; need pre-built views that answer "how are we doing?" in under 30 seconds |
| **Client-Facing Teams** | Account managers presenting insights to retail/restaurant clients | White-labeled satisfaction reports customized per client, delivered on a recurring schedule | Auto-generated reports; need templated outputs that look polished without manual formatting |

### Discovery Process

Understanding these audiences required structured, recurring conversations — not one-off interviews. The following cadences were established:

**Weekly:**
| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Sprint Planning | PM + 8 engineers + solutions architect | Define sprint goals, assign stories, surface blockers |
| Engineering Standup (async) | PM + engineering team | Daily progress updates via Slack standup bot, sync calls 3x/week |
| Product Sync | PM + solutions architect | Align on technical feasibility of upcoming roadmap items, review architecture decisions |

**Biweekly:**
| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Sprint Review & Demo | PM + engineers + stakeholders | Demo completed work, collect feedback, validate direction |
| Retrospective | PM + engineering team | Process improvements, velocity review, team health check |
| Client Success Alignment | PM + client-facing team leads | Review upcoming client deliverables, prioritize report templates, surface client pain points |

**Monthly:**
| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Executive Product Review | PM + VP of Product + VP of Engineering | Roadmap progress, cost savings update, adoption metrics, strategic pivots |
| Marketing Data Review | PM + marketing analytics lead | Review data quality, discuss new segmentation needs, validate data model changes |
| Architecture Decision Review | PM + solutions architect + senior engineers | Evaluate ADRs (Architecture Decision Records), approve schema changes, plan migration phases |

### Key Insights from Research

1. **Marketing teams were losing 4-6 hours/week** waiting for Denodo queries to return results on large satisfaction datasets — this became the primary justification for lakehouse migration priority
2. **Executives didn't want dashboards — they wanted answers.** The self-service tool was redesigned from a "build your own dashboard" approach to a "pick your question, get your answer" model
3. **Client-facing teams needed consistency, not flexibility.** They preferred standardized report templates over customizable ones, because their clients valued visual consistency across quarterly deliverables

---

## 🗺️ Product Roadmap

### 12-Month Execution Timeline

```
Q1 (Months 1-3): Foundation & Discovery
──────────────────────────────────────────
Month 1  │ Stakeholder interviews across all 3 audiences
         │ Audit existing Denodo data sources (47 active connections)
         │ Define migration priority matrix (by query volume × cost impact)
         │
Month 2  │ Architecture design with solutions architect
         │ Medallion architecture (Bronze/Silver/Gold) schema design
         │ Set up Databricks workspace and CI/CD pipelines
         │
Month 3  │ Migrate top 5 highest-volume data sources (Phase 1)
         │ Validate data parity: Denodo vs. Databricks outputs
         │ First sprint demo to executive stakeholders

Q2 (Months 4-6): Core Migration
──────────────────────────────────────────
Month 4  │ Migrate next 15 data sources (Phase 2)
         │ Begin internal tooling development (data access layer)
         │ Marketing team pilot: direct lakehouse access
         │
Month 5  │ Complete remaining data source migrations (Phase 3)
         │ Internal tooling MVP — replaces 80% of Denodo queries
         │ Decommission first batch of Denodo connections
         │
Month 6  │ Full Denodo decommission
         │ Cost savings validated and reported to executives
         │ Mid-year roadmap review and reprioritization

Q3 (Months 7-9): Self-Service & Reporting
──────────────────────────────────────────
Month 7  │ Self-service reporting: requirements gathered from executives
         │ Prototype "pick your question" interface
         │ Client-facing report template engine — design phase
         │
Month 8  │ Self-service reporting MVP launched to executive team
         │ Iteration based on executive feedback (simplified navigation)
         │ Report template engine: first 5 templates in production
         │
Month 9  │ Self-service adoption tracking begins
         │ 15 report templates live for client-facing teams
         │ Gold layer optimization for most-queried KPIs

Q4 (Months 10-12): Scale & Optimize
──────────────────────────────────────────
Month 10 │ Self-service tool rolled out to full executive team
         │ Client-facing teams fully transitioned to auto-reporting
         │ Performance tuning on Spark jobs (30% query time reduction)
         │
Month 11 │ Documentation sprint: Confluence runbooks for all systems
         │ Knowledge transfer sessions for new team members
         │ Data quality monitoring framework design
         │
Month 12 │ Final executive review: full-year outcomes presentation
         │ Roadmap handoff for Year 2 initiatives
         │ Team retrospective and lessons learned
```

---

## 📊 Success Metrics

### Outcome Metrics (Business Impact)

| Metric | Before | After | Impact |
|--------|--------|-------|--------|
| **Annual infrastructure cost** | Denodo license + maintenance | Databricks pay-per-use | **Six-figure annual savings** |
| **Query response time (avg)** | 45-90 seconds (Denodo virtualization) | 5-15 seconds (direct lakehouse) | **~80% reduction** |
| **Time to generate client report** | 2-4 hours (manual) | 5-10 minutes (auto-generated) | **~90% reduction** |
| **Analytics products available** | 3 standard reports | 15+ templates + self-service | **5x expansion** |
| **Analyst hours on report generation** | ~20 hrs/week across team | ~3 hrs/week (review only) | **~85% reduction** |

### Process Metrics (Team Health)

| Metric | Target | Achieved |
|--------|--------|----------|
| Sprint velocity consistency | ±15% variance | ±12% variance across 24 sprints |
| On-time delivery rate | 80% of committed stories | 87% average over 12 months |
| Blocker resolution time | < 48 hours | 36 hours average |
| Documentation coverage | All production systems | 100% — Confluence runbooks for every component |
| Team retention | Maintain full team for project duration | 8/8 engineers retained for 12 months |

### Adoption Metrics (Product Usage)

| Metric | Q3 Launch | Q4 End | Trend |
|--------|-----------|--------|-------|
| Executive self-service weekly active users | 4 | 12 | Growing |
| Client reports generated via auto-reporting | 15/month | 60+/month | Growing |
| Support tickets for report requests | 25/week | 4/week | Declining |
| Marketing team direct lakehouse queries | 50/week | 200+/week | Growing |

---

## 📂 Project Structure

```
1. Data Product Management for US Retail Analytics/
├── presentations/
│   └── README.md              # Case study summary and key artifacts
└── README.md                  # This file
```

> **Note:** This is a case study project. No source code is included as the work was proprietary. The documentation focuses on the product management approach, decisions made, and business outcomes delivered.

---

## 🔧 Product Management Approach

```
Product Roadmap          Stakeholder Alignment         Engineering Delivery
─────────────────        ──────────────────────        ────────────────────

Define data products  →  Align with solutions      →  Sprint planning with
and prioritization       architect on technical         8 offshore engineers
                         feasibility

Sequence migration    →  Validate approach with    →  Phased migration
phases by impact         client success team            execution

Design self-service   →  Review with executives    →  Iterative delivery
reporting features       for adoption readiness         and testing

Measure outcomes      →  Report cost savings       →  Retrospectives and
and iterate              and adoption metrics           continuous improvement
```

---

## Technologies & Tools

| Category | Tools |
|----------|-------|
| **Project Management** | Jira (epics, sprints, backlog), Confluence (specs, ADRs) |
| **Data Platform** | Databricks, Spark SQL, Medallion Architecture |
| **Previous Stack** | Denodo (data virtualization — removed) |
| **Data** | SQL, post-purchase satisfaction datasets |
| **Methodology** | Agile Scrum, 2-week sprints |

---

## 🔗 Key Decisions

### Why Databricks over Denodo?

| Factor | Denodo (Before) | Databricks (After) |
|--------|----------------|-------------------|
| **Cost** | High recurring license fees | Pay-per-use compute, no virtualization license |
| **Latency** | Virtualization adds query overhead | Direct lakehouse queries, optimized storage |
| **Scalability** | Limited by virtualization layer capacity | Elastic compute, scales with data volume |
| **Ownership** | Dependent on third-party abstractions | Full control over data models and pipelines |
| **Analytics** | Reports required custom development | Self-service tools enable executive reporting |

### Why a Single Migration Instead of Parallel Systems?

- Running Denodo and Databricks in parallel would have doubled infrastructure costs during the transition
- A phased migration (data source by data source, ordered by query volume) minimized risk while delivering incremental cost savings
- Each phase was validated with the client success team before proceeding to the next

---

## 🚀 Next Steps

- Expand self-service reporting to include predictive satisfaction scoring
- Introduce data quality monitoring and alerting across the lakehouse
- Evaluate real-time streaming ingestion for near-instant satisfaction feedback

---

**Author:** Brandon Rodriguez
**Last Updated:** 2026-02-24
