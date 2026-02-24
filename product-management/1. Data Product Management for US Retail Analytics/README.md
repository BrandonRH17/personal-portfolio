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

## 📎 Artifacts

Detailed product management artifacts are available in the [artifacts/](artifacts/) folder:

| Artifact | Description |
|----------|-------------|
| [User Research](artifacts/user-research.md) | Target audiences (marketing, executive, client-facing), discovery process, meeting cadences, and key insights |
| [Product Roadmap](artifacts/roadmap.md) | 12-month execution timeline across 4 quarters — from foundation & discovery through scale & optimization |
| [Success Metrics](artifacts/metrics.md) | Outcome metrics (cost, speed, adoption), process metrics (team health), and product usage tracking |

---

## 📂 Project Structure

```
1. Data Product Management for US Retail Analytics/
├── artifacts/
│   ├── user-research.md       # Target audiences, discovery process, key insights
│   ├── roadmap.md             # 12-month execution timeline (Q1-Q4)
│   └── metrics.md             # Outcome, process, and adoption metrics
└── README.md                  # This file — project overview (STAR format)
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
