# User Research & Stakeholder Discovery

[Back to Project README](../README.md)

---

## Target Audiences

Three distinct user groups consumed the data products, each with different needs, technical literacy, and decision-making authority:

| Audience | Who They Are | What They Need | How They Use Data |
|----------|-------------|----------------|-------------------|
| **Marketing Teams** | Internal analysts building campaign performance reports | Fast access to post-purchase satisfaction scores segmented by region, store, and time period | Query the lakehouse directly via SQL or BI tools; need granular, filterable data |
| **Executive Leadership** | C-suite and VPs making strategic decisions | High-level KPIs, trend lines, and competitive benchmarks — delivered on-demand without analyst dependency | Self-service dashboards; need pre-built views that answer "how are we doing?" in under 30 seconds |
| **Client-Facing Teams** | Account managers presenting insights to retail/restaurant clients | White-labeled satisfaction reports customized per client, delivered on a recurring schedule | Auto-generated reports; need templated outputs that look polished without manual formatting |

---

## Discovery Process

Understanding these audiences required structured, recurring conversations — not one-off interviews. The following cadences were established:

### Weekly

| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Sprint Planning | PM + 8 engineers + solutions architect | Define sprint goals, assign stories, surface blockers |
| Engineering Standup (async) | PM + engineering team | Daily progress updates via Slack standup bot, sync calls 3x/week |
| Product Sync | PM + solutions architect | Align on technical feasibility of upcoming roadmap items, review architecture decisions |

### Biweekly

| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Sprint Review & Demo | PM + engineers + stakeholders | Demo completed work, collect feedback, validate direction |
| Retrospective | PM + engineering team | Process improvements, velocity review, team health check |
| Client Success Alignment | PM + client-facing team leads | Review upcoming client deliverables, prioritize report templates, surface client pain points |

### Monthly

| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Executive Product Review | PM + VP of Product + VP of Engineering | Roadmap progress, cost savings update, adoption metrics, strategic pivots |
| Marketing Data Review | PM + marketing analytics lead | Review data quality, discuss new segmentation needs, validate data model changes |
| Architecture Decision Review | PM + solutions architect + senior engineers | Evaluate ADRs (Architecture Decision Records), approve schema changes, plan migration phases |

---

## Key Insights from Research

1. **Marketing teams were losing 4-6 hours/week** waiting for Denodo queries to return results on large satisfaction datasets — this became the primary justification for lakehouse migration priority
2. **Executives didn't want dashboards — they wanted answers.** The self-service tool was redesigned from a "build your own dashboard" approach to a "pick your question, get your answer" model
3. **Client-facing teams needed consistency, not flexibility.** They preferred standardized report templates over customizable ones, because their clients valued visual consistency across quarterly deliverables

---

[Back to Project README](../README.md)
