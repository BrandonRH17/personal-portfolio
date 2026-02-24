# User Research & Stakeholder Discovery

[Back to Project README](../README.md)

---

## Target Audiences

Five distinct user groups interacted with the products, each with different workflows, technical literacy, and daily pressures:

| Audience | Who They Are | What They Need | How They Use the Products |
|----------|-------------|----------------|---------------------------|
| **Operations Team** | Dispatchers and coordinators managing daily freight assignments | Real-time transport availability, pilot GPS locations, shipment status at a glance, streamlined vendor payments | CRM platform — assigning units, tracking shipments, processing invoices and payments |
| **Workshop / Mechanics** | Maintenance technicians servicing the fleet | Simple way to log unit status, track installed parts, view scheduled services, report issues | Mobile app — registering maintenance activities, updating unit readiness, managing pilot-unit assignments |
| **Pilots / Drivers** | Heavy-transport drivers on cross-border routes | Minimal friction communication, clear route instructions, timely payment confirmations | WhatsApp — receiving and responding to AI agent messages, sharing location, confirming status updates |
| **Client Logistics Teams** | Freight buyers and logistics managers at client companies | Self-service trip booking, real-time shipment tracking, border documentation access | Client web platform + WhatsApp companion — booking trips, monitoring loads, receiving checkpoint notifications |
| **Management** | Operations directors and company leadership | Fleet utilization metrics, maintenance readiness, financial oversight, client satisfaction trends | CRM dashboards — reviewing KPIs, approving payments, monitoring operational performance |

---

## Discovery Process

Understanding these audiences required embedded observation and recurring feedback loops — not just interviews. The following cadences were established:

### Weekly

| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Sprint Planning | PM + engineering team | Define sprint goals, assign stories, surface blockers |
| Operations Ride-Along | PM + dispatchers/coordinators | Shadow daily workflows to identify friction points and validate product decisions |
| Engineering Standup | PM + engineers | Daily async updates via Slack, sync calls 3x/week |

### Biweekly

| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Sprint Review & Demo | PM + engineers + operations leads | Demo completed features, collect feedback, validate usability |
| Retrospective | PM + engineering team | Process improvements, velocity review, team health check |
| Client Feedback Session | PM + client success lead | Review client-reported issues, prioritize platform improvements, track adoption |

### Monthly

| Meeting | Participants | Purpose |
|---------|-------------|---------|
| Executive Product Review | PM + operations director + finance lead | Roadmap progress, efficiency metrics, budget review, strategic pivots |
| Workshop Review | PM + workshop supervisor | Review mobile app usage, maintenance data quality, fleet readiness metrics |
| AI Agent Performance Review | PM + engineering lead | Review automation rates, escalation frequency, WhatsApp response metrics, agent accuracy |

---

## Key Insights from Research

1. **Dispatchers were switching between 4-5 disconnected tools daily** to manage assignments, track pilots, and process payments — the unified CRM eliminated this context-switching entirely
2. **Mechanics resisted digital tools initially.** The mobile app adoption only took off after simplifying the UI to 3 taps or fewer for the most common actions (log service, update unit status, assign pilot)
3. **Pilots preferred WhatsApp over any other channel.** Attempts to use email or SMS for follow-ups had near-zero engagement — WhatsApp messages had 95%+ read rates within 15 minutes
4. **Clients didn't want "more features" — they wanted fewer phone calls.** The highest-value feature was the checkpoint notification system, which eliminated the majority of "where is my load?" support calls
5. **Management needed visibility, not control.** Dashboards showing fleet utilization and payment status were used daily; approval workflows were rarely triggered, suggesting the operations team was already making good decisions

---

[Back to Project README](../README.md)
