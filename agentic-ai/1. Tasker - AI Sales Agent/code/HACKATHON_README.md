# Tasker

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/profile_picture.jpeg" alt="Profile Picture" width="300"/>
</p>

## Reinventing how high-ticket businesses sell — with sales team assisted by AI agents that turn every customer interaction into a high-converting conversation.

---

## The Problem

### The cost of not responding fast

> ⏱️ According to a [MIT study](https://hbr.org/2011/03/the-short-life-of-online-sales-leads), companies that respond to leads within **5 minutes** are **100x more likely** to connect with them than those that wait 30 minutes or more.

Most sales teams respond hours or days later.

### Before Tasker

| 🏢 Real Estate Listing | 🚗 Automobile Dealership |
|------------------------|--------------------------|
| A customer asks about a new project. | A customer asks about a specific model. |
| The agent replies with the same generic PDF as 10 others. | The agent replies with the same generic PDF as 20 others. |
| If there's interest, they manually build a quote in Google Sheets and follow up via WhatsApp. | If interested, a manual quote is built in Google Sheets and followed up via WhatsApp. |
| Eventually, the deal may close. | Eventually, the deal may close. |

---

## Key Features

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/quote_ready.jpeg" alt="Quote Ready" width="600"/>
</p>

- **Instant Response Times:** Responds within the first 5 minutes of receiving a new lead via Facebook Ads or Instagram Forms using their preferred method (WhatsApp or Gmail).
- **Lead pre qualification:** Sends generic prequalification content to engage the lead immediately.
- **Lead notification:** Notifies the assigned real estate advisor with lead details and status update.
- **CRM Management:** Automatically creates the client in the CRM and updates the deal status.
- **Personalized Content:** Supports the advisor by generating personalized videos based on the lead persona, including the lead name in 3 minutes.
- **Quote Creation:** Creates personalized quotes based on up to date data on the available properties in less than 30 seconds.


<p align="center">
  <strong>Instant Quote Example:</strong><br>
  <img src="https://github.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/raw/main/assets/instant%20quote.png" alt="Instant Quote Example" width="600"/>
</p>

<p align="center">
  <strong>Example 1 - Personalized video: Esther</strong><br>
  <em>Family mom, interested in luxury and family amenities</em><br>
  <img src="https://github.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/raw/main/assets/Esther%20Video.gif" alt="Esther GIF" width="600"/>
</p>

<p align="center">
  <strong>Example 2 - Personalized video: Oscar</strong><br>
  <em>Young single, interested in the zone and the amenities</em><br>
  <img src="https://github.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/raw/main/assets/Oscar%20Video.gif" alt="Oscar GIF" width="600"/>
</p>


---
## Architecture

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/quote_ready_2.jpeg" alt="Quote Ready 2" width="700"/>
</p>

- **CRM:** Retool  
- **Agent Web App:** Static Web Apps  
- **Database:** Azure SQL  

- **Vector Searcher:** Azure AI Search  
- **Agent Functions:** Azure Logic Apps  
- **Agent Deployment:** Azure AI Agents  

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/Tasker%20-%20Architecture-General%20Architecture.drawio.png" alt="Tasker Architecture Diagram" width="800"/>
</p>

For more technical details, each folder in the repository documents the corresponding component in depth.

---
## Components

### Retool CRM

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/Retool%20-%20Clients.png" alt="Retool - Clients" width="600"/>
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/Retool%20-%20Projects.png" alt="Retool - Projects" width="600"/>
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/Retool%20-%20Resources.png" alt="Retool - Resources" width="600"/>
</p>


### Tasker Website

<p align="center">
  <img src="https://github.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/raw/main/assets/tasker%20web%20app.png" alt="Tasker Web App" width="600"/>
</p>

---
## 👇 Try It Yourself

Since shared drives require giving explicit access to email accounts, video generations and quotes will only be trigerred, but the testers will not be able to access the documents.

> [🔗 Chat with Tasker](https://foundryagentchat-gdfjggbpbgchhadu.mexicocentral-01.azurewebsites.net/)

> [🔗 Video - Business Case](https://youtu.be/iKjZrwZJBUE)

> [🔗 Video - Tutorial](https://www.youtube.com/watch?v=0n6wq0PEYI4) 

---

## Next Steps

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/new_lead.png" alt="Tasker - Dancing" width="600"/>
</p>

- User validation through Redis  
- Deployment across channels such as WhatsApp, Telegram, and SMS

