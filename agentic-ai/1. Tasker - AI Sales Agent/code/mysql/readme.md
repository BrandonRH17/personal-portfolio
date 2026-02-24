# Azure SQL Database

<p align="center">
  <img src="https://cdn.freelogovectors.net/svg12/azure_sql_database_logo_freelogovectors.net.svg" alt="Azure SQL Logo" width="150"/>
</p>

## Why Azure SQL?

We chose **Azure SQL Database** due to its ease of deployment, seamless integration with the rest of the Azure ecosystem, and strong support for scalability and reliability. It serves as the core data layer for all interactions within the Tasker ecosystem.

---

## How It's Used

- The database is partially filled **manually** by users through the CRM built in **Retool**, when creating new clients, projects, and properties.
- It is also **automatically updated by Tasker**, our AI agent, which:
  - Updates the status of leads and clients
  - Inserts lead data captured through **Facebook Ads** and **Instagram Lead Forms**
  - Synchronizes user interactions and follow-up status in real-time

---

## Database Structure Overview

<p align="center">
  <img src="https://raw.githubusercontent.com/BrandonRH17/AIAgentsHackathon2025-Neutrino/main/assets/Tasker%20-%20Architecture-Tables.drawio.png" alt="SQL Architecture Diagram" width="800"/>
</p>

This schema supports a modular, scalable structure for high-ticket sales operations, with separate tables for users, clients, projects, properties, updates, and tasks—all mapped to key CRM actions and AI-driven automations.
