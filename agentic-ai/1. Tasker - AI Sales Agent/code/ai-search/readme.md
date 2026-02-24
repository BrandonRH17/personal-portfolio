# AI Search Integration

<p align="center">
  <img src="https://azure.microsoft.com/svghandler/search/?width=600&height=315" alt="AI Search" width="600" height="315">
</p>

Our **AI Search** is directly connected to our **MySQL** database, allowing seamless and intelligent retrieval of information across three key tables:

## 1. Clients Table (`clients`)
This table contains information about leads who have shown interest in our projects. It includes the following fields:
- **Name**: Full name of the client
- **Phone Number**: Contact number of the client
- **Email Address**: Email address provided by the client
- **Investment Type**: The type of investment the client is interested in (e.g., residential, commercial)
- **Interested Project**: The project the client is most interested in
- **Preferred Contact Method**: The preferred communication method (e.g., WhatsApp, email, phone call)

## 2. Properties Table (`properties`)
This table contains detailed information about available properties. It includes:
- **Project**: The project to which the property belongs
- **Property Identifier**: A unique identifier for each property
- **Price**: The listing price of the property in USD
- **Square Meters**: The area of the property in square meters
- **Number of Rooms**: Total number of rooms
- **Number of Parking Spaces**: Total number of parking spots included
- **Floor Plan URL**: A link to the property's floor plan document

## 3. Projects Table (`projects`)
This table stores information related to each project we offer:
- **Project Name**: The official name of the project
- **Multimedia Content URLs**: Links to the project's promotional videos
- **Brochure URL**: A link to download the project brochure

## Embedding Strategy

We use **OpenAI's large embeddings** to perform **vector-based search** across our data. (text-embedding-3-large)

The reasons for choosing this embedding type are:
- **Higher precision**: Property and project information can often be very similar, and large embeddings allow us to retrieve the most accurate match for the client's query.
- **Low query volume**: Each user typically generates a small number of queries, so using a higher-quality embedding does not result in significant cost increases.
- **Short input texts**: The texts we embed (e.g., property descriptions, project details) are relatively short, making the use of large embeddings **cost-efficient**.

---

Thanks to this architecture, AI Search can deliver **fast**, **accurate**, and **relevant** responses, helping clients quickly find the properties or projects that best fit their needs.
