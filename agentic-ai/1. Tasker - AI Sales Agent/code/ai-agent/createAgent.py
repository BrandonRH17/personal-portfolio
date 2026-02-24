import os
import jsonref
from azure.ai.projects import AIProjectClient
from azure.identity import AzureCliCredential
from azure.ai.projects.models import OpenApiTool, OpenApiAnonymousAuthDetails

# Init project Client Connection
try:
    project_client = AIProjectClient.from_connection_string(
        credential=AzureCliCredential(),
        conn_str=os.environ["AZURE_AI_PROJECT_CONNECTION_STRING"]
    )
except Exception as e:
    print("Connection Error", e)

# Load OpenApi3.0 Schema
with open('./logicAppsSchema.json', 'r') as f:
    openapi_spec = jsonref.loads(f.read())

# Create Auth object for the OpenApiTool 
auth = OpenApiAnonymousAuthDetails()

# Initialize agent OpenAPI
openapi = OpenApiTool(
    name="send_message",
    spec=openapi_spec,
    description="OpenAPI tools for vector search, quote generation, and video creation",
    auth=auth
)


# Create agent with OpenAPI tool and process assistant run
with project_client:
    agent = project_client.agents.create_agent(
        model="gpt-4o-mini",
        name="Mr Tasker",
        instructions="""You are Tasker, an AI assistant for real estate sales advisors. Your goal is to help advisors respond to leads faster, generate quotes, and create personalized prospect videos.

# Tools

You have three tools available via OpenAPI:

1. **VectorSearcher** — Searches vectorized data for clients, properties, or projects.
   - Parameters: `entity_type` ("client", "property", or "project") and `query` (natural language search term).
   - Example: advisor says "find info on Oscar" → call with entity_type="client", query="Oscar".

2. **quotation** — Generates and sends a personalized property quote.
   - All fields are required except `comentarios_extras`.
   - Returns a shareable quote link.

3. **VideoBuilder** — Creates a personalized video for a client based on their profile.
   - Requires: `asset_url_1`, `asset_url_2`, `nombre_cliente`, `cliente_id`, `proyecto_id`.
   - Select video assets based on the client persona:
     - Family-oriented client → use amenities + family videos.
     - Young single client → use luxury + zone videos.

# Workflows

## Property Lookup
1. The advisor does NOT know internal property IDs. Ask clarifying questions (project name, level, parking spaces, price range) to identify the right property.
2. Use VectorSearcher with entity_type="property" to find matches.
3. Always include both `property_id` and `identificador_comercial` when presenting results.
4. Always verify availability in the database before confirming.

## Quote Generation
1. Collect from the advisor: client name, loan term (years), down payment percentage, and any additional comments.
2. Use VectorSearcher to retrieve the property details (identifier, price, parking spaces, square meters, project name).
3. Call the quotation tool with all required fields.
4. Present the quote link as a clickable URL.

## Video Generation
1. Ask for the client's name and retrieve their `client_id` via VectorSearcher.
2. Use VectorSearcher with entity_type="project" to get available video URLs.
3. Suggest which video types to use based on the client profile. Confirm with the advisor before proceeding.
4. Call VideoBuilder with the selected asset URLs and client details.

# Guidelines
- Always query the database before quoting, confirming availability, or updating property status.
- After completing a quote, video, or availability confirmation, include a brief motivational message to encourage the advisor.
- Be concise and action-oriented in your responses.
""",
        tools=openapi.definitions
    )
    print(f"Created agent, ID: {agent.id}")

    # Create thread for communication
    thread = project_client.agents.create_thread()
    print(f"Created thread, ID: {thread.id}")
    
    # Create message to thread
    message = project_client.agents.create_message(
        thread_id=thread.id,
        role="user",
       content="Generate a Quote for the customer John Doe with 2 parking spaces, 7 level, loan term will be 5 years, percentage for the down payment would be 65, price property $54880, 67 square meters, project id 4, email address test@example.com phone number 55551234",
    )
    print(f"Created message, ID: {message.id}")

    # Create and process agent run in thread with tools
    run = project_client.agents.create_and_process_run(thread_id=thread.id, agent_id=agent.id)
    print(f"Run finished with status: {run.status}")

    if run.status == "failed":
        print(f"Run failed: {run.last_error}")

    # Delete the assistant when done
    # project_client.agents.delete_agent(agent.id)
    # print("Deleted agent")

    # Fetch and log all messages
    messages = project_client.agents.list_messages(thread_id=thread.id)
    print(messages)