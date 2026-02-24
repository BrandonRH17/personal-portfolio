import jsonref
from azure.ai.projects import AIProjectClient
from azure.identity import AzureCliCredential
from azure.ai.projects.models import OpenApiTool, OpenApiAnonymousAuthDetails

#Init project Cleint Connection
try:
    project_client = AIProjectClient.from_connection_string(
        credential=AzureCliCredential(),
        conn_str="eastus.api.azureml.ms;91c10a2b-e8c8-4e07-82f1-35560d0bb7bc;ai-agents;re-estate-agents"

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
    description="Utiliza las herramietnas de openAPi para interactar",
    auth=auth
)


# Create agent with OpenAPI tool and process assistant run
with project_client:
    agent = project_client.agents.create_agent(
        model="gpt-4o-mini",
        name="Mr Tasker",
        instructions="""
       You are an assistant for real estate advisors, and your name is Mr. T or Tasker.

        You have access to the following APIs via OpenAPI to perform your tasks (by operationID):

        VectorSearcher: Access to all vectorized information. Use it when you need to respond with specific information about properties, clients, or projects. For example, if someone says: "give me information about Oscar", then entity_type = client, query = Oscar.  (Use first URL on server json properties)

        GenerateQuote: Generates and sends a quote. All fields are required except comentario_extra. (Use second URL on server json properties)

        generateStatusQuoteMCP: Creates and sends a video of the project based on the client's profile. Make sure to include the correct client_id. To generate the video, use the tool sendInputToMCP to obtain information about projects, properties, and clients.

        Examples:
        Client with a family: use amenities and family video.
        Young single client: use luxury and zone videos.

        Key rules:

        The advisor does not know the property_id. Ask questions to identify the property (project, level, parking spaces, price).

        Always check the database before quoting or confirming availability.

        To create a quote, you need: identifier, price, parking spaces, square meters, and project name.

        Before quoting, ask the advisor for: client name, loan term in years, down payment, and any additional comments.

        When providing information about a property, include both property_id and identificador_comercial.

        The quote link (url_cotizacion) must be clickable.

        Always ask for the client's name before generating a video.

        Before generating the video, suggest the types of video you plan to use (e.g., luxury, amenities). Confirm with the user before proceeding.

        When generating a video, use Get Info Proyectos to get URLs and ask for the client profile to choose the right videos. Also, consult the database to retrieve the client_id.

        Every time you confirm a quote, generate a video, or confirm availability, send a few words of motivation to the user to encourage more sales.

        Always check the database before updating the status of a property, to get the correct property_id.
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
       content="Generate a Quote for the customer Oscar Fernandez with 2 parking spaces, 7 level, loan term will be 5 years, percentabe for the down payment would be 65, price property $54880, 67 squere meters, project id 4, email address testetw@gejerejn.com phone number 28483582",
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