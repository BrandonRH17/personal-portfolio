
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential
import time
import os

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

# Template configuration
templates = Jinja2Templates(directory="templates")

# Set Agent to Use
AGENT_ID = os.environ["AZURE_AGENT_ID"]

# Init Client Project
try:
    project_client = AIProjectClient.from_connection_string(
        credential=DefaultAzureCredential(),
        conn_str=os.environ["AZURE_AI_PROJECT_CONNECTION_STRING"]
    )
except Exception as e:
    print("Connection error:", e)

thread = project_client.agents.create_thread()
print(f"Thread created, ID: {thread.id}")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/send_message")
async def send_message(request: Request):
    body = await request.json()
    user_input = body.get("user_input")

    if not user_input:
        return JSONResponse({"assistant_response": "User not send a message."})

    try:
        project_client.agents.create_message(thread_id=thread.id, role="user", content=user_input)
        run = project_client.agents.create_run(thread_id=thread.id, agent_id=AGENT_ID)

        while True:
            run = project_client.agents.get_run(thread_id=thread.id, run_id=run.id)
            if run.status in ['completed', 'failed', 'cancelled']:
                break
            time.sleep(1)

        if run.status != "completed":
            return JSONResponse({"assistant_response": "Sorry, I didn't find anything about your request, try again"})

        messages = project_client.agents.list_messages(thread_id=thread.id)
        assistant_response = ""

        for msg in sorted(messages['data'], key=lambda x: x['created_at'], reverse=True):
            if msg['role'] == 'assistant':
                content_list = msg.get('content', [])
                for content in content_list:
                    if 'text' in content:
                        assistant_response = content['text'].get('value', 'Not response')
                break

        return JSONResponse({"assistant_response": assistant_response})

    except Exception as e:
        print("Error:", e)
        return JSONResponse({"assistant_response": "Internal Error"})
