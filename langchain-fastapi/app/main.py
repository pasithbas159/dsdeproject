import json
import os
import uvicorn
import random
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional
import asyncio


import boto3
import time
import csv
import io
from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
import os
from langchain_core.messages import  HumanMessage

os.environ['ANTHROPIC_API_KEY'] = "<_CLAUDE_API_KEY_>"

llm = ChatAnthropic(model="claude-3-5-sonnet-20240620")

@tool
def preview_table_call(table_name) : 
    """Use this to preview table to understand database not the answer
    if you don't have enough information to answer the question 
        table is ['master_table_final_author_parquet','master_table_final_parquet'] which
        master_table_final_author_parquet for list of authors and master_table_final_parquet for tables details
    """

    return sql_query(query='''select * from {} LIMIT 2'''.format(table_name))
@tool
def sql_query(query):
    """Use this to run SQL to sever if information is not enough to answer and more understand  data."""

    region = "us-east-1"  # Update to your AWS region
    database = "data_scopus"  # Update to your Glue Data Catalog database name
    s3_output = "s3://anthenaquery/"  # Update with your S3 bucket for query results

    # Initialize Athena client
    athena_client = boto3.client("athena", region_name=region)

    # Start query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": database,
            "Catalog": "AwsDataCatalog"  # Explicitly specify AwsDataCatalog
        },
        ResultConfiguration={
            "OutputLocation": s3_output,
        },
    )

    query_execution_id = response["QueryExecutionId"]

    # Poll for query completion
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)  # Wait before checking again

    if status != "SUCCEEDED":
        return f"Query failed with status: {query_status['QueryExecution']['Status']['AthenaError']['ErrorMessage']}"

    # Retrieve query results
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract headers and rows
    rows = results.get("ResultSet", {}).get("Rows", [])
    if not rows:
        return "No data returned"

    headers = [col["VarCharValue"] for col in rows[0]["Data"]]  # First row contains headers
    data = [
        [col.get("VarCharValue", "") for col in row["Data"]]
        for row in rows[1:]
    ]

    # Convert to CSV format
    csv_output = io.StringIO()
    csv_writer = csv.writer(csv_output)
    csv_writer.writerow(headers)  # Write headers
    csv_writer.writerows(data)  # Write data rows

    # Return CSV text
    return csv_output.getvalue()


tools = [preview_table_call, sql_query]

graph = create_react_agent(llm, tools=tools, state_modifier="You task is helpful assistant to more understand table by lookup"
                           "First please understand table structure first by use preview tools in both table"
                           "Please limit sql to don't be too much queries if you know exactly answer please start with FINAL_ANSWER")

app = FastAPI()
app.mount("/demo", StaticFiles(directory="static", html=True))

@app.get("/")
async def root():
    return RedirectResponse(url='/demo/')

class Story(BaseModel):
    topic: Optional[str] = None

@app.post("/api/story")
def api_story(story: Story):
    if story.topic == None or story.topic == "":
        return None
    return StreamingResponse(simulated_stream(story.topic), media_type="text/html")

async def simulated_stream(topic: str):
    # Sample story chunks that could be randomly selected
    inputs = [HumanMessage(content=topic)]
    #text_result = ""
    async for event in graph.astream_events({"messages": inputs}, version="v1"):
        kind = event["event"]
        
        if kind == "on_chat_model_stream":
            content = event["data"]["chunk"].content
            
            if content:
                for each_content in content : 
                    if "text" in each_content.keys():
                        text = each_content['text']
                        #text_result += text
                        # Use regex to match both "FINAL ANSWER:" and "FINAL_ANSWER:"
                        # pattern = re.compile(r"(?i)\bFINAL\s+ANSWER\s*:")

                        # # Find matches
                        # matches = [match.end() for match in pattern.finditer(text_result)]
                        # # print(matches)

                        # if final_answer :
                        #     print(text, end="|")

                        # elif len(matches) > 0 :

                        #     final_answer = True
                        #     idx = text.find(":")
                        #     print(text[idx:], end="|")
                        yield text
        #yield chunk
    
    yield "\n"  # Final newline to complete the story

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))