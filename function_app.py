import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="requests",
    connection="QueueAzureWebJobsStorage"
)

def QueueTriggerReportReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    logging.info(f"message: {record}")
