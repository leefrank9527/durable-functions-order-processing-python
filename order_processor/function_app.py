import azure.functions as func
import azure.durable_functions as df
import logging
from time import sleep

import orjson

from models import *

CONTENT_TYPE = 'application/json'

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="orchestrators/process_orchestrator")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    req_body = req.get_json()
    print(req_body)

    instance_id = await client.start_new("process_orchestrator", client_input=req_body)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    response = client.create_check_status_response(req, instance_id)
    return response


@app.orchestration_trigger(context_name="context")
def process_orchestrator(context: df.DurableOrchestrationContext):
    # Retry options for activity functions
    retry_interval_in_milliseconds = 2000
    max_number_of_attempts = 3
    retry_options = df.RetryOptions(retry_interval_in_milliseconds, max_number_of_attempts)

    # Reserve requested inventory
    instance_id = context.instance_id
    req_fixity = context.get_input()
    req_fixity_json_bytes = orjson.dumps(req_fixity)

    parallel_tasks = []
    for i in range(100):
        options = orjson.loads(req_fixity_json_bytes)
        options['sn'] = i
        parallel_tasks.append(context.call_activity("fixity_file", options))
    results = yield context.task_all(parallel_tasks)
    context.set_custom_status(results)
    return results


@app.activity_trigger(input_name="req")
def fixity_file(req):
    rsp = req
    sleep(1)
    print(rsp)
    return rsp


# @app.route(route="worker/fixity")
# def fixity_file_service(req: func.HttpRequest) -> func.HttpResponse:
#     rsp = orjson.dumps({'success', True})
#     sleep(2)
#     print('fixity_file_service')
#     return func.HttpResponse(
#         body=rsp,
#     )
def hello_world(req: func.HttpRequest) -> func.HttpResponse:
    name = req.params.get("name", "Anonymous")
    # return func.HttpResponse(f"Hello, {name}!\r\n")
    return func.HttpResponse(
        orjson.dumps({"name": name}) + b'\r\n'
    )


@app.route(route="hello")
@app.function_name(name="HttpHello")
def http_hello(req: func.HttpRequest) -> func.HttpResponse:
    return hello_world(req)
