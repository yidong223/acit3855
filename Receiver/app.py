import connexion
from connexion import NoContent
import datetime
import json
import logging
import logging.config
import pykafka
from pykafka import KafkaClient
import requests
import uuid
import yaml 

def process_event(event, endpoint):
    trace_id = str(uuid.uuid4())
    event['trace_id'] = trace_id

    # TODO: call logger.debug and pass in message "Received event <type> with trace id <trace_id>"
    logger.debug(f'Received event {endpoint} with {trace_id}')

    headers = { 'Content-Type': 'application/json' }

    # TODO: update requests.post to use app_config property instead of hard-coded URL
    with open("app_conf.yml", "r") as ymlfile:
        app_config = yaml.safe_load(ymlfile)

    # app_config[f"{endpoint}"]['url']

    res = requests.post(url=f"{app_config[f'{endpoint}']['url']}", data=json.dumps(event), headers=headers)
    
    # TODO: call logger.debug and pass in message "Received response with trace id <trace_id>, status code <status_code>"
    logger.debug(f"Received response with trace id {trace_id}, status code {res.status_code}")

    return res.text, res.status_code

# Endpoints
def buy(body):
    process_event(body, 'buy')
    return NoContent, 201

def sell(body):
    process_event(body, 'sell')
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8080)