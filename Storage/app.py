import datetime
import json

import connexion
from connexion import NoContent
import swagger_ui_bundle

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from buy import Buy
from sell import Sell
import logging
import logging.config
import yaml


with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# # TODO: create connection string, replacing placeholders below with variables defined in log_conf.yml
DB_ENGINE = create_engine(f"mysql+pymysql://{app_config['user']}:{app_config['password']}@{app_config['hostname']}:{app_config['port']}/{app_config['db']}") # need to replace hard coded string with app config values
# DB_ENGINE = create_engine("sqlite:///events.sqlite")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

# Endpoints
def buy(body):
    # TODO create a session
    session = DB_SESSION()

    # TODO create a Buy object and populate it with values from the body
    buy_obj = Buy(body['buy_id'], body['item_name'], body['item_price'], body['buy_qty'], body['trace_id'])
  

    # TODO add, commit, and close the session

    session.begin()
    session.add(buy_obj)
    session.commit()
    session.close()

    return NoContent, 201
# end


# TIMESTAMP %Y-%m-%dT%H:%M:%SZ

def get_buys(timestamp): # queries for everything that is past the timestamp
    # TODO create a DB SESSION
    session = DB_SESSION()

    # TODO query the session and filter by Buy.date_created >= timestamp
    # e.g. rows = session.query(Buy).filter etc...
    rows = session.query(Buy).filter(Buy.date_created >= timestamp)

    # TODO create a list to hold dictionary representations of the rows
    data = []

    # TODO loop through the rows, appending each row (use .to_dict() on each row) to 'data'
    for row in rows:
        data.append(row.to_dict())

    # TODO close the session
    session.close()
    # TODO log the request to get_buys including the timestamp and number of results returned
    logger.debug(f"Received request to GET sells {timestamp}. Number of results {len(data)}")
    return data, 200

def sell(body):
    # TODO create a session

    session = DB_SESSION()

    # TODO create a Buy object and populate it with values from the body

    sell_obj = Sell(body['sell_id'], body['item_name'], body['item_price'], body['sell_qty'], body['trace_id'])

    # TODO add, commit, and close the session

    session.begin()
    session.add(sell_obj)
    session.commit()
    session.close()

    # get_sells("2023-01-24 10:12:47Z")
    return NoContent, 201
# end

def get_sells(timestamp):
    print("hello")
    # TODO create a DB SESSION
    session = DB_SESSION()

    # TODO query the session and filter by Sell.date_created >= timestamp
    # e.g. rows = session.query(Sell).filter etc...
    rows = session.query(Sell).filter(Sell.date_created >= timestamp)

    data = []
    # TODO loop through the rows, appending each row (use .to_dict() on each row) to 'data'
    for row in rows:
        data.append(row.to_dict())

    print(data)

    # TODO close the session
    session.close()
    # TODO log the request to get_sells including the timestamp and number of results returned
    logger.debug(f"Received request to GET sells {timestamp}. Number of results {len(data)}")
    return data, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8090)