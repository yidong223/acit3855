import connexion
from connexion import NoContent
import datetime
import json
import logging
import logging.config
import requests
import yaml
import time
import apscheduler
from apscheduler.schedulers.background import BackgroundScheduler

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from base import Base
from stats import Stats

DB_ENGINE = create_engine("sqlite:///stats.sqlite")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def get_latest_stats(): # just reads from latest record of stats.sqlite
    # is run manually through localhost:8100/ui

    # TODO create a session
    session = DB_SESSION()
    # TODO query the session for the first Stats record, ordered by Stats.last_updated
    rows = session.query(Stats).order_by(Stats.last_updated.desc()).first()
    # TODO if result is not empty, convert it to a dict and return it (with status code 200)
    if rows != None:
        row_dict = rows.to_dict()
        return row_dict, 200
    
    # TODO if result is empty, return NoContent, 201
    else:
        return NoContent, 201

def populate_stats(): # makes get call to storage
    # runs every 5 seconds
    # hits GET buy and GET sell at the same time

    now = datetime.datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%SZ')

    logger.debug(f"Beginning of processing")
    # print("hello")
    # request made to mysql db 

    buy = requests.get(url=f"{app_config['buy_url']}?timestamp='{timestamp}'")
    sell = requests.get(url=f"{app_config['sell_url']}?timestamp='{timestamp}'")
    # print(f"{app_config['sell_url']}?timestamp='{f'{timestamp}'}'")

    # print(sell.text)

    buy_events = json.loads(buy.text)
    sell_events = json.loads(sell.text)

    # print(buy_events)

    # for event in buy_events:
    #     print(event)

    # return 0

    new_buys = 0
    new_sells = 0
    temp_max_buy = 0
    temp_max_sell = 0


    # query for max price in sqllite first
    session = DB_SESSION()
    stored = session.query(func.max(Stats.max_buy_price).label("max_buy"),
                            func.max(Stats.max_sell_price).label("max_sell"),
                            Stats.num_buys.label("num_buy"), Stats.num_sells.label("num_sell"))
    
    res = stored.one()
    max_buy = res.max_buy
    max_sell = res.max_sell
    num_buys = res.num_buy
    num_sells = res.num_sell

    print(max_buy, max_sell, num_buys, num_sells, timestamp)

    for event in buy_events:
        new_buys += 1
        if event['item_price'] > temp_max_buy:
            temp_max_buy = event['item_price']

    for event in sell_events:
        new_sells += 1
        if event['item_price'] > temp_max_sell:
            temp_max_sell = event['item_price']
    
    num_buys += new_buys
    num_sells += new_sells
    
    if temp_max_buy > max_buy and temp_max_sell > max_sell:
        stats = Stats(temp_max_buy, num_buys, temp_max_sell, num_sells, timestamp)
        session.add(stats)
        session.commit()
        session.close()
        # add temp_max_buy into sqllite
    elif temp_max_buy > max_buy:
        stats = Stats(temp_max_buy, num_buys, max_sell, num_sells, timestamp)
        session.add(stats)
        session.commit()
        session.close()
    elif temp_max_sell > max_sell:
        stats = Stats(max_buy, num_buys, temp_max_sell, num_sells, timestamp)
        session.add(stats)
        session.commit()
        session.close()
    else:
        stats = Stats(max_buy, num_buys, max_sell, num_sells, timestamp)
        session.add(stats)
        session.commit()
        session.close()
        
        # add temp_max_sell intosqllite

    # TODO write a new Stats record to stats.sqlite using timestamp and the statistics you just generated
    # session.add(stats)
    # session.commit()
    # session.close()

    # TODO add, commit and optionally close the session

    # update num_buys, num_sells


    


    # information from GET gets processed and then stored into local SQLITE db (stats.sqlite) 
    #   IMPORTANT: all stats calculated by populate_stats must be CUMULATIVE
    # 
    #   The number of buy and sell events received must be added to the previous total held in stats.sqlite,
    #   and the max_buy_price and max_sell_price must be compared to the values held in stats.sqlite
    #
    #   e.g. if the latest Stat in the sqlite db is:
    #   { 19.99, 10, 10.99, 5, 2023-01-01T00:00:00Z }  (max_buy_price, num_buys, max_sell_price, num_sells, timestamp)
    #
    #   and 4 new buy events are received with a max price of 99.99, the next stat written to stats must be similar to:
    #   { 99.99, 14, 10.99, 5, 2023-01-01T00:00:05Z }
    #
    #   if 10 more sell events were also received, but the max price of all events did not exceed 10.99, the stat would
    #   then look something like:
    #   { 99.99, 14, 10.99, 15, 2023-01-01T00:00:05Z }
    #


    # TODO create a timestamp (e.g. datetime.datetime.now().strftime('...'))
    # TODO create a last_updated variable that is initially assigned the timestamp, i.e. last_updated = timestamp

    # TODO log beginning of processing

    # TODO create a session

    # TODO read latest record from stats.sqlite (ordered by last_updated)
    # e.g. result = session.query(Stats)...  etc.

    # TODO if result is not empty, convert result to a dict, read the last_updated property and store it in a variable named last_updated
    # if result does not exist, create a dict with default keys and values, and store it in the result variable

    # TODO call the /buy GET endpoint of storage, passing last_updated
    # TODO convert result to a json object, loop through and calculate max_buy_price of all recent records
    
    # TODO call the /sell GET endpoint of storage, passing last_updated
    # TODO convert result to a json object, loop through and calculate max_sell_price of all recent records

    # TODO write a new Stats record to stats.sqlite using timestamp and the statistics you just generated
    
    # TODO add, commit and optionally close the session

    return NoContent, 201

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['period'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)
