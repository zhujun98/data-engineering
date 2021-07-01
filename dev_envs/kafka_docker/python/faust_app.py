import asyncio
import configparser
import os.path as osp

import faust

config = configparser.ConfigParser()
config.read(osp.join(osp.dirname(__file__), '..', 'config.ini'))

BROKER_ENDPOINT = config['CLUSTER']['BROKER_ENDPOINT']
TOPIC = config['TOPIC1']['NAME']

app = faust.App("hello-world-faust", broker=BROKER_ENDPOINT)


@app.agent(app.topic(TOPIC))
async def print_purchases(purchases):
    async for p in purchases:
        print(p)


if __name__ == "__main__":
    app.main()
