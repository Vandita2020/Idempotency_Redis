# from aws_lambda_powertools.utilities.idempotency import (
#     DynamoDBPersistenceLayer, idempotent
# )

# persistence_layer = DynamoDBPersistenceLayer(table_name="idempotency-example-table")

# @idempotent(persistence_store=persistence_layer)
# def lambda_handler(event, context):
#     return {
#         "message": "success",
#         "statusCode": 200,
#     }
import time
from os import environ
from uuid import uuid5
import sys

from aws_lambda_powertools.utilities.idempotency import (
    idempotent,
    RedisCachePersistenceLayer,
    IdempotencyConfig
)    

persistence_layer = RedisCachePersistenceLayer(host="host.docker.internal", port="6379")
config =  IdempotencyConfig(
    expires_after_seconds=1*60,  # 1 minutes
)

@idempotent(config=config, persistence_store=persistence_layer)
def lambda_handler(event, context):
    return {"message":"Vandita"}