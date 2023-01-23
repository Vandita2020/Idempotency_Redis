import datetime
import logging
import os
from typing import Any, Dict, Optional

import redis

from aws_lambda_powertools.shared import constants
from aws_lambda_powertools.utilities.idempotency import BasePersistenceLayer
from aws_lambda_powertools.utilities.idempotency.exceptions import (  
    IdempotencyRedisConnectionError,
    IdempotencyItemNotFoundError,
    IdempotencyPersistenceLayerError,
)
from aws_lambda_powertools.utilities.idempotency.persistence.base import (
    STATUS_CONSTANTS,
    DataRecord,
)

logger = logging.getLogger(__name__)

class RedisCachePersistenceLayer(BasePersistenceLayer):

    def __init__(
        self, 
        host: str, 
        port: str = "6379", 
        username: Optional[str] = None, 
        password: Optional[str] = None,
        key_attr: str = "id",
        table_name: str = "db0",
        static_pk_value: Optional[str] = None,
        sort_key_attr: Optional[str] = None,
        expiry_attr: str = "expiration",
        in_progress_expiry_attr: str = "in_progress_expiration",
        status_attr: str = "status",
        data_attr: str = "data",
        validation_key_attr: str = "validation",
    ):
        """
        Initialize the Redis client
        """
        self._connection = None
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        if sort_key_attr == key_attr:
            raise ValueError(f"key_attr [{key_attr}] and sort_key_attr [{sort_key_attr}] cannot be the same!")

        if static_pk_value is None:
            static_pk_value = f"idempotency#{os.getenv(constants.LAMBDA_FUNCTION_NAME_ENV, '')}"
    
        self.key_attr = key_attr
        self.static_pk_value = static_pk_value
        self.sort_key_attr = sort_key_attr
        self.expiry_attr = expiry_attr
        self.in_progress_expiry_attr = in_progress_expiry_attr
        self.status_attr = status_attr
        self.data_attr = data_attr
        self.validation_key_attr = validation_key_attr
        super(RedisCachePersistenceLayer, self).__init__()

    @property
    def connection(self):
        """
        Caching property to store redis connection
        """
        print("Test")
        if self._connection:
            return self._connection

        self._connection = redis.Redis(host=self.host, port=self.port) #, username=self.username, password=self.password)
        try:
            self._connection.ping()
        except redis.exceptions.ConnectionError as exc:
            logger.debug(f"Cannot connect in Redis Host: {self.host}")
            raise IdempotencyRedisConnectionError
        return self._connection

    @connection.setter
    def connection(self, connection):
        """
        Allow redis connection variable to be set directly, primarily for use in tests
        """
        self._connection = connection
    
    def _get_key(self, idempotency_key: str) -> dict:
        if self.sort_key_attr:
            return {self.key_attr: self.static_pk_value, self.sort_key_attr:idempotency_key}
        return {self.key_attr: idempotency_key}

    def _item_to_data_record(self, item: Dict[str,Any]) -> DataRecord:
        return DataRecord(
            idempotency_key=item[self.key_attr],
            status=item[self.status_attr],
            expiry_timestamp=item[self.expiry_attr],
            in_progress_expiry_timestamp=item.get(self.in_progress_expiry_attr),
            response_data=item.get(self.data_attr),
            payload_hash=item.get(self.validation_key_attr),
        )

    def _get_record(self, idempotency_key) -> DataRecord:
        response = self.connection.get(idempotency_key)

        try:
            item = response
        except KeyError:
            raise IdempotencyItemNotFoundError
        return self._item_to_data_record(item)

    def _put_record(self, data_record: DataRecord) -> None:
        try:
            logger.debug(f"Putting record in Redis for idempotency key: {data_record.idempotency_key}")
            vandita = self.connection.set(data_record.idempotency_key, "")
            print(vandita)
        except:
            raise IdempotencyPersistenceLayerError

    def _delete_record(self, data_record: DataRecord) -> None:
        logger.debug(f"Deleting record in Redis for idempotency key: {data_record.idempotency_key}")
        self.connection.delete(data_record.idempotency_key)

    def _update_record(self, data_record: DataRecord) -> None:
        logger.debug(f"Updating record in Redis for idempotency key: {data_record.idempotency_key}")
        update_expression = "SET #response_data = :response_data, #expiry = :expiry, #status = :status"
        expression_attr_values = {
            ":expiry": data_record.expiry_timestamp,
            ":response_data": data_record.response_data,
            ":status": data_record.status,
        }
        expression_attr_names = {
            "#expiry": self.expiry_attr,
            "#response_data": self.data_attr,
            "#status": self.status_attr,
        }

        if self.payload_validation_enabled:
            update_expression += ", #validation_key = :validation_key"
            expression_attr_values[":validation_key"] = data_record.payload_hash
            expression_attr_names["#validation_key"] = self.validation_key_attr

        self.connection.set(data_record.idempotency_key, data_record.response_data)