import uuid
from datetime import datetime
from azure.cosmos import CosmosClient, exceptions
from typing import Any, Dict, List
# from flask import current_app
import logging

logger = logging.getLogger(__name__)


class CosmosDatabaseAPI:
    _instances = {}  # Dictionary to store instances per (url, db_name)

    def __new__(cls, url: str, key: str, db_name: str):
        # Create a unique key for this specific connection
        connection_key = (url, db_name)

        if connection_key not in cls._instances:
            logger.info(f"Creating New Singleton Instance for DB: {db_name}")
            instance = super(CosmosDatabaseAPI, cls).__new__(cls)

            # Store connection details on the instance
            instance.url = url
            instance.key = key
            instance.db_name = db_name
            instance.connect()

            cls._instances[connection_key] = instance

        return cls._instances[connection_key]

    def connect(self):
        """Establish or re-establish the physical connection."""
        try:
            self.client = CosmosClient(self.url, credential=self.key)
            self.db = self.client.get_database_client(self.db_name)
            logger.info(f"Connected to Cosmos DB: {self.db_name}")
        except Exception as e:
            logger.error(f"Critical: Failed to connect to {self.db_name}: {repr(e)}")
            raise

    def _get_container(self, container_name):
        """Heartbeat check: ensures the specific DB connection is still alive."""
        try:
            container = self.db.get_container_client(container_name)
            # Heartbeat check
            container.read_item(item="healthcheck", partition_key="id")
            return container
        except exceptions.CosmosResourceNotFoundError:
            return container # Alive, just no healthcheck item
        except Exception as e:
            logger.warning(f"Connection for {self.db_name} stale. Reconnecting...")
            self.connect()
            return self.db.get_container_client(container_name)
        
    def execute_raw_query(self, container_name, query, params=None):
        try:
            # Pass the container name dynamically
            container = self._get_container(container_name)
            
            results = list(
                container.query_items(
                    query=query,
                    parameters=params or [],
                    enable_cross_partition_query=True,
                )
            )
            return results

        except Exception as e:
            print(f"DEBUG ERROR: {e}")
            logger.error(f"Cosmos query execution failed: {repr(e)}")
            return None

    def dbGet(self, collection_name, query_or_filters, params=None):
        """
        Versatile getter:
        - If query_or_filters is a dict: Performs a simple 'AND' equality check.
        - If query_or_filters is a string: Executes it as a raw SQL query.
        """
        try:
            container = self._get_container(collection_name)
            
            # CASE 1: Raw SQL Query string passed from outside
            if isinstance(query_or_filters, str):
                query = query_or_filters
                parameters = params or []
            
            # CASE 2: Dictionary of filters passed
            elif isinstance(query_or_filters, dict):
                if not query_or_filters:
                    query = "SELECT * FROM c"
                    parameters = []
                else:
                    where_clause = " AND ".join([f"c.{k}=@{k}" for k in query_or_filters.keys()])
                    query = f"SELECT * FROM c WHERE {where_clause}"
                    parameters = [{"name": f"@{k}", "value": v} for k, v in query_or_filters.items()]
            
            else:
                raise ValueError("query_or_filters must be a string (SQL) or a dict (Filters)")

            return list(
                container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                )
            )
        except Exception as e:
            # Note: Ensure logger is imported or use current_app.logger
            logger.error(f"Cosmos get failed: {repr(e)}") 
            return None

    def dbInsert(self, collection_name, item):
        try:
            # 1. Use the heartbeat check to get the container
            container = self._get_container(collection_name)

            # 2. Ensure Cosmos-required 'id' exists
            if "id" not in item:
                item["id"] = str(uuid.uuid4())

            # 3. Serialize datetimes (Cosmos doesn't support raw datetime objects)
            for k, v in item.items():
                if isinstance(v, datetime):
                    item[k] = v.isoformat()

            # 4. Insert
            return container.create_item(body=item)
        except Exception as e:
            # Use class-level logger or current_app.logger
            logger.error(f"Cosmos insert failed into {collection_name}: {repr(e)}")
            return None

    def dbInsertMany(self, collection, items: List[Dict[str, Any]]):
        try:
            container = self._container(collection)

            for item in items:
                if "id" not in item:
                    item["id"] = str(uuid.uuid4())

                for k, v in item.items():
                    if isinstance(v, datetime):
                        item[k] = v.isoformat()

                container.create_item(body=item)  # queued

            return items
        except Exception as e:
            logger.error(f"Cosmos bulk insert failed: {repr(e)}")
            return None
        
    def dbUpdate(self, collection, keyValues):
        try:
            container = self._container(collection)

            if "id" not in keyValues:
                raise ValueError("Cosmos update requires 'id'")

            # Convert datetime fields
            for k, v in keyValues.items():
                if isinstance(v, datetime):
                    keyValues[k] = v.isoformat()

            container.replace_item(
                item=keyValues["id"],
                body=keyValues
            )

            return True
        except Exception as e:
            logger.error(f"Cosmos update failed: {repr(e)}")
            return False
    
    def dbUpdateWithPull(self, collection, keyValues, pullValues):
        try:
            item = self.dbGetOne(collection, keyValues)
            if not item:
                return False

            for field, value in pullValues.items():
                if field in item and isinstance(item[field], list):
                    item[field] = [v for v in item[field] if v != value]

            self._container(collection).replace_item(item["id"], item)
            return True
        except Exception as e:
            logger.error(f"Cosmos pull update failed: {repr(e)}")
            return False

    def dbGetOne(self, collection, keyValues=None, params=None, conditions=None):
        """
        Generic GetOne:
        :param collection: Name of the collection (string).
        :param keyValues: DICT for simple equality OR STRING for raw SQL.
        :param params: List of dicts for SQL parameters (only if keyValues is string).
        :param conditions: Extra SQL logic string (only if keyValues is dict).
        """
        try:
            # 1. Use the heartbeat-protected container getter
            container = self._get_container(collection)
            
            query = ""
            parameters = []

            # Case 1: Raw SQL Query String
            if isinstance(keyValues, str):
                query = keyValues
                parameters = params if params else []
                # Optimization: If query doesn't have TOP 1, you could inject it, 
                # but usually, we trust the caller's SQL string.

            # Case 2: Dictionary based filtering
            else:
                filters = []
                if keyValues:
                    for k, v in keyValues.items():
                        filters.append(f"c.{k}=@{k}")
                        parameters.append({"name": f"@{k}", "value": v})
                
                if conditions:
                    filters.append(conditions)

                where_clause = " WHERE " + " AND ".join(filters) if filters else ""
                # Ensure we only fetch one record for efficiency
                query = f"SELECT TOP 1 * FROM c{where_clause}"

            # 2. Execute Query
            items = list(
                container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                )
            )
            
            return items[0] if items else None

        except Exception as e:
            # Use current_app.logger or the class logger
            logger.error(f"Cosmos dbGetOne failed on {collection}: {repr(e)}")
            return None

    def dbCount(self, collection, keyValues):
        try:
            where_clause = " AND ".join(
                [f"c.{k}=@{k}" for k in keyValues.keys()]
            )
            query = f"SELECT VALUE COUNT(1) FROM c WHERE {where_clause}"
            params = [{"name": f"@{k}", "value": v} for k, v in keyValues.items()]

            result = list(
                self._container(collection).query_items(
                    query=query,
                    parameters=params,
                    enable_cross_partition_query=True,
                )
            )
            return result[0] if result else 0
        except Exception:
            return 0
        
    def dbDeleteOne(self, collection, keyValues):
        try:
            item = self.dbGetOne(collection, keyValues)
            if not item:
                return None

            self._container(collection).delete_item(
                item=item["id"],
                partition_key=item["partitionKey"],
            )
            return item
        except Exception as e:
            logger.error(f"Cosmos delete failed: {repr(e)}")
            return None
        
    def dbDelete(self, container_name: str, item_id: str, partition_key: Any):
        """
        Deletes a single document from the specified container.
        """
        try:
            # Reuses your existing heartbeat/reconnect logic
            container = self._get_container(container_name)
            
            container.delete_item(item=item_id, partition_key=partition_key)
            logger.info(f"Deleted item {item_id} from {container_name}")
            return True
        except exceptions.CosmosResourceNotFoundError:
            logger.warning(f"Item {item_id} not found in {container_name}. Already deleted?")
            return False
        except Exception as e:
            logger.error(f"Cosmos delete failed for ID {item_id}: {repr(e)}")
            return False

    def dbGetN(self, collection, keyValues=None, max: int = 10, offset: int = 0, params=None, conditions=None):
        """
        Generic GetN (Paginated):
        :param keyValues: DICT for simple equality OR STRING for raw SQL.
        :param max: Number of records to return (LIMIT).
        :param offset: Number of records to skip (OFFSET).
        :param params: Parameter list for raw SQL or custom conditions.
        :param conditions: Extra WHERE logic string.
        """
        try:
            container = self._container(collection)
            parameters = params if params else []

            if isinstance(keyValues, str):
                # Case 1: Raw SQL String
                query = keyValues
            else:
                # Case 2: Dictionary/Hybrid building
                filters = []
                if keyValues:
                    for k, v in keyValues.items():
                        filters.append(f"c.{k}=@{k}")
                        parameters.append({"name": f"@{k}", "value": v})
                
                if conditions:
                    filters.append(conditions)

                where_clause = " WHERE " + " AND ".join(filters) if filters else ""
                
                # Cosmos DB uses OFFSET and LIMIT for pagination
                # Note: OFFSET and LIMIT are mandatory together in Cosmos SQL
                query = (f"SELECT * FROM c{where_clause} "
                        f"OFFSET @offset LIMIT @limit")
                
                parameters.append({"name": "@offset", "value": offset})
                parameters.append({"name": "@limit", "value": max})

            items = list(
                container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                )
            )
            return items

        except Exception as e:
            logger.error(f"Cosmos dbGetN generic failed: {repr(e)}")
            return []

    def dbAggregate(self, collection, keyValues):
        # If not used, raise error or return empty
        raise NotImplementedError("dbAggregate not implemented for Cosmos yet.")

    def dbCollections(self):
        # List all containers in the database
        try:
            return [container['id'] for container in self.db.list_containers()]
        except Exception as e:
            logger.error(f"dbCollections failed: {e}")
            return []

    def dbUpsert(self, collection_name, item):
        """Upserts an item (creates if new, replaces if exists). Ideal for Kafka streams."""
        try:
            container = self._get_container(collection_name)

            if "id" not in item:
                item["id"] = str(uuid.uuid4())

            for k, v in item.items():
                if isinstance(v, datetime):
                    item[k] = v.isoformat()

            return container.upsert_item(body=item)
        except Exception as e:
            logger.error(f"Cosmos upsert failed into {collection_name}: {repr(e)}")
            return None

