import logging
from datetime import datetime
from typing import Optional

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


class MongoDBSetup:
    def __init__(self, uri: str = "mongodb://localhost:27017/"):
        self.uri = uri
        self.client: Optional[MongoClient] = None
        self.db_name = "content_aggregator"
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("mongodb_setup")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def connect(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            # Test the connection
            self.client.admin.command("ping")
            self.logger.info("Successfully connected to MongoDB")
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def setup_database(self) -> bool:
        """Set up the database and collections with proper indexes"""
        try:
            db = self.client[self.db_name]

            # Create collections
            collections = {
                "tasks": self._setup_tasks_collection,
                "queries": self._setup_queries_collection,
                "sources": self._setup_sources_collection,
                "content": self._setup_content_collection,
                "results": self._setup_results_collection,
            }

            for name, setup_func in collections.items():
                self.logger.info(f"Setting up collection: {name}")
                setup_func(db)

            self.logger.info("Database setup completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to set up database: {e}")
            return False

    def _setup_tasks_collection(self, db):
        """Set up tasks collection with indexes"""
        collection = db.tasks
        collection.create_index([("status", 1)])
        collection.create_index([("celery_task_id", 1)], unique=True)
        collection.create_index([("created_at", 1)])

    def _setup_queries_collection(self, db):
        """Set up queries collection with indexes"""
        collection = db.queries
        collection.create_index([("subject", 1)])
        collection.create_index([("status", 1)])
        collection.create_index([("created_at", 1)])
        collection.create_index([("cache_key", 1)], unique=True)

    def _setup_sources_collection(self, db):
        """Set up sources collection with indexes"""
        collection = db.sources
        collection.create_index([("url", 1)], unique=True)
        collection.create_index([("domain", 1)])
        collection.create_index([("last_scraped", 1)])

    def _setup_content_collection(self, db):
        """Set up content collection with indexes"""
        collection = db.content
        collection.create_index([("query_id", 1)])
        collection.create_index([("source_id", 1)])
        collection.create_index([("cache_key", 1)], unique=True)
        collection.create_index([("status", 1)])

    def _setup_results_collection(self, db):
        """Set up results collection with indexes"""
        collection = db.results
        collection.create_index([("query_id", 1)])
        collection.create_index([("created_at", 1)])
        collection.create_index([("cache_key", 1)], unique=True)

    def test_connection(self) -> bool:
        """Test the database connection and basic operations"""
        try:
            # Insert a test document
            db = self.client[self.db_name]
            test_collection = db.test
            test_doc = {"test": True, "timestamp": datetime.utcnow()}
            result = test_collection.insert_one(test_doc)

            # Verify insertion
            found = test_collection.find_one({"_id": result.inserted_id})

            # Clean up
            test_collection.delete_one({"_id": result.inserted_id})

            self.logger.info("Database connection test passed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False

    def close(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")
