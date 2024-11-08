import logging
from typing import Optional

import redis


class RedisSetup:
    def __init__(self, url: str = "redis://localhost:6379/0"):
        self.url = url
        self.redis: Optional[redis.Redis] = None
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("redis_setup")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def connect(self) -> bool:
        """Establish connection to Redis"""
        try:
            self.redis = redis.Redis.from_url(self.url)
            # Test the connection
            self.redis.ping()
            self.logger.info("Successfully connected to Redis")
            return True
        except redis.exceptions.ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            return False

    def test_connection(self) -> bool:
        """Test the Redis connection and basic operations"""
        try:
            # Set a test key-value pair
            self.redis.set("test_key", "test_value")
            # Retrieve the value
            value = self.redis.get("test_key")
            # Clean up
            self.redis.delete("test_key")

            if value == b"test_value":
                self.logger.info("Redis connection test passed successfully")
                return True
            else:
                self.logger.error("Redis connection test failed")
                return False
        except redis.exceptions.RedisError as e:
            self.logger.error(f"Redis connection test failed: {e}")
            return False

    def test_expiration(self) -> bool:
        # tests for expiration
        try:
            # Setex a test key-value pair
            self.redis.setex("test_key", 10, "test_value")
            # Retrieve the value
            value = self.redis.get("test_key")

            if value == b"test_value":
                self.logger.info("Redis expiration test passed successfully")
                return True
            else:
                self.logger.error("Redis expiration test failed")
                return False
        except redis.exceptions.RedisError as e:
            self.logger.error(f"Redis expiration test failed: {e}")
            return False

    def close(self):
        """Close the Redis connection"""
        if self.redis:
            self.redis.close()
            self.logger.info("Redis connection closed")
