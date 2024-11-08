from setup_redis import RedisSetup


def main():
    redis = RedisSetup()
    if redis.connect():
        redis.test_connection()
        redis.test_expiration()
        redis.close()


if __name__ == "__main__":
    main()
