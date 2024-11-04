# Create a new file setup_db.py in your scripts directory
from setup_mongodb import MongoDBSetup


def main():
    mongodb = MongoDBSetup()
    if mongodb.connect():
        mongodb.setup_database()
        mongodb.test_connection()
        mongodb.close()


if __name__ == "__main__":
    main()
