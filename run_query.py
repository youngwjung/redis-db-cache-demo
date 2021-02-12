import os
import json
import time

import redis
import pymysql


class DB:
    def __init__(self, **params):
        params.setdefault("charset", "utf8mb4")
        params.setdefault("cursorclass", pymysql.cursors.DictCursor)

        self.mysql = pymysql.connect(**params)

    def query(self, sql):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


# Time to live for cached data
TTL = 3600

# Read the Redis credentials from the REDIS_URL environment variable.
REDIS_URL = os.environ.get('REDIS_URL')

# Read the DB credentials from the DB_* environment variables.
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_NAME = os.environ.get('DB_NAME')

# Initialize the database
Database = DB(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME)

# Initialize the cache
Cache = redis.Redis.from_url(REDIS_URL)


def fetch(sql):
    """Retrieve records from the cache, or else from the database."""
    res = Cache.get(sql)

    if res:
        return [True, json.loads(res)]

    res = Database.query(sql)
    Cache.setex(sql, TTL, json.dumps(res))
    return [False, res]


# Display the result of some queries
start = time.time()
result = fetch("SELECT * FROM salaries")
end = time.time()
if result[0]:
    print(result[1])
    print("Cache Found")
else:
    print(result[1])
    print("Cache not found")

print(f'Time elapsed: {end - start} sec')