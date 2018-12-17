import asyncio
import aiohttp
import psycopg2
from psycopg2.extras import Json
import uuid


class BlacklightAPI(object):

    def __init__(self):
        self.baseUri = "https://demo.projectblacklight.org/catalog.json"
        self.default_params = {
            'utf8': "%E2%9C%93",
        }

    async def fetch(self, session, params={}):
        async with session.get(url=self.baseUri, params=self.fix_query_for_aiohttp(params)) as response:
            return await response.json()

    # Runs the query/queries using aiohttp. The return value is a list containing the results in the corresponding order.
    async def async_query(self, queries):
        query_is_a_list = type(queries) is list
        if not query_is_a_list:
            queries = [queries]
        tasks = []
        async with aiohttp.ClientSession() as session:
            for query in queries:
                params = self.default_params.copy()
                params.update(query)
                print("Log, appending query: {}".format(params))
                tasks.append(self.fetch(session, params))
            results = await asyncio.gather(*tasks, return_exceptions=True)
        print("Queries finished, returning results")
        return results

    # Unlike the requests package, aiohttp doesn't support key: [value_list] pairs for defining multiple values for
    # a single parameter. Instead, a list of (key, value) tuples is used.
    def fix_query_for_aiohttp(self, query):
        new_query = []
        for key in query.keys():
            if type(query[key]) is list:
                new_query.extend([(key, value) for value in query[key]])
            else:
                new_query.append((key, query[key]))
        return new_query


class PSQLAPI(object):

    def __init__(self):
        self._conn = psycopg2.connect("dbname=investigator")

    def initialize_db(self):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    CREATE TABLE users (
                        user_id serial PRIMARY KEY,
                        username varchar(255) UNIQUE NOT NULL,
                        created_on timestamptz DEFAULT NOW(),
                        last_login timestamptz
                    );
                    """)

    def add_user(self, username):
        try:
            with self._conn as conn:
                with conn.cursor() as curs:
                    curs.execute("""
                        INSERT INTO users (username)
                        VALUES (%s)
                    """, [username])
        except psycopg2.IntegrityError:
            raise TypeError("Error creating user {}: username already in use!")

    def get_last_login(self, username):
        with self._conn as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT last_login FROM users
                WHERE username = %s;
                """, [username])
                last_login = cur.fetchall()
        return last_login[0][0]

    def set_last_login(self, username, time):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    UPDATE users
                    SET last_login = %s
                    WHERE username = %s;
                """, [time, username])

    def add_query(self, username, query, parent_id=None):
        with self._conn as conn:
            with conn.cursor() as curs:
                query_id = uuid.uuid4()
                while True:
                    try:
                        curs.execute("""
                            INSERT INTO queries (query_id, user_id, query, parent_id)
                            SELECT %s, user_id, %s, %s FROM users WHERE username = %s;
                        """, [query_id, Json(query), parent_id, username])
                        break
                    except psycopg2.IntegrityError:
                        query_id = uuid.uuid4()
        return query_id

    def find_query(self, username, query):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    SELECT query_id FROM queries
                    WHERE
                        CAST (
                            query AS VARCHAR
                        ) = %s
                        AND
                        user_id IN (
                            SELECT user_id FROM users WHERE username = %s
                        );
                """, [Json(query), username])
                query_ids = [item[0] for item in curs.fetchall()]
        return query_ids

    def set_user_query(self, username, query_id):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    UPDATE users
                    SET current_query = %s
                    WHERE username = %s;
                """, [query_id, username])
