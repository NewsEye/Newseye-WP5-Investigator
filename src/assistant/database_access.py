import asyncio
import aiohttp
import psycopg2
from psycopg2.extras import Json, execute_values, register_uuid
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
        register_uuid()

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

    def add_query(self, username, query, parent_id=None, query_result={'message': 'Not yet ready'}):
        query_id = uuid.uuid4()
        while True:
            try:
                with self._conn as conn:
                    with conn.cursor() as curs:
                        curs.execute("""
                            INSERT INTO queries (query_id, user_id, query, parent_id, result)
                            SELECT %s, user_id, %s, %s, %s FROM users WHERE username = %s;
                        """, [query_id, Json(query), parent_id, Json(query_result), username])
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
                        query = %s
                        AND
                        user_id = (
                            SELECT user_id FROM users WHERE username = %s
                        );
                """, [Json(query), username])
                result = curs.fetchall()
        if not result:
            return None
        return [item[0] for item in result]

    def set_current_query(self, username, query_id):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    UPDATE users
                    SET current_query = %s
                    WHERE username = %s;
                """, [query_id, username])

    def get_current_query_id(self, username):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    SELECT current_query
                    FROM users
                    WHERE username = %s;
                """, [username])
                current_query_id = curs.fetchone()
        if not current_query_id:
            return None
        return current_query_id[0]

    def get_current_query(self, username):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    SELECT query_id, query, result, parent_id FROM queries
                    WHERE query_id = (
                        SELECT current_query
                        FROM users
                        WHERE username = %s
                    );
                """, [username])
                current_query = curs.fetchone()
        if not current_query:
            return None
        return dict(zip(['query_id', 'query', 'result', 'parent_id'], current_query))

    def get_query_by_id(self, username, query_id):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    SELECT query_id, query, result, parent_id FROM queries
                    WHERE 
                        query_id = %s
                        AND
                        user_id = (
                            SELECT user_id FROM users WHERE username = %s
                        );
                """, [query_id, username])
                query = curs.fetchone()
        if not query:
            return None
        return dict(zip(['query_id', 'query', 'result', 'parent_id'], query))

    def get_user_queries(self, username):
        with self._conn as conn:
            with conn.cursor() as curs:
                curs.execute("""
                    SELECT query_id, query, result, parent_id FROM queries
                    WHERE 
                        user_id IN (
                            SELECT user_id FROM users WHERE username = %s
                        );
                """, [username])
                queries = curs.fetchall()
        if not queries:
            return None
        history = {}
        for item in queries:
            history[item[0]] = dict(zip(['query_id', 'query', 'result', 'parent_id'], item))
        return history

    def add_queries(self, query_list):
        query_list = [(item['username'], Json(item['query']), item['parent_id'], Json(item['result'])) for item in query_list]
        id_list = [uuid.uuid4() for item in query_list]
        while True:
            try:
                with self._conn as conn:
                    with conn.cursor() as curs:
                        execute_values(curs, """
                            INSERT INTO queries (query_id, user_id, query, parent_id, result)
                            SELECT query_id, user_id, query, parent_id, result 
                            FROM users INNER JOIN (VALUES %s) AS data (query_id, username, query, parent_id, result)
                            ON users.username = data.username
                        """, [(i, *q) for i, q in zip(id_list, query_list)], template='(%s::uuid, %s, %s::jsonb, %s::uuid, %s::json)')
                        break
            except psycopg2.IntegrityError:
                id_list = [uuid.uuid4() for item in query_list]
            except Exception:
                print(Exception)
        return id_list

    def update_results(self, query_list):
        with self._conn as conn:
            with conn.cursor() as curs:
                execute_values(curs, """
                    UPDATE queries
                    SET result = data.result
                    FROM (VALUES %s) AS data (query_id, result)
                    WHERE queries.query_id = data.query_id 
                """, [(item['query_id'], Json(item['result'])) for item in query_list], template='(%s::uuid, %s::json)')
