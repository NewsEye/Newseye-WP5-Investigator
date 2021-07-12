from contextlib import asynccontextmanager
import aiohttp
import asyncio
from flask import current_app
from threading import Lock
import copy
from random import randint
from config import Config
from multiprocessing import Manager


class SolrController:
    def __init__(self):
        self.session_no = 0
        self.max_session_no = Config.SOLR_MAX_SESSIONS

        self.manager = Manager() 
        self.lock = self.manager.Lock()

        #self.lock = Lock()
        
        self.global_counter = 0

    @asynccontextmanager
    async def acquire_session(self):
        try:
            with self.lock:
                self.global_counter += 1
                current_entry = copy.copy(self.global_counter)

            while self.session_no >= self.max_session_no:
                current_app.logger.debug(
                    "%d SLEEP SESSION_NO: %d" % (current_entry, self.session_no)
                )
                await asyncio.sleep((randint(10, 60)))

            with self.lock:
                self.session_no += 1

            session = aiohttp.ClientSession()
            current_app.logger.info(
                "%d IN ACQUIRE: SESSION_NO: %d" % (current_entry, self.session_no)
            )
            yield session
        except Exception as e:
            raise e
        finally:
            current_app.logger.debug("FINALLY: %s" % self.session_no)
            await self.release_session(session)
                

    async def release_session(self, session):
        await session.close()
        with self.lock:
            self.session_no -= 1
        current_app.logger.debug("RELEASE_SESSION: SESSION_NO: %d" % self.session_no)
