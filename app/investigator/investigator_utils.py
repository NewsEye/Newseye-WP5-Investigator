

class Investigator(object):
    
    async def __init__(self, task):
        current_app.logger.debug("HERE INVESTIGATIONS START")
        self.input_data = await search_database(task.search_query, retrieve=retrieve)
        current_app.logger.debug(self.input_data)
        return
        

    async def plan(self, task):
        results = []
        while not self.satisfied(results):
            research_plan = self.plan_the_research(task, results)
            results = self.async_analysis(research_plan)
        return results

    @staticmethod
    def satisfied(task, results):
        return True

    @staticmethod
    def plan_the_research(task, results):
        return []  # return a list of new tasks



'''
asyncio.as_completed(aws, *, loop=None, timeout=None)
Run awaitable objects in the aws set concurrently. Return an iterator of Future objects. Each Future object returned represents the earliest result from the set of the remaining awaitables.

Raises asyncio.TimeoutError if the timeout occurs before all Futures are done.

Example:

for f in as_completed(aws):
    earliest_result = await f
    # ...
'''
