

class Investigator(object):
    
    def __init__(self, task):
        input_task = generate_tasks(queries=('search', search_parameters), user=self.user, parent_id=task.uuid,
                                    return_tasks=True)


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
