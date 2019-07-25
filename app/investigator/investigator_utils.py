# TODO: planner plans the task according to the task dependencies tree
#  Later on this will become an investigator
class Planner(object):
    """
    This class is not used for anything. Here as a remainder of the planned structure for the planner, to be removed
    when TaskPlanner is ready
    """
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
