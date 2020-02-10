import heapq


class Investigator:
    def __init__(self, documentset, **user_parameters):
        self.root_set = documentset
        self.task_queue = self.initialize_queue(**user_parameters)
        self.action_id = 0
        self.to_stop = False

    def describe_documentset(self):
        pass

    def run(self):
        while not self.to_stop:
            self.select_tasks()
            self.execute_tasks()
            self.update_queue()
            self.check_for_stop()
        self.stop()

    # ACTIONS
    # recorded in DB for Explainer

    def initialize_queue(self, **user_parameters):
        self.describe_documentset()
        # create tasks in the database
        # put tasks into the taskq
        # initialize run in db
        return None

    def select_tasks(self):
        self.action_id += 1
        pass

    def execute_tasks(self):
        self.action_id += 1
        pass

    def update_queue(self):
        self.action_id += 1
        pass

    def stop(self):
        self.action_id += 1
        pass

    def check_for_stop(self):
        if True:
            self.to_stop = True
