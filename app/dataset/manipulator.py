from sqlalchemy.exc import IntegrityError
from flask import current_app
from flask_login import current_user
from app.models import User, Dataset, DatasetOperations
from app import db
import threading
import asyncio



def execute_transformation(api_args):
    """
    Interface to execute dataset transformations from the API
    """
    t = threading.Thread(target=transformation_thread,
                         args = [current_app._get_current_object(), current_user.id, api_args])


    
    t.setDaemon(False)
    t.start()

    return "Ok, got it"

def transformation_thread(app, user_id, args):
    with app.app_context():
        manipulator = Manipulator(User.query.get(user_id))
        asyncio.run(manipulator.execute_user_command(args))
        




class Manipulator(object):

    def __init__(self, user):
        # currently not used, need to think over (and coordinate with demonstrator)
        self.user = user
        self.command_dict = {"create" : self.create_dataset,
                             "add" : self.add_to_dataset,
                             "delete" : self.delete_from_dataset}
                             
        
    async def execute_user_command(self, args):
        current_app.logger.debug(args)
        await self.command_dict.get(args['command'])(**{k:v for k,v in args.items() if k!='command'})


    async def create_dataset(self, dataset_name, searches, articles, issues):
        try:
            dataset = Dataset(dataset_name = dataset_name)
            db.session.add(dataset)
            db.session.commit()
        except IntegrityError:
            raise
            
#
#        submit
#        catch unique constraint
#        create = True
#        for searches
#          if create
#             self.add_operation(create, dataset)
#             submit
#          else
#             operations.append(self.add_operation(add))
#        for articles
#
#        for issues


        
        

    async def add_to_dataset(self, args):
        raise NotImplementedError

    async def delete_from_dataset(self, args):
        raise NotImplementedError
    
