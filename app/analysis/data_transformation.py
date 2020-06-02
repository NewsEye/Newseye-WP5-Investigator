from app.models import Processor, Task
from app.analysis.processors import AnalysisUtility
from app.utils.search_utils import search_database
from app.analysis import assessment
from flask import current_app
from app.analysis.facet_processors import AVAILABLE_FACETS
from copy import copy
import asyncio
import numpy as np


class SplitByFacet(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Split dataset by a given facet",
            parameter_info=[
                {
                    "name": "facet",
                    "description": "facet used to split data, could be LANGUAGE, NEWSPAPER_NAME or PUB_YEAR",
                    "type": "string",
                    "default": "LANGUAGE",
                    "required": False,
                }
            ],
            input_type="facet_list",
            output_type="dataset_list",
        )

    async def get_input_data(self, previous_task_result):
        return previous_task_result[self.task.parameters["facet"]]

    async def make_result(self):
        # if len(self.input_data) == 1:
        #     return []
        facet_field = AVAILABLE_FACETS[self.task.parameters["facet"]]

        current_app.logger.debug("******FACET_FIELD: %s" % facet_field)

        search_query = self.task.search_query
        fq = search_query.get("fq", [])
        if isinstance(fq, str):
            fq = [fq]
        queries = {}
        for f in self.input_data:
            q = copy(search_query)
            q["fq"] = [*fq, "{}:{}".format(facet_field, f)]
            queries[f] = q

        return queries

    async def estimate_interestingness(self):
        interestingness = {
            f: 1 - d if d > 0.5 else d
            for f, d in assessment.recoursive_distribution(self.input_data).items()
        }
        return {
            f: 2 * interestingness[f]
            for f in sorted(interestingness, key=interestingness.get, reverse=True)
        }

    async def _estimate_interestingness(self):
        interestingness = await self.estimate_interestingness()
        interestingness.update(
            {"overall": assessment.normalized_entropy(self.input_data.values())}
        )
        return interestingness



class Comparison(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Special type of the utility which takes as an input a list of tasks with the same input type and finds the difference.",
            parameter_info=[
                {
                    "name": "task_ids",
                    "description": "The list of tasks with the same output type, refered by id",
                    "type": "task_list",
                    "default": [],
                    "required": False,
                },
                {
                    "name": "task_uuids",
                    "description": "The list of tasks with the same output type, refered by uuid",
                    "type": "task_list",
                    "default": [],
                    "required": False,
                }
            ],
            input_type="task_id_list",
            output_type="comparison",
        )

    
    
    async def get_input_data(self):
        if self.task.parameters.get('task_ids'):
            tasks = Task.query.filter(Task.id.in_(task.parameters['task_ids'])).all()
        elif self.task.parameters.get('task_uuid'):
            tasks = TaskInstance.query.filter(TaskInstance.uuid.in_(task.utility_parameters['task_uuids'])).all()
                            
        else:
            raise BadRequest('Request missing valid task_uuids or task_ids!')


        wait_time = 0
        while any([task.task_status != 'finished' for task in tasks]) and wait_time < 100:
            asyncio.sleep(wait_time)
            wait_time += 1
            
        tasks = [task.task for task in tasks]
        input_data_type = [task.output_type for task in tasks]
        try:
            assert(len(set(input_data_type))==1)
        except AssertionError:
            raise BadRequest('All input tasks must have the same output types')
        self.data_type = input_data_type[0]
        
        return [task.task_result.result for task in tasks]
        
    async def make_result(self):
        dicts = [self.make_dict(data) for data in self.input_data]
        if len(dicts) > 2:
            raise NotImplementedError("At the moment comparison of more than two results is not supported")
        js_divergence = assessment.dict_js_divergence(dicts[0], dicts[1])
        return {'result': {'jensen_shannon_divergence':js_divergence},
                'interestingness' : {'jensen_shannon_divergence':js_divergence}}
        
        
    def make_dict(self, data):
        if self.data_type == 'tf_idf':
            return self.make_ipm_dict(data)
        elif self.data_type == 'facet_list':
            return self.make_facet_dict(data)
        elif self.data_type == 'topic_analysis':
            return self.make_topic_dict(data)
        else:
            raise NotImplementedError("Unknown data_type: %s" %self.data_type)

    @staticmethod
    def make_ipm_dict(tf_idf_output):
        return {k:v['ipm'] for k,v in tf_idf_output.items()}

    @staticmethod
    def make_facet_dict(facet_list_output):
        facet_dict = {f['facet_value']:f['document_count'] for f in facet_list_output}
        total = float(sum(facet_dict.values()))
        return {k:v/total for k,v in facet_dict.items()}

    @staticmethod
    def make_topic_dict(topic_analysis_output):
        return dict(enumerate(topic_analysis_output['topic_weights']))

        
    
