from app.analysis.processors import AnalysisUtility
from app.models import Processor
from config import Config

class ExtractFacets(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Examines the document set given as input, and finds all the different facets for which values have been set in at least some of the documents.",
            parameter_info=[],
            input_type="dataset",
            output_type="facet_list",
        )

    async def make_result(self):
        """ Extract all facet values found in the input data and the number of occurrences for each."""
        # too complicated
        # seems nobody is using these Config.constants
        # except for this processor --- get read of them???
        facets = {}
        for feature in self.input_data[Config.FACETS_KEY]:
            values = {}
            for item in feature[Config.FACET_ITEMS_KEY]:
                values[item[Config.FACET_VALUE_LABEL_KEY]] = item[Config.FACET_VALUE_HITS_KEY]

            if feature[Config.FACET_ID_KEY] in Config.AVAILABLE_FACETS.values():
                facets[feature[Config.FACET_ID_KEY]] = values

        return facets
