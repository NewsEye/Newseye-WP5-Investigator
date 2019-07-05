import asyncio
import aiohttp
from config import Config
from flask import current_app
from werkzeug.exceptions import Unauthorized


# Runs the query/queries using aiohttp. The return value is a list containing the results in the corresponding order.
async def search_database(queries, **kwargs):
    return_list = isinstance(queries, list)
    if not isinstance(queries, list):
        queries = [queries]
    tasks = []
    async with aiohttp.ClientSession() as session:
        # if queries: current_app.logger.info("Log, appending searches: {}".format(queries))
        for query in queries:
            tasks.append(query_solr(session, query, **kwargs))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # if results: current_app.logger.info("Searches finished, returning results")
    if return_list:
        return results
    else:
        return results[0]


async def query_solr(session, query, retrieve='all', max_return_value=100000):
    """
    :param session: an aiohttp ClientSession
    :param query: query to be run on the solR server
    :param retrieve: what kind of data should be retrieved. Current options are 'facets', 'docids' and 'all'
              facets: retrieves just the facet information on the results
              docids: retrieves all docids that match the query. If query doesn't specify the number of rows to be fetched, all matching rows are retrieved
              all: retrieves (almost) all metadata for the documents matching the search. Only useless fields, such as the various access control fields are ignored.
                   By default this retrieves only the first 10 matches, but this can be changed by specifying a desired value using the 'rows' parameter
    :return:
    """
    # First read the default parameters for the query
    parameters = {key: value for key, value in Config.SOLR_PARAMETERS['default'].items()}
    # If parameters specific to the chosen retrieve value are found, they override the defaults
    if retrieve in Config.SOLR_PARAMETERS.keys():
        for key, value in Config.SOLR_PARAMETERS[retrieve].items():
            parameters[key] = value
    # Parameters specifically defined in the query override everything else
    for key, value in query.items():
            parameters[key] = value
    #current_app.logger.debug("QUERY_SOLR: %s" %parameters)
    #current_app.logger.debug("retrieve %s" %retrieve)
    async with session.get(Config.SOLR_URI, json={'params': parameters}) as response:
        if response.status == 401:
            raise Unauthorized
        response = await response.json()   
    # For retrieving docids, retrieve all of them, unless the number of rows is specified in the query
    if retrieve in ['docids', 'words'] and 'rows' not in query.keys():
        num_results = response['response']['numFound']
        # Set a limit for the maximum number of documents to fetch at one go to 10000
        parameters['rows'] = min(num_results, max_return_value)
        if num_results > max_return_value:
            current_app.logger.debug("too many raws to return, returnung %d" %max_return_value)
        async with session.get(Config.SOLR_URI, json={'params': parameters}) as response:
            if response.status == 401:
                raise Unauthorized
            response = await response.json()
    result = {'numFound': response['response']['numFound'], 'docs': response['response']['docs'], 'facets': format_facets(response['facet_counts']['facet_fields'])}
    return result


def format_facets(facet_dict):
    """
    Change the facet format returned by solR {"language_ssi": ["de", 1977, "fi", 29], ...} into the format used by
    the NewsEye demonstrator:
    [{"name": "language_ssi",
      "items": [{"value": "de", "hits": 1977},
                {"value": "fi", "hits": 29}]
     },
     ...
    ]
    """
    labels = {
        'language_ssi': 'Language Ssi',
        'member_of_collection_ids_ssim': 'Newspaper',
        'year_isi': 'Year',
        'has_model_ssim': 'Type',
        'date_created_dtsi': 'Date',
    }
    facet_list = [{'name': name,
                   'items': [{'value': value,
                              'hits': hits,
                              'label': value}
                             for value, hits in zip(itemlist[::2], itemlist[1::2])],
                   'label': labels[name]}
                  for name, itemlist in facet_dict.items()]
    return facet_list
