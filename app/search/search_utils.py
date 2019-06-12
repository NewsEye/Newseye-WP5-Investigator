import asyncio
import aiohttp
from config import Config
from flask import current_app
from aiohttp.web import HTTPUnauthorized


async def fetch(session, params={}):
    async with session.get(url=Config.BLACKLIGHT_URI, params=fix_query_for_aiohttp(params)) as response:
        if response.status == 401:
            raise HTTPUnauthorized
        result_body = await response.json()
        return result_body['response']


# Runs the query/queries using aiohttp. The return value is a list containing the results in the corresponding order.
async def search_database(queries, database='demonstrator', **kwargs):
    return_list = isinstance(queries, list)
    if not isinstance(queries, list):
        queries = [queries]
    tasks = []
    if database == 'solr':
        async with aiohttp.ClientSession() as session:
            for query in queries:
                current_app.logger.info("Log, appending search: {}".format(query))
                tasks.append(query_solr(session, query, **kwargs))
            results = await asyncio.gather(*tasks)
        current_app.logger.info("Tasks finished, returning results")
        if return_list:
            return results
        else:
            return results[0]
    elif database == 'demonstrator':
        try:
            async with aiohttp.ClientSession(cookies=Config.COOKIES, headers=Config.HEADERS) as session:
                for query in queries:
                    params = Config.BLACKLIGHT_DEFAULT_PARAMETERS.copy()
                    params.update(query)
                    current_app.logger.info("Log, appending search: {}".format(params))
                    tasks.append(fetch(session, params))
                results = await asyncio.gather(*tasks)
        except HTTPUnauthorized:
            tasks = []
            Config.HEADERS['Authorization'] = await get_token()
            async with aiohttp.ClientSession(cookies=Config.COOKIES, headers=Config.HEADERS) as session:
                for query in queries:
                    params = Config.BLACKLIGHT_DEFAULT_PARAMETERS.copy()
                    params.update(query)
                    current_app.logger.info("Log, appending search: {}".format(params))
                    tasks.append(fetch(session, params))
                results = await asyncio.gather(*tasks)
        current_app.logger.info("Tasks finished, returning results")
        if return_list:
            return results
        else:
            return results[0]


# Unlike the requests package, aiohttp doesn't support key: [value_list] pairs for defining multiple values for
# a single parameter. Instead, a list of (key, value) tuples is used.
def fix_query_for_aiohttp(query):
    new_query = []
    for key in query.keys():
        if isinstance(query[key], list):
            new_query.extend([(key, value) for value in query[key]])
        else:
            new_query.append((key, query[key]))
    return new_query


async def get_token():
    async with aiohttp.ClientSession() as session:
        payload = {
            'email': Config.NEWSEYE_USERNAME,
            'password': Config.NEWSEYE_PASSWORD
        }
        async with session.post("https://platform.newseye.eu/authenticate", json=payload) as response:
            body = await response.json()
    return body['auth_token']


async def query_solr(session, query, retrieve='all'):
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
    async with session.get(Config.SOLR_URI, params=fix_query_for_aiohttp(parameters)) as response:
        if response.status == 401:
            raise HTTPUnauthorized
        response = await response.json()
    # For retrieving docids, retrieve all of them, unless the number of rows is specified in the query
    if retrieve in ['docids'] and 'rows' not in query.keys():
        num_results = response['response']['numFound']
        # Set a limit for the maximum number of documents to fetch at one go to 10000
        parameters['rows'] = min(num_results, 10000)
        async with session.get(Config.SOLR_URI, params=fix_query_for_aiohttp(parameters)) as response:
            if response.status == 401:
                raise HTTPUnauthorized
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
