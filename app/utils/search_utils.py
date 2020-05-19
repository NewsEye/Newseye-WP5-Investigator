import asyncio
import aiohttp
from config import Config
from flask import current_app
from werkzeug.exceptions import Unauthorized


# Runs the query/queries using aiohttp. The return value is a list containing the results in the corresponding order.
async def search_database(queries, **kwargs):
    current_app.logger.debug("QUERIES: %s" % queries)
    return_list = isinstance(queries, list)
    if not isinstance(queries, list):
        queries = [queries]
    tasks = []
    async with aiohttp.ClientSession() as session:
        # if queries: current_app.logger.info("Log, appending searches: {}".format(queries))
        for query in queries:
            tasks.append(query_solr(session, query, **kwargs))
        results = await asyncio.gather(
            *tasks, return_exceptions=(not current_app.debug)
        )
        # if results: current_app.logger.info("Searches finished, returning results")
    if return_list:
        return results
    else:
        return results[0]


async def query_solr(
    session, query, retrieve="all", max_return_value=Config.SOLR_MAX_RETURN_VALUES,
):
    #### TODO: store queries and outputs, check if output exists, and reuse

    current_app.logger.debug("============== QUERY: %s RETRIEVE: %s" %(query, retrieve))

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

    # TODO: might need to store query outputs if queries become too heavy

    if retrieve in ["tokens", "stems"]:
        solr_index = "tvrh"
    else:
        solr_index = "select"

    solr_uri = Config.SOLR_URI + solr_index

    # First read the default parameters for the query
    parameters = {
        key: value for key, value in Config.SOLR_PARAMETERS["default"].items()
    }
    # If parameters specific to the chosen retrieve value are found, they override the defaults
    if retrieve in Config.SOLR_PARAMETERS.keys():
        for key, value in Config.SOLR_PARAMETERS[retrieve].items():
            parameters[key] = value
    # Parameters specifically defined in the query override everything else
    for key, value in query.items():
        parameters[key] = value

    async with session.get(solr_uri, json={"params": parameters}) as response:
        current_app.logger.debug("SOLR_URI %s" % solr_uri)
        current_app.logger.debug("parameters %s" % parameters)
        current_app.logger.debug("response.status: %s" % response.status)

        if response.status == 401:
            raise Unauthorized
        response = await response.json()
        # current_app.logger.debug("RESPONSE in search_utils: %s" %response)

    if retrieve in ["tokens", "stems"]:
        num_results = response["response"]["numFound"]
        current_app.logger.debug("!!!!NUM_RESULTS %d" %num_results)
        if num_results > max_return_value:
            current_app.logger.info(
                "TOO MANY ROWS TO RETURN, returning %d" % max_return_value
            )
            num_results=max_return_value

        # TODO: parameter, move to config
        # keeping them here is easier to debug, I don't know what is the best numbers
        rows_in_one_query = 20
        pages_in_parallel=5
        pages = []
        result_dict = {}
        page_count = 0
        for start in range(0, num_results, rows_in_one_query):
            parameters["start"] = start
            parameters["rows"] = rows_in_one_query
            #current_app.logger.debug("START: %d" %start)
            #current_app.logger.debug("parameters %s" % parameters)
            pages.append(parameters.copy())
            page_count += 1
            if page_count >= pages_in_parallel:
                for response in asyncio.as_completed([get_response(session, solr_uri, page) for page in pages]):
                    response = await response
                    result_dict = convert_vector_response_to_dictionary(response["termVectors"], result_dict)
                    current_app.logger.debug("RESULT_DICT %d" %len(result_dict))
                pages = []
                page_count = 0
                
        # Last batch:
        for response in asyncio.as_completed([get_response(session, solr_uri, page) for page in pages]):
            response = await response
            result_dict = convert_vector_response_to_dictionary(response["termVectors"], result_dict)
            current_app.logger.debug("RESULT_DICT %d" %len(result_dict))
    
        return result_dict

    # For retrieving docids, retrieve all of them, unless the number of rows is specified in the query
    if retrieve in ["docids"] and "rows" not in query.keys():
        num_results = response["response"]["numFound"]
        # Set a limit for the maximum number of documents to fetch at one go to 100000
        parameters["rows"] = min(num_results, max_return_value)
        if num_results > max_return_value:
            current_app.logger.info(
                "TOO MANY ROWS TO RETURN, returning %d" % max_return_value
            )

        async with session.get(solr_uri, json={"params": parameters}) as response:
            if response.status == 401:
                raise Unauthorized
            response = await response.json()

    result = {
        "numFound": response["response"]["numFound"],
        "docs": response["response"]["docs"],
        "facets": format_facets(response["facet_counts"]["facet_fields"]),
    }
    return result


async def query_size(query):
    return 1


async def get_response(session, solr_uri, parameters):
    async with session.get(solr_uri, json={"params": parameters}) as response:
        return await response.json()

def convert_vector_response_to_dictionary(term_vectors, result_dict):
    # bunch of hacks
    for article in term_vectors:
        if article[0] == "uniqueKey":
            article_id = article[1]
            word_list = article[3]
            article_dict = {}
            for i in range(0, len(word_list), 2):
                word = word_list[i]
                info = word_list[i + 1]
                word_dict = {}
                for j in range(0, len(info), 2):
                    field = info[j]
                    value = info[j + 1]
                    if field == "positions":
                        word_dict[field] = value[1::2]
                    elif field == "offsets":
                        word_dict[field] = [
                            (value[k], value[k + 2]) for k in range(1, len(value), 4)
                        ]
                    else:
                        word_dict[field] = value
                article_dict[word] = word_dict
            result_dict[article_id] = article_dict
    # current_app.logger.debug("RESULT_DICT %s" %result_dict)
    return result_dict


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
        "language_ssi": "Language Ssi",
        "member_of_collection_ids_ssim": "Newspaper",
        "year_isi": "Year",
        "has_model_ssim": "Type",
        "date_created_dtsi": "Date",
        "linked_persons_ssim" : "Person",
        "linked_locations_ssim" : "Location",
        "linked_organisations_ssim" : "Organizations"        
    }
    facet_list = [
        {
            "name": name,
            "items": [
                {"value": value, "hits": hits, "label": value}
                for value, hits in zip(itemlist[::2], itemlist[1::2])
            ],
            "label": labels[name],
        }
        for name, itemlist in facet_dict.items()
    ]
    return facet_list
