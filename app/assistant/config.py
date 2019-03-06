# Configuration parameters:

DATABASE_IN_USE = 'demo'

# DATABASE_IN_USE = 'newseye'

if DATABASE_IN_USE == 'demo':

    AVAILABLE_FACETS = {
        'PUB_YEAR': 'pub_date_ssim',
        'TOPIC': 'subject_ssim',
        'ERA': 'subject_era_ssim',
        'REGION': 'subject_geo_ssim',
        'LANGUAGE': 'language_ssim',
        'FORMAT': 'format',
    }

if DATABASE_IN_USE == 'newseye':

    AVAILABLE_FACETS = {
        'DOCUMENT_LANGUAGE_FACET': 'language_ssi',
        'NEWSPAPER_NAME_FACET': 'member_of_collection_ids_ssim',
        'PUB_DATE': 'date_created_dtsi'
    }
