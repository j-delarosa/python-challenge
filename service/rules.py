import hashlib
import json


def unique_residences(record):
    """ unique_residences method

    This method provides a final report with records transformed
    which is a collection with unique residences values.

    Parameters
    ----------
    record : dict{str:any}

    Returns
    -------
    dict{str:any}
        Returns a dictionary as final version of the report records as same structured
        but with residences as unique values.

    """
    unique_records = []
    residences_tokens = []
    for report in record['reports']:
        if report['title'] == 'Residences Report':
            for residence in report['residences']:
                residence_tokenized = hashlib.md5(json.dumps(residence).encode("utf-8")).hexdigest()
                if tokenized not in residences_tokens:
                    residences_tokens.append(residence_tokenized)
                    unique_records.append(residence)
            report['residences'] = unique_records

    return record
