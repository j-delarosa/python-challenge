def unique_residences_and_shared_address(record):
    """ unique_residences_and_shared_address method

    This method provides a final report with records transformed
    which is a collection with unique residences values.
    In addition, it detects if borrower and coborrower are sharing
    the same address

    Parameters
    ----------
    record : dict{str:any}

    Returns
    -------
    dict{str:any}
        Returns a dictionary as final version of the report records as same structured
        but with residences as unique values and with a flag in Borrowers report which
        indicates if borrower and coborrower are sharing address.

    """
    unique_records = []
    for report in record['reports']:
        if report['title'] == 'Residences Report':
            for residence in report['residences']:
                if residence not in unique_records:
                    unique_records.append(residence)
            report['residences'] = unique_records
        if report['title'] == 'Borrowers Report':
            report['shared_address'] = True if len(unique_records) > 0 else False

    return record
