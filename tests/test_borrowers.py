import os
import sys
import json
import pytest

# Update Path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from handler import main
from tests.framework import generate_event


@pytest.mark.parametrize('file_name', ['tests/test_input/loandata_differentstreet.json',
                                       'tests/test_input/loandata_differentcity.json',
                                       'tests/test_input/loandata_differentstate.json',
                                       'tests/test_input/loandata_differentpostalcode.json'])
def test_borrowers_report_different_addresses_shared_address_false(file_name):
    with open(file_name) as file:
        event = generate_event(json.load(file))
    response = main(event)

    assert response.get('reports') is not None
    borrowers_report = list(filter(lambda r: r.get('title') == 'Borrowers Report', response.get('reports')))
    assert borrowers_report is not None
    assert len(borrowers_report) == 1
    assert not borrowers_report[0].get('shared_address')


def test_borrowers_report_same_address_shared_address_true():
    with open('tests/test_input/loandata_sameaddress.json') as file:
        event = generate_event(json.load(file))
    response = main(event)

    assert response.get('reports') is not None
    borrowers_report = list(filter(lambda r: r.get('title') == 'Borrowers Report', response.get('reports')))
    assert borrowers_report is not None
    assert len(borrowers_report) == 1
    assert borrowers_report[0].get('shared_address')
