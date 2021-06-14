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


@pytest.fixture()
def abs_path():
    """Forcing absolute path so that pytest can be run from anywhere"""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    test_root = os.path.join(project_root, 'tests')
    return test_root


def test_borrowers_report_multiple_applications_appends(abs_path):
    # Arrange
    with open(os.path.join(abs_path, 'test_input/loandata_multipleapplications.json')) as file:
        event = generate_event(json.load(file))

    # Act
    response = main(event)

    # Assert
    assert response.get('reports') is not None
    borrowers_report = list(
        filter(lambda r: r.get('title') == 'Borrowers Report', response.get('reports')))
    assert borrowers_report is not None
    borrowers = borrowers_report[0].get('borrowers')
    assert len(borrowers) == 4
    assert len(list(filter(lambda r: r.get('first_name') == 'JOHN', borrowers))) == 1
    assert len(list(filter(lambda r: r.get('first_name') == 'JANE', borrowers))) == 1
    assert len(list(filter(lambda r: r.get('first_name') == 'JANET', borrowers))) == 1
    assert len(list(filter(lambda r: r.get('first_name') == 'JOHNATHAN', borrowers))) == 1
