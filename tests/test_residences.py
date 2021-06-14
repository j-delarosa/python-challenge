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


def test_residences_report_different_street_addresses_not_filtered(abs_path):
    # Arrange
    with open(os.path.join(abs_path, 'test_input/loandata_differentstreet.json')) as file:
        event = generate_event(json.load(file))

    # Act
    response = main(event)

    # Assert
    assert response.get('reports') is not None
    res_report = list(filter(lambda r: r.get('title') == 'Residences Report', response.get('reports')))
    assert res_report is not None
    assert len(res_report) == 1
    residences = res_report[0].get('residences')
    assert len(residences) == 2
    assert len(list(filter(lambda r: r.get('street') == '123 EXAMPLE PKWY.', residences))) == 1
    assert len(list(filter(lambda r: r.get('street') == '456 EXAMPLE PKWY.', residences))) == 1


def test_residences_report_different_city_not_filtered(abs_path):
    # Arrange
    with open(os.path.join(abs_path, 'test_input/loandata_differentcity.json')) as file:
        event = generate_event(json.load(file))

    # Act
    response = main(event)

    # Assert
    assert response.get('reports') is not None
    res_report = list(filter(lambda r: r.get('title') == 'Residences Report', response.get('reports')))
    assert res_report is not None
    assert len(res_report) == 1
    residences = res_report[0].get('residences')
    assert len(residences) == 2
    assert len(list(filter(lambda r: r.get('city') == 'FLOWER MOUND', residences))) == 1
    assert len(list(filter(lambda r: r.get('city') == 'AUSTIN', residences))) == 1


def test_residences_report_different_statenot_filtered(abs_path):
    # Arrange
    with open(os.path.join(abs_path, 'test_input/loandata_differentstate.json')) as file:
        event = generate_event(json.load(file))

    # Act
    response = main(event)

    # Assert
    assert response.get('reports') is not None
    res_report = list(filter(lambda r: r.get('title') == 'Residences Report', response.get('reports')))
    assert res_report is not None
    assert len(res_report) == 1
    residences = res_report[0].get('residences')
    assert len(residences) == 2
    assert len(list(filter(lambda r: r.get('state') == 'TX', residences))) == 1
    assert len(list(filter(lambda r: r.get('state') == 'CA', residences))) == 1


def test_residences_report_different_postal_code_not_filtered(abs_path):
    # Arrange
    with open(os.path.join(abs_path, 'test_input/loandata_differentpostalcode.json')) as file:
        event = generate_event(json.load(file))

    # Act
    response = main(event)

    # Assert
    assert response.get('reports') is not None
    res_report = list(filter(lambda r: r.get('title') == 'Residences Report', response.get('reports')))
    assert res_report is not None
    assert len(res_report) == 1
    residences = res_report[0].get('residences')
    assert len(residences) == 2
    assert len(list(filter(lambda r: r.get('zip') == '12345', residences))) == 1
    assert len(list(filter(lambda r: r.get('zip') == '67890', residences))) == 1


def test_residences_report_same_address_filtered(abs_path):
    # Arrange
    with open(os.path.join(abs_path, 'test_input/loandata_sameaddress.json')) as file:
        event = generate_event(json.load(file))

    # Act
    response = main(event)

    # Assert
    assert response.get('reports') is not None
    res_report = list(filter(lambda r: r.get('title') == 'Residences Report', response.get('reports')))
    assert res_report is not None
    assert len(res_report) == 1
    residences = res_report[0].get('residences')
    assert len(residences) == 1
    assert len(list(filter(lambda r: r.get('street') == '123 EXAMPLE PKWY.', residences))) == 1


def test_residences_report_multiple_applications_appends(abs_path):
    # Arrange
    with open(os.path.join(abs_path, 'test_input/loandata_multipleapplications.json')) as file:
        event = generate_event(json.load(file))

    # Act
    response = main(event)

    # Assert
    assert response.get('reports') is not None
    res_report = list(filter(lambda r: r.get('title') == 'Residences Report', response.get('reports')))
    assert res_report is not None
    assert len(res_report) == 1
    residences = res_report[0].get('residences')
    assert len(residences) == 1
    assert len(list(filter(lambda r: r.get('street') == '123 EXAMPLE PKWY.', residences))) == 1
