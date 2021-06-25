"""Service entry-point."""
import json
import logging

from service.dal import Project
from service.models import JSONManifest, JSONFactory


# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def addAddressRules(app_idx: int, residence_idx: int, borrower: str = "borrower"):
    """Add a rule to the list for adding a basic address.

    Helps make the code more KISS
    """
    if borrower in {"borrower", "coborrower"}:
        return [
            {
                "source": f"$.applications[{app_idx}].{borrower}.mailingAddress.addressStreetLine1",
                "target": f"$.reports[?(@.title == 'Residences Report')].residences[{residence_idx}].street",
            },
            {
                "source": f"$.applications[{app_idx}].{borrower}.mailingAddress.addressCity",
                "target": f"$.reports[?(@.title == 'Residences Report')].residences[{residence_idx}].city",
            },
            {
                "source": f"$.applications[{app_idx}].{borrower}.mailingAddress.addressState",
                "target": f"$.reports[?(@.title == 'Residences Report')].residences[{residence_idx}].state",
            },
            {
                "source": f"$.applications[{app_idx}].{borrower}.mailingAddress.addressPostalCode",
                "target": f"$.reports[?(@.title == 'Residences Report')].residences[{residence_idx}].zip",
            },
        ]
    else:
        raise ValueError("borrower should be one of: borrower, coborrower")


# Lambda entry
def main(event, context=None):  # pylint: disable=unused-argument
    """Handle loandata as Eventbridge event and return report.

    The reports generated by the service have the following envelope:

    ```json
    {
        "reports": [
            {
                "title": "<the report title>",
                ...
            },
            ...
        ]
    }
    ```

    Parameters
    ----------
    event : dict
        The Eventbridge event payload with loandata for reporting as its detail.
    context : LambdaContext
        The lambda context object (for Lambda use only).

    Returns
    -------
    dict{str:any}
        Returns a dict which contains the reports generated by the service.

    """
    event = {} if event is None else event
    logger.info("Service invoked by event: %s", json.dumps(event, indent=2))

    # Load all rules
    project = Project()
    rules = [rule for _ in project.resources.values() for rule in _]
    logger.info("Service loaded rules: %s", json.dumps(rules, indent=2))

    # Confirm event is valid EventBridge -> SQS payload
    loans = []
    for record in event.get("Records", [{}]):
        if not all(key in record for key in ["source", "detail-type", "detail"]):
            logger.error("Service received invalid EventBridge event- Skipping event")
            continue

        # Attempt to load loandata
        try:
            loans.append(json.loads(record["detail"]))
        except json.JSONDecodeError:
            logger.error("Service received invalid event detail- Skipping event")
            continue

    logger.info("Service recieved loans: %s", json.dumps(loans, indent=2))

    # Generate Manifests
    reports = []
    for loan in loans:
        rules = process_app_rules(loan, rules)

        manifest = JSONManifest(loan, rules)
        logger.info("Generated manifest: %s", json.dumps(manifest.items, indent=2))

        projection = JSONFactory(manifest).get_projection()
        logger.info("Generated projection: %s", json.dumps(projection, indent=2))

        reports.extend(projection.get("reports", []))

    # Reformat report output and return
    return {"reports": reports}


def process_app_rules(loan, rules):
    """Process loan data to handle any special rules."""

    for app in range(len(loan["applications"])):
        # Here we can setup a call for each check/validation we want to run
        rules = check_borrower_addresses(loan, app, rules)

    return rules


def check_borrower_addresses(loan, app, rules):
    """Validate borrower addresses."""
    # ideally, this should be more idempotent and not magically
    # update the `load` data and not return it. no side effects

    borrower = loan["applications"][app]["borrower"]
    coborrower = loan["applications"][app]["coborrower"]
    # Do they borrowers have the same address?
    if borrower["mailingAddress"] == coborrower["mailingAddress"]:
        # they do, set the default False flag to True
        loan["applications"][app]["shared_address"] = True
    else:
        # They do not, so let's add the coborrowers address
        # param 2 (`residence_idx`) here is set to `1` because `0` already
        # would be populated with the primary borrowers address
        # we could possibly do this so if they are the same, it just
        # overwrites address 0, but depending on ingested data size,
        # that could produce issues? and lead to complexity.
        # this also needs more work to properly process the extra loan data I added
        # for testing, and append all the addresses properly and not overwrite by
        # generating the correct `residence_idx`. But currently works based on ticket.
        rules += addAddressRules(app, app, "coborrower")

    return rules
