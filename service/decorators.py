"""Service decorators."""
import logging
import functools


# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Decorator function 
def remove_duplicate_addresses(func):
    """Ensure only unique 'maillingAddress' on each loan application"""
    @functools.wraps(func)
    def wrapper_remove_duplicate_addresses(*args, **kwargs):
        result = func(*args, **kwargs)
        reports = [] if 'reports' not in result else result['reports']
        for report in reports:
            if report['title'] == 'Residences Report':
                try:
                    # Delete any duplicate residences
                    report['residences'] = list(
                        {v['street']:v for v in report['residences']}.values()
                    )
                except KeyError:
                    logger.error(
                        'Unable to find key for residence - skipping loan'
                    )
                    continue
        # Update reports to new value
        result['reports'] = reports
        return result
    return wrapper_remove_duplicate_addresses