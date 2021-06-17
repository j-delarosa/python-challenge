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
        new_args = []
        for arg in args:
            if type(arg) == dict:
                data = arg
                for app in data['applications']:
                    try:
                        # Delete any duplicate residences
                        if app['borrower']['mailingAddress'] == \
                            app['coborrower']['mailingAddress']:
                            del app['coborrower']['mailingAddress']
                    except KeyError:
                        logger.error(
                            'Unable to find key for residence - skipping loan'
                        )
                        continue
                new_args.append(data)
            else:
                new_args.append(arg)
        return func(*new_args, **kwargs)
    return wrapper_remove_duplicate_addresses