import os
import sys
import json
import logging

# Initialize Logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(level=logging.DEBUG, filename="output.log", filemode="w")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Import all environment variables
env_path = os.path.join(os.path.dirname(__file__), "dev.env")
try:
    with open(env_path) as f:
        env_data = json.load(f)
    os.environ.update(env_data)
except Exception as error:  # pylint: disable=broad-except
    logger.exception("Could not load dev.env due to %s", str(error))
os.environ['local'] = 'true'

# Update Path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)


if __name__ == '__main__':
    from handler import main
    from tests.framework import generate_event

    with open('loandata.json') as file:
        event = generate_event(json.load(file))
    response = main(event)

    logger.info('Reports: %s', json.dumps(response, indent=2))
    with open('reports.json', 'w') as reports:
        json.dump(response, reports, indent=2)
