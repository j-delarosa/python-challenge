import json


def generate_event(detail=None):
    """Generate a mock EventBridge event with the given detail."""
    detail = {} if detail is None else detail
    return {
        'Records': [
            {
                'source': 'testing.local',
                'detail-type': 'Local Testing',
                'detail': json.dumps(detail),
            }
        ]
    }