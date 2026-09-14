"""Server-owned registration metadata, derived only from campaigns.created."""
import math
from datetime import datetime, timedelta, timezone


def registration_fields(created):
    if (isinstance(created, bool) or not isinstance(created, (int, float))
            or not math.isfinite(created) or created <= 0):
        return dict(registered_at=None, registered_at_kst=None, registered_at_utc=None)
    return dict(registered_at=created,
                registered_at_kst=datetime.fromtimestamp(created, timezone(timedelta(hours=9))).isoformat(timespec='seconds'),
                registered_at_utc=datetime.fromtimestamp(created, timezone.utc).isoformat(timespec='seconds'))


def campaign_label(campaign):
    stamp=registration_fields(campaign.get('registered_at'))['registered_at_kst']
    return campaign['id'] + ('-' + stamp[8:10] + 'd-' + stamp[11:13] + 'h' + stamp[14:16] + 'm' if stamp else '')
