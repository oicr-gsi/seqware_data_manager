import json
import logging
import timeit
import urllib.request

import pandas as pd

log = logging.getLogger(__name__)


def load(url, provider) -> pd.DataFrame:
    start_time = timeit.default_timer()
    log.info('Started loading lane provenance from {}'.format(url))
    lpj = json.load(urllib.request.urlopen(url))
    lp = pd.io.json.json_normalize(lpj)
    lp.createdDate = pd.to_datetime(lp.createdDate)
    lp.lastModified = pd.to_datetime(lp.lastModified)
    lp = lp.rename(columns={'laneProvenanceId': 'provenanceId'})
    lp['provider'] = provider

    # file provenance generation adds underscores
    lp['iusTag'] = 'NoIndex'
    lp['sequencerRunPlatformModel'] = lp['sequencerRunPlatformModel'].str.replace(' ', '_')

    log.info(
        'Completed loading {} lane provenance records in {:.1f}s'.format(len(lp), timeit.default_timer() - start_time))
    return lp
