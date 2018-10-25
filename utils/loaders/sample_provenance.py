import json
import logging
import timeit
import urllib.request

import pandas as pd

log = logging.getLogger(__name__)


def load(url, provider) -> pd.DataFrame:
    start_time = timeit.default_timer()
    log.info('Started loading sample provenance from {}'.format(url))
    spj = json.load(urllib.request.urlopen(url))
    sp = pd.io.json.json_normalize(spj)
    sp.createdDate = pd.to_datetime(sp.createdDate)
    sp.lastModified = pd.to_datetime(sp.lastModified)
    sp = sp.rename(columns={'sampleProvenanceId': 'provenanceId'})
    sp['provider'] = provider

    # file provenance generation adds underscores
    sp['sequencerRunPlatformModel'] = sp['sequencerRunPlatformModel'].str.replace(' ', '_')

    log.info('Completed loading {} sample provenance records in {:.1f}s'.format(len(sp),
                                                                                timeit.default_timer() - start_time))
    return sp
