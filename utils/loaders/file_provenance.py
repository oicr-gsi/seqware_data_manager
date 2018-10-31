import collections
import logging
import timeit

import numpy as np
import pandas as pd

from utils.file import getpath

log = logging.getLogger(__name__)
fpr_cols = collections.OrderedDict([
    ('Last Modified', 'object'),
    ('Study Title', 'str'),
    ('Study SWID', 'float'),
    ('Study Attributes', 'str'),
    ('Experiment Name', 'str'),
    ('Experiment SWID', 'float'),
    ('Experiment Attributes', 'str'),
    ('Root Sample Name', 'str'),
    ('Root Sample SWID', 'float'),
    ('Parent Sample Name', 'str'),
    ('Parent Sample SWID', 'float'),
    ('Parent Sample Organism IDs', 'str'),
    ('Parent Sample Attributes', 'str'),
    ('Sample Name', 'str'),
    ('Sample SWID', 'float'),
    ('Sample Organism ID', 'str'),
    ('Sample Organism Code', 'str'),
    ('Sample Attributes', 'str'),
    ('Sequencer Run Name', 'str'),
    ('Sequencer Run SWID', 'float'),
    ('Sequencer Run Attributes', 'str'),
    ('Sequencer Run Platform ID', 'str'),
    ('Sequencer Run Platform Name', 'str'),
    ('Lane Name', 'str'),
    ('Lane Number', 'str'),
    ('Lane SWID', 'float'),
    ('Lane Attributes', 'str'),
    ('IUS Tag', 'str'),
    ('IUS SWID', 'float'),
    ('IUS Attributes', 'str'),
    ('Workflow Name', 'str'),
    ('Workflow Version', 'str'),
    ('Workflow SWID', 'float'),
    ('Workflow Attributes', 'str'),
    ('Workflow Run Name', 'str'),
    ('Workflow Run Status', 'str'),
    ('Workflow Run SWID', 'float'),
    ('Workflow Run Attributes', 'str'),
    ('Workflow Run Input File SWAs', 'str'),
    ('Processing Algorithm', 'str'),
    ('Processing SWID', 'float'),
    ('Processing Attributes', 'str'),
    ('Processing Status', 'str'),
    ('File Meta-Type', 'str'),
    ('File SWID', 'float'),
    ('File Attributes', 'str'),
    ('File Path', 'str'),
    ('File Md5sum', 'str'),
    ('File Size', 'float'),
    ('File Description', 'str'),
    ('Path Skip', 'str'),
    ('Skip', 'str'),
    ('Status', 'str'),
    ('Status Reason', 'str'),
    ('LIMS IUS SWID', 'int64'),
    ('LIMS Provider', 'object'),
    ('LIMS ID', 'object'),
    ('LIMS Version', 'object'),
    ('LIMS Last Modified', 'object')])


def load(file_provenance_url):
    start_time = timeit.default_timer()
    log.info('Started loading file provenance from {}'.format(file_provenance_url))

    # check if fpr has header
    first_record = pd.read_csv(getpath(file_provenance_url), delimiter='\t', nrows=1, header=None)
    if first_record.iloc[0, 0] == 'Last Modified':
        header_mode = 0
    else:
        header_mode = None

    fpr = pd.read_csv(file_provenance_url, delimiter='\t', names=fpr_cols.keys(), dtype=fpr_cols, header=header_mode, low_memory=False)

    fpr['LIMS Last Modified'] = pd.to_datetime(fpr['LIMS Last Modified'])
    z = fpr.loc[fpr['Workflow Run Input File SWAs'].notnull(),
                'Workflow Run Input File SWAs'].apply(np.fromstring, dtype='int', sep=';')
    fpr.update(z)

    # remove orphaned IUS-LIMS key records
    fpr_orphans = fpr.loc[fpr['Workflow Name'].isnull() & fpr['IUS Attributes'].isnull() & (fpr['Skip'] == 'false')]
    fpr.drop(fpr_orphans.index, inplace=True)

    # remove records that do not have status = OKAY
    # need historical provenance data to be able to correct these records
    fpr_status_not_okay = fpr.loc[fpr['Status'] != 'OKAY']
    fpr.drop(fpr_status_not_okay.index, inplace=True)

    def string_to_dict(x, prefix=''):
        d = {}
        for attr in x.split(';'):
            key, values = attr.split('=')
            d[prefix + key] = values.split('&')
        return d

    attr_fields = ['Sample Attributes',
                   'Parent Sample Attributes',
                   'Lane Attributes',
                   'Study Attributes',
                   'Sequencer Run Attributes'];

    # for attribute fields, extract attributes from string (key1=value1&value2;key2=value3 format)
    for attr_field in attr_fields:
        log.debug('Parsing file provenance {}'.format(attr_field))

        col = pd.Series(index=fpr.index)
        col.update(fpr.loc[fpr[attr_field].isnull(), attr_field].apply(lambda x: {}))
        col.update(fpr.loc[fpr[attr_field].notnull(), attr_field].apply(string_to_dict, prefix=attr_field + '.'))

        attr_df = pd.DataFrame.from_records(col.values, index=col.index)
        fpr = pd.concat([fpr.drop(attr_field, axis=1), attr_df], axis=1)

    log.info(
        'Completed loading {} file provenance records in {:.1f}s'.format(len(fpr), timeit.default_timer() - start_time))

    return fpr
