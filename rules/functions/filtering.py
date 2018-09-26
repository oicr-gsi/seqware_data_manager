import pandas as pd
import logging
log = logging.getLogger(__name__)
import utils.logic

def filter_changes(fpr,changes,filters_file):
    filters = pd.read_csv(filters_file, index_col=None)
    mask = False
    for i, rule in filters.iterrows():
        log.debug('Applying filter {}:[{}]->[{}]'.format(rule['field'], rule['from'], rule['to']))
        current_mask = True
        current_mask = current_mask & utils.logic.string_match(changes, 'workflow', rule['workflow'])
        current_mask = current_mask & utils.logic.string_match(changes, 'provider', rule['provider'])
        current_mask = current_mask & utils.logic.greater_than_or_equals(changes, 'processing_date', rule['start_processing_date'])
        current_mask = current_mask & utils.logic.less_than(changes, 'processing_date', rule['end_processing_date'])
        current_mask = current_mask & utils.logic.string_match(changes, 'field', rule['field'])
        current_mask = current_mask & utils.logic.string_match(changes, 'from', rule['from'])
        current_mask = current_mask & utils.logic.string_match(changes, 'to', rule['to'])
        mask = mask | current_mask
    return mask