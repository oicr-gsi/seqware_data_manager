import logging

import pandas as pd

import utils.logic
from utils.file import getpath

log = logging.getLogger(__name__)


def filter_changes(fpr, changes, filters_csv_file):
    filters = pd.read_csv(getpath(filters_csv_file), index_col=None)
    mask = False
    for i, rule in filters.iterrows():
        log.info('Applying filter {}:[{}]->[{}]'.format(rule['field'], rule['from'], rule['to']))
        current_mask = True
        current_mask = current_mask & utils.logic.string_match(changes, 'workflow', rule['workflow'])
        current_mask = current_mask & utils.logic.string_match(changes, 'provider', rule['provider'])
        current_mask = current_mask & utils.logic.greater_than_or_equals(changes, 'processing_date',
                                                                         rule['start_processing_date'])
        current_mask = current_mask & utils.logic.less_than(changes, 'processing_date', rule['end_processing_date'])
        current_mask = current_mask & utils.logic.string_match(changes, 'field', rule['field'])
        current_mask = current_mask & utils.logic.string_match(changes, 'from', rule['from'])
        current_mask = current_mask & utils.logic.string_match(changes, 'to', rule['to'])
        mask = mask | current_mask
    return mask


def generic_filter_changes(fpr, changes, filters=None):
    if filters and type(filters) == dict:
        filters = pd.DataFrame([filters])  # type: pd.DataFrame
    elif filters and type(filters) == list:
        filters = pd.DataFrame(filters)  # type: pd.DataFrame
    else:
        raise Exception('No filters provided')

    required_fields = {'field', 'from', 'to'}
    if not required_fields.issubset(filters.columns):
        raise Exception(f'Required fields not found: {",".join(required_fields - set(filters.columns))}')

    filter_columns = set(filters.columns) - set(required_fields)  # type:set
    if not filter_columns.issubset(fpr.columns):
        missing_filter_columns = set(filter_columns) - set(fpr.columns)
        raise Exception('The following filter columns are missing: ' + ','.join(missing_filter_columns))

    mask = False
    for i, rule in filters.iterrows():
        current_mask = True
        for filter_column in filter_columns:
            if type(rule[filter_column]) is str:
                matches = utils.logic.string_match(fpr, filter_column, rule[filter_column])
                new_mask = changes.index.isin(matches[matches].index)
                current_mask = current_mask & new_mask
            elif type(rule[filter_column]) is list:
                field_mask = False
                for filter_value in rule[filter_column]:
                    matches = utils.logic.string_match(fpr, filter_column, filter_value)
                    new_mask = changes.index.isin(matches[matches].index)
                    field_mask = field_mask | new_mask
                current_mask = current_mask & field_mask
            else:
                raise Exception(
                    f"Unsupported filter type = {type(rule[filter_column])} for rule filter {filter_column}")
        for required_column in required_fields:
            current_mask = current_mask & utils.logic.string_match(changes, required_column, rule[required_column])
        mask = mask | current_mask
    return mask


def character_replace_check_equality(fpr, changes, config=None):
    if config and type(config) == dict:
        config = pd.DataFrame([config])  # type: pd.DataFrame
    elif config and type(config) == list:
        config = pd.DataFrame(config)  # type: pd.DataFrame
    else:
        raise Exception('No config provided')

    config_fields = {'field', 'from_characters_regex', 'to_characters_regex', 'replace_with_character'}
    required_config_fields = {'field', 'replace_with_character'}
    if not required_config_fields.issubset(config.columns):
        raise Exception(f'Required config fields not found: {",".join(required_config_fields - set(config.columns))}')

    filter_columns = set(config.columns) - set(config_fields)  # type:set
    if not filter_columns.issubset(fpr.columns):
        missing_filter_columns = set(filter_columns) - set(fpr.columns)
        raise Exception('The following filter columns are missing: ' + ','.join(missing_filter_columns))

    mask = False
    for i, rule in config.iterrows():
        current_mask = True
        for filter_column in filter_columns:
            current_mask = current_mask & \
                           changes.index.isin(utils.logic.string_match(fpr, filter_column, rule[filter_column]).index)

        if 'from_characters_regex' in rule:
            from_values = changes['from'].str.replace(
                rule['from_characters_regex'], rule['replace_with_character'], regex=True)
        else:
            from_values = changes['from']

        if 'to_characters_regex' in rule:
            to_values = changes['to'].str.replace(
                rule['to_characters_regex'], rule['replace_with_character'], regex=True)
        else:
            to_values = changes['to']

        current_mask = current_mask & \
                       utils.logic.string_match(changes, 'field', rule['field']) & (from_values == to_values)

        mask = mask | current_mask

    return mask
