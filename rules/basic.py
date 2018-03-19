from utils.common import *
import utils as u
import pandas as pd
import rules as r


def apply_rules(fpr, changes, rules):
    allowed_rules_mask = False
    # rules = json.load(open('/home/mlaszloffy/Code/python/update_records/rules.json'))
    for rule in rules['allow']:
        rule_name = rule['rule']
        rule_args = rule['args']
        log.debug('Applying inclusion rule {} with args {}'.format(rule_name, rule_args))
        if hasattr(r, rule_name):
            rule_func = getattr(r, rule_name)
        else:
            raise Exception('Missing rule "{}"'.format(rule_name))
        allowed_rules_mask = allowed_rules_mask | rule_func(fpr, changes, **rule_args)

    not_allowed_rules_mask = False
    # rules = json.load(open('/home/mlaszloffy/Code/python/update_records/rules.json'))
    for rule in rules['deny']:
        rule_name = rule['rule']
        rule_args = rule['args']
        log.debug('Applying exclusion rule {} with args {}'.format(rule_name, rule_args))
        if hasattr(r, rule_name):
            rule_func = getattr(r, rule_name)
        else:
            raise Exception('Missing rule "{}"'.format(rule_name))
        not_allowed_rules_mask = not_allowed_rules_mask | rule_func(fpr, changes, **rule_args)

    return (allowed_rules_mask) & (~not_allowed_rules_mask)


def filter_changes(fpr,changes,filters_file):
    filters = pd.read_csv(filters_file, index_col=None)
    mask = False
    for i, rule in filters.iterrows():
        log.debug('Applying filter {}:[{}]->[{}]'.format(rule['field'], rule['from'], rule['to']))
        current_mask = True
        current_mask = current_mask & u.string_match(changes, 'workflow', rule['workflow'])
        current_mask = current_mask & u.string_match(changes, 'provider', rule['provider'])
        current_mask = current_mask & u.greater_than_or_equals(changes, 'processing_date', rule['start_processing_date'])
        current_mask = current_mask & u.less_than(changes, 'processing_date', rule['end_processing_date'])
        current_mask = current_mask & u.string_match(changes, 'field', rule['field'])
        current_mask = current_mask & u.string_match(changes, 'from', rule['from'])
        current_mask = current_mask & u.string_match(changes, 'to', rule['to'])
        mask = mask | current_mask
    return mask