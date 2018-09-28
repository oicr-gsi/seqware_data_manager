import json
import timeit

import pandas as pd

import rules.functions as rule_functions
from context.basecontext import BaseContext
from context.changecontext import ChangeContext
from reports.change_summary import generate_change_summary_report
from utils.file import getpath, get_file_path


class RuleContext(BaseContext):

    def __init__(self, ctx: ChangeContext, changes_allowed, changes_not_allowed):
        self._ctx = ctx
        self.changes_allowed = changes_allowed
        self.changes_not_allowed = changes_not_allowed

    @classmethod
    def apply_rules(cls, fpr, changes, rules):
        allowed_rules_mask = False
        for rule in rules['allow']:
            rule_name = rule['rule']
            rule_args = rule['args']
            cls._log.info('Applying inclusion rule {} with args {}'.format(rule_name, rule_args))
            if hasattr(rule_functions, rule_name):
                rule_func = getattr(rule_functions, rule_name)
            else:
                raise Exception('Missing rule "{}"'.format(rule_name))
            allowed_rules_mask = allowed_rules_mask | rule_func(fpr, changes, **rule_args)

        not_allowed_rules_mask = False
        for rule in rules['deny']:
            rule_name = rule['rule']
            rule_args = rule['args']
            cls._log.info('Applying exclusion rule {} with args {}'.format(rule_name, rule_args))
            if hasattr(rule_functions, rule_name):
                rule_func = getattr(rule_functions, rule_name)
            else:
                raise Exception('Missing rule {}'.format(rule_name))
            not_allowed_rules_mask = not_allowed_rules_mask | rule_func(fpr, changes, **rule_args)

        return (allowed_rules_mask) & (~not_allowed_rules_mask)

    @classmethod
    def generate_and_apply_rules(cls, ctx: ChangeContext, rules_config_path):
        cls._log.info('Applying rules from {}'.format(rules_config_path))
        start_time = timeit.default_timer()

        # allowed_rules_mask = False
        rules_config = json.load(getpath(rules_config_path).open())
        allowed_mask = cls.apply_rules(ctx.fpr, ctx.changes, rules_config)
        changes_allowed = ctx.changes[allowed_mask]
        changes_not_allowed = ctx.changes[~allowed_mask]

        cls._log.info('{} / {} changes allowed'.format(len(changes_allowed), len(ctx.changes)))
        cls._log.info('{} / {} changes not allowed'.format(len(changes_not_allowed), len(ctx.changes)))

        elapsed = timeit.default_timer() - start_time
        cls._log.info('Execution time = {:.1f}s ({:.0f} records/s)'.format(elapsed, len(ctx.fpr) / elapsed))

        return cls(ctx, changes_allowed, changes_not_allowed)

    def get_invalid_workflow_runs(self):
        direct_wfr_swids = self.fpr.loc[
            self.fpr.index.isin(self.changes_not_allowed), 'Workflow Run SWID'].drop_duplicates().tolist()

        def get_downstream(xs):
            if len(xs) > 1:
                return get_downstream([xs[0]]) + get_downstream(xs[1:])
            elif len(xs) == 1:
                return [xs[0]] + get_downstream(
                    self.hierarchy.loc[self.hierarchy['parent'] == xs[0], 'child'].dropna().tolist())
            else:
                return []

        return pd.Series(get_downstream(direct_wfr_swids)).drop_duplicates().astype('int')

    def summarize(self, out_dir):
        generate_change_summary_report(self.fpr, self.changes_not_allowed,
                                       get_file_path(out_dir, 'changes_blocked_by_workflow_run.csv'))

        generate_change_summary_report(self.fpr, self.changes_allowed,
                                       get_file_path(out_dir, 'changes_allowed_by_workflow_run.csv'))

    @property
    def fpr(self):
        return self._ctx.fpr
