import json
import timeit

import pandas as pd

import rules.functions as rule_functions
from context.basecontext import BaseContext
from context.changecontext import ChangeContext
from reports.change_summary import generate_change_summary_report
from utils.file import getpath, get_file_path


class RuleContext(BaseContext):

    def __init__(self, ctx: ChangeContext, changes_allowed, changes_blocked):
        self._ctx = ctx
        self.changes_allowed = changes_allowed
        self.changes_blocked = changes_blocked

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

        blocked_rules_mask = False
        for rule in rules['deny']:
            rule_name = rule['rule']
            rule_args = rule['args']
            cls._log.info('Applying exclusion rule {} with args {}'.format(rule_name, rule_args))
            if hasattr(rule_functions, rule_name):
                rule_func = getattr(rule_functions, rule_name)
            else:
                raise Exception('Missing rule {}'.format(rule_name))
            blocked_rules_mask = blocked_rules_mask | rule_func(fpr, changes, **rule_args)

        return (allowed_rules_mask) & (~blocked_rules_mask)

    @classmethod
    def generate_and_apply_rules(cls, ctx: ChangeContext, rules_config_path):
        cls._log.info('Applying rules from {}'.format(rules_config_path))
        start_time = timeit.default_timer()

        # allowed_rules_mask = False
        rules_config = json.load(getpath(rules_config_path).open())
        allowed_mask = cls.apply_rules(ctx.fpr, ctx.changes, rules_config)
        changes_allowed = ctx.changes[allowed_mask]
        changes_blocked = ctx.changes[~allowed_mask]

        cls._log.info('{} / {} changes allowed'.format(len(changes_allowed), len(ctx.changes)))
        cls._log.info('{} / {} changes blocked'.format(len(changes_blocked), len(ctx.changes)))

        elapsed = timeit.default_timer() - start_time
        cls._log.info('Execution time = {:.1f}s ({:.0f} records/s)'.format(elapsed, len(ctx.fpr) / elapsed))

        return cls(ctx, changes_allowed, changes_blocked)

    def get_invalid_workflow_runs(self):
        direct_wfr_swids = self.fpr.loc[
            self.fpr.index.isin(self.changes_blocked), 'Workflow Run SWID'].drop_duplicates().tolist()

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
        allowed_changes_file = get_file_path(out_dir, 'changes_allowed.csv')
        blocked_changes_file = get_file_path(out_dir, 'changes_blocked.csv')

        self._log.info('Writing allowed changes to %s', allowed_changes_file)
        generate_change_summary_report(self.fpr, self.changes_allowed, allowed_changes_file)

        self._log.info('Writing blocked changes to %s', blocked_changes_file)
        generate_change_summary_report(self.fpr, self.changes_blocked, blocked_changes_file)

    @property
    def fpr(self):
        return self._ctx.fpr
