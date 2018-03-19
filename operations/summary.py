from context.analysis import ChangeContext
from reports import generate_change_summary_report, workflow_run_report
from utils import analysis
from utils.common import *


def generate_summary(ctx: ChangeContext):
    log.info('Generating summary files...')

    # summary of changes not allowed
    cols = ['field', 'from', 'to']
    c = ctx.changes_not_allowed.loc[:, cols]
    for col in cols:
        c[col] = c[col].astype(str).fillna('')
    c.groupby(cols, as_index=False).size().sort_values(ascending=False).to_csv('/tmp/changes.csv')

    generate_change_summary_report(ctx.fpr, ctx.changes_not_allowed,
                                   config.out_dir + 'changes_blocked_by_workflow_run.csv')
    generate_change_summary_report(ctx.fpr, ctx.changes_allowed,
                                   config.out_dir + 'changes_allowed_by_workflow_run.csv')

    invalid_wfrs = analysis.get_related_workflow_runs(ctx.fpr, ctx.changes_not_allowed.index, ctx.hierarchy)
    workflow_run_report(ctx.fpr, invalid_wfrs, config.out_dir + 'invalid_workflow_runs.csv')
