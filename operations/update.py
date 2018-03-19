import pandas as pd

from context.analysis import ChangeContext, UpdateLimsKeyContext, AnnotateWorkflowRunContext
from utils import generate_lims_key_updates, generate_workflow_run_annotations, analysis
from utils.common import *


def generate_updates(ctx: ChangeContext):
    cols = ['lims_ius_swid', 'lims_id', 'lims_version', 'lims_last_modified', 'lims_provider']

    from_table_okay = ctx.fpr.loc[~ctx.fpr.index.isin(ctx.changes_not_allowed.index),
                                  ['LIMS IUS SWID', 'LIMS ID', 'LIMS Version', 'LIMS Last Modified',
                                   'LIMS Provider']]
    from_table_okay.columns = cols

    to_table_okay = ctx.fpr.loc[~ctx.fpr.index.isin(ctx.changes_not_allowed.index),
                                ['LIMS IUS SWID', 'provenanceId', 'version', 'lastModified', 'provider']]
    to_table_okay.columns = cols

    no_update_required_mask = (from_table_okay == to_table_okay).all(axis=1)
    from_table_pre = from_table_okay[~no_update_required_mask]
    to_table_pre = to_table_okay[~no_update_required_mask]

    from_table = from_table_pre.drop_duplicates().reset_index(drop=True)
    to_table = to_table_pre.drop_duplicates().reset_index(drop=True)

    from_to_table = pd.merge(from_table, to_table, on='lims_ius_swid', suffixes=['_from', '_to'], how='left',
                             indicator=True)
    if not (from_to_table['_merge'] == 'both').all():
        raise Exception('Failure')
    if from_to_table['lims_ius_swid'].duplicated().any():
        raise Exception('Failure')

    return UpdateLimsKeyContext(from_table, to_table)


def invalid_workflow_runs(ctx: ChangeContext, tags):
    invalid_wfrs = analysis.get_related_workflow_runs(ctx.fpr, ctx.changes_not_allowed.index, ctx.hierarchy)
    return AnnotateWorkflowRunContext(ctx.fpr, invalid_wfrs, tags)


def generate_update_actions(ctx):
    log.info('Generating updates...')

    filename = config.out_dir + 'update_lims_keys.sql'
    with open(filename, 'w') as out:
        out.write(generate_lims_key_updates(ctx.from_table, ctx.to_table))

    filename = config.out_dir + 'update_workflow_run_annotations.sql'
    with open(filename, 'w') as out:
        out.write(generate_workflow_run_annotations(invalid_wfrs, {'skip': 'GP-1504', 'delete': 'GP-1504'}))
