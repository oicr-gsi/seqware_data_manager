import collections
import json
from pathlib import Path

from context.analysis import DataContext
from utils import analysis
from utils.common import *


def load():
    log.info('Loading data...')
    start_time = timeit.default_timer()

    fpr_to_provenance_map = json.loads(config.fpr_to_provenance_map_str, object_pairs_hook=collections.OrderedDict)

    if config.data_context_file_path:
        data_context_file_path = Path(config.data_context_file_path)

    if config.data_context_file_path and data_context_file_path.exists():
        log.info('Loading data context from {}'.format(config.data_context_file_path))
        ctx = DataContext.load(data_context_file_path)
    else:
        ctx = analysis.load(fpr_to_provenance_map)
        if config.data_context_file_path:
            log.info('Saving data context to {}'.format(config.data_context_file_path))
            data_context_file_path = Path(config.data_context_file_path)
            ctx.save(data_context_file_path)

    elapsed = timeit.default_timer() - start_time

    return ctx


def filter(ctx, selectors):
    log.info('Selecting records...')
    start_time = timeit.default_timer()

    select_mask = True
    for k, vs in selectors.items():
        key_mask = False
        key = config.filter_config[k]
        for v in vs.split(','):
            key_mask = key_mask | (ctx.fpr[key] == v)
            select_mask = select_mask & key_mask

    target_fpr = ctx.fpr[select_mask]
    target_changes = ctx.changes[ctx.changes.index.isin(target_fpr.index)]
    target_ctx = DataContext(target_fpr, target_changes, ctx.hierarchy)

    elapsed = timeit.default_timer() - start_time

    return target_ctx
