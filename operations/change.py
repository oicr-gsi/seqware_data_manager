import json

import rules as r
from context.analysis import ChangeContext
from utils.common import *
from utils.file import getpath


def get_changes(ctx):
    log.info('Applying rules...')
    start_time = timeit.default_timer()

    # allowed_rules_mask = False
    rules = json.load(getpath(config.rules_file).open())
    allowed_mask = r.apply_rules(ctx.fpr, ctx.changes, rules)
    changes_allowed = ctx.changes[allowed_mask]
    changes_not_allowed = ctx.changes[~allowed_mask]

    log.info('{} / {} changes allowed'.format(len(changes_allowed), len(ctx.changes)))
    log.info('{} / {} changes not allowed'.format(len(changes_not_allowed), len(ctx.changes)))

    elapsed = timeit.default_timer() - start_time
    log.info('Execution time = {:.1f}s ({:.0f} records/s)'.format(elapsed, len(ctx.fpr) / elapsed))

    return ChangeContext(ctx, changes_allowed, changes_not_allowed)
