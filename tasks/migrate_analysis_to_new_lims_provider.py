from context import config
from utils.file import getpath

config.log_level = 'INFO'

import operations

ctx = operations.data.load()

target_ctx = operations.data.filter(ctx, config.selectors)

change_ctx = operations.change.get_changes(target_ctx)
change_ctx.summarize(out_dir=getpath(config.out_dir))

updates = [operations.update.generate_updates(change_ctx),
           operations.update.invalid_workflow_runs(change_ctx, config.annotations)]

for x in updates:
    x.summarize(config.out_dir)
    x.to_sql(config.out_dir)
