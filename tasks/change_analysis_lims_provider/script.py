import logging
import sys

from context.annotatecontext import AnnotateWorkflowRunContext
from context.changecontext import ChangeContext
from context.analysislimsupdatedatacontext import AnalysisLimsUpdateDataContext
from context.updatecontext import UpdateLimsKeyContext
from tasks.change_analysis_lims_provider import config
from utils.file import getpath

## setup
config.log_level = 'INFO'
logging.basicConfig(
    level=logging.getLevelName(config.log_level),
    # format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S", stream=sys.stdout)

## validate input
#

## load data
if config.data_context_path and getpath(config.data_context_path).exists():
    ctx = AnalysisLimsUpdateDataContext.load(config.data_context_path)
else:
    ctx = AnalysisLimsUpdateDataContext.load_from_files(sample_provenance_path=config.sample_provenance_path,
                                                        lane_provenance_path=config.lane_provenance_path,
                                                        provider=config.provider,
                                                        file_provenance_path=config.file_provenance_path)
## filter data
ctx = ctx.filter(config.filters)

## generate changes
change_ctx = ChangeContext.generate_and_apply_rules(ctx, config.rules_path)
change_ctx.summarize(out_dir=getpath(config.output_dir))

## generate updates and summarize updates
update_ctx = UpdateLimsKeyContext.generate_updates(change_ctx)
annotate_ctx = AnnotateWorkflowRunContext(change_ctx.fpr, change_ctx.get_invalid_workflow_runs(), config.annotations)

for update in [update_ctx, annotate_ctx]:
    update.summarize(config.output_dir)
    update.to_sql(config.output_dir)
