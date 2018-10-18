import logging
import sys

from operations.analyze_changes.rulecontext import RuleContext
from operations.annotate_analysis.annotatecontext import AnnotateWorkflowRunContext
from operations.apply_changes.updatecontext import UpdateLimsKeyContext
from operations.calculate_changes.changecontext import ChangeContext
from operations.join_data.analysislimsupdatedatacontext import AnalysisLimsUpdateDataContext
from tasks.change_analysis_lims_provider import config
from utils.file import getpath

## setup
config.log_level = 'INFO'
logging.basicConfig(
    level=logging.getLevelName(config.log_level),
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S", stream=sys.stdout)

log = logging.getLogger(__name__)

## validate input
#


## load data
if config.data_context_path:
    ctx = AnalysisLimsUpdateDataContext.load(config.data_context_path)
else:
    ctx = AnalysisLimsUpdateDataContext.load_from_files(sample_provenance_path=config.sample_provenance_url,
                                                        lane_provenance_path=config.lane_provenance_url,
                                                        provider=config.provider,
                                                        file_provenance_path=config.file_provenance_url)
## filter data
if config.filters:
    ctx = ctx.filter(config.filters)

## generate changes
change_ctx = ChangeContext.generate_changes(ctx)

## apply change resolution change_filters
if config.rules_config_path:
    rule_context = RuleContext.generate_and_apply_rules(change_ctx, config.rules_config_path)
    rule_context.summarize(out_dir=getpath(config.output_dir))
else:
    log.info('Exiting')
    exit()

## generate updates and summarize updates
update_ctx = UpdateLimsKeyContext.generate_updates(rule_context)
annotate_ctx = AnnotateWorkflowRunContext(rule_context.fpr, rule_context.get_invalid_workflow_runs(),
                                          config.annotations)

for update in [update_ctx, annotate_ctx]:
    update.summarize(config.output_dir)
    update.to_sql(config.output_dir)
