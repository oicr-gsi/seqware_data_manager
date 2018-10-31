import logging
import sys

from models.exception import HaltException
from operations.analyze_changes import AnalyzedChangeSet
from operations.annotate_analysis import AnnotateRecords
from operations.apply_changes import UpdateRecords
from operations.calculate_changes import JoinedDataChangeSet
from operations.import_and_join_data import JoinedData
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
    ctx = JoinedData.load(config.data_context_path)
else:
    ctx = JoinedData.load_from_files(sample_provenance_url=config.sample_provenance_url,
                                     lane_provenance_url=config.lane_provenance_url,
                                     provider=config.provider,
                                     file_provenance_url=config.file_provenance_url)
## filter data
record_count_before_filtering = len(ctx.fpr)
ctx = ctx.apply_include_filters(config.include_filters)
ctx = ctx.apply_exclude_filters(config.exclude_filters)
log.info('Selected {}/{} records'.format(len(ctx.fpr), record_count_before_filtering))

## generate changes
change_ctx = JoinedDataChangeSet.generate_changes(ctx)

## apply change resolution change_filters
if config.rules_config_path:
    rule_context = AnalyzedChangeSet.generate_and_apply_rules(change_ctx, config.rules_config_path)
    rule_context.summarize(out_dir=getpath(config.output_dir))
else:
    raise HaltException('No analyze change set configuration provided')

## generate updates and summarize updates
update_ctx = UpdateRecords.generate_updates(rule_context)
annotate_ctx = AnnotateRecords(rule_context.fpr, rule_context.get_invalid_workflow_runs(),
                               config.annotations)

for update in [update_ctx, annotate_ctx]:
    update.summarize(config.output_dir)
    update.to_sql(config.output_dir)
