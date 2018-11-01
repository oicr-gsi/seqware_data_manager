import logging
import os
import sys

from models.exception import HaltException, ConfigurationException
from operations.analyze_changes import AnalyzedChangeSet
from operations.annotate_analysis import AnnotateRecords
from operations.apply_changes import UpdateRecords
from operations.calculate_changes import JoinedDataChangeSet
from operations.import_and_join_data import JoinedData
from tasks.change_analysis_lims_provider import config
from utils.file import getpath

## setup
logging.basicConfig(
    level=logging.getLevelName(config.log_level),
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout)

log = logging.getLogger(__name__)

## validate input

if not os.access(getpath(config.output_dir), os.W_OK):
    raise ConfigurationException(f'Output dir = [{config.output_dir}] is inaccessible')

if config.change_analysis_config_path is None:
    log.info('No change analysis configuration provided, only generating changes')

## load data
if config.joined_data_path:
    joined_data = JoinedData.load(config.joined_data_path)
else:
    joined_data = JoinedData.load_from_files(sample_provenance_url=config.sample_provenance_url,
                                             lane_provenance_url=config.lane_provenance_url,
                                             provider_id=config.provider_id,
                                             file_provenance_url=config.file_provenance_url)

## filter data
record_count_before_filtering = len(joined_data.fpr)
joined_data = joined_data.apply_include_filters(config.include_filters)
joined_data = joined_data.apply_exclude_filters(config.exclude_filters)
log.info('Selected {}/{} records'.format(len(joined_data.fpr), record_count_before_filtering))

## generate changes
joined_data_change_set = JoinedDataChangeSet.generate_changes(joined_data)

## analyze changes
if config.change_analysis_config_path:
    analyzed_change_set = AnalyzedChangeSet.generate_and_apply_rules(joined_data_change_set,
                                                                     config.change_analysis_config_path)
    analyzed_change_set.summarize(out_dir=getpath(config.output_dir))
else:
    raise HaltException('No change analysis rules configuration provided')

## generate updates and summarize updates
updatable_records = UpdateRecords.generate_updates(analyzed_change_set)
invalid_records = AnnotateRecords(analyzed_change_set.fpr, analyzed_change_set.get_invalid_workflow_runs(),
                                  config.annotations)

for update in [updatable_records, invalid_records]:
    update.summarize(config.output_dir)
    update.to_sql(config.output_dir)
