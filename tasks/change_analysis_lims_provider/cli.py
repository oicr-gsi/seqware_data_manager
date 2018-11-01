import json

from tasks.change_analysis_lims_provider import config


def change_analysis_lims_provider(*,
                                  output_dir: str,
                                  joined_data_path: str,
                                  include_filters: str = config.include_filters,
                                  exclude_filters: str = config.exclude_filters,
                                  change_analysis_config_path: str = config.change_analysis_config_path):
    """
    Task to summarize changes and generate updates for analysis lims provider updates

    :param output_dir: directory where to write summary reports and updates
    :param joined_data_path: load joined analysis + lims data from file path
    :param include_filters: filters to be used to include file provenance records to update
    :param exclude_filters: filters to be used to exclude file provenance records to update
    :param change_analysis_config_path: change analysis configuration to apply to changes
    """

    config.output_dir = output_dir
    config.joined_data_path = joined_data_path
    config.include_filters = json.loads(str(include_filters))
    config.exclude_filters = json.loads(str(exclude_filters))
    config.change_analysis_config_path = change_analysis_config_path

    # noinspection PyUnresolvedReferences
    import tasks.change_analysis_lims_provider.script
