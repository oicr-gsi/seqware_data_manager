import json

from tasks.change_analysis_lims_provider import config


def change_analysis_lims_provider(output_dir: str = config.output_dir,
                                  include_filters: str = config.include_filters,
                                  exclude_filters: str = config.exclude_filters,
                                  data_context_path: str = config.data_context_path,
                                  rules_config_path: str = None):
    """
    Task to summarize changes and generate updates for analysis lims provider updates

    :param output_dir: directory where to write summary reports and updates
    :param include_filters: filters to be used to include file provenance records to update
    :param exclude_filters: filters to be used to exclude file provenance records to update
    :param rules_config_path: change_filters configuration to apply to changes
    :param data_context_path: load analysis lims data context from local file
    """

    config.output_dir = output_dir
    config.include_filters = json.loads(str(include_filters))
    config.exclude_filters = json.loads(str(exclude_filters))
    config.data_context_path = data_context_path
    config.rules_config_path = rules_config_path

    # noinspection PyUnresolvedReferences
    import tasks.change_analysis_lims_provider.script
