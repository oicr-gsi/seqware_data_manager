import json

from tasks.change_analysis_lims_provider import config


def change_analysis_lims_provider(output_dir: str = config.output_dir,
                                  filters: str = config.filters,
                                  data_context_path: str = config.data_context_path):
    """
    Task to summarize changes and generate updates for analysis lims provider updates

    :param output_dir: directory where to write summary reports and updates
    :param filters: filters to be used to select file provenance records to update
    """

    config.output_dir = output_dir
    config.filters = json.loads(str(filters))
    config.data_context_path = data_context_path

    # noinspection PyUnresolvedReferences
    import tasks.change_analysis_lims_provider.script


if __name__ == '__main__':
    change_analysis_lims_provider()
