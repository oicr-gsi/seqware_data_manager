import logging
import sys

import tasks.load_analysis_lims_data.config as config
from context.analysislimsupdatedatacontext import AnalysisLimsUpdateDataContext


def load_analysis_lims_data(output_path: str,
                            sample_provenance_url: str = config.sample_provenance_url,
                            lane_provenance_url: str = config.lane_provenance_url,
                            provider: str = config.provider,
                            file_provenance_url: str = config.file_provenance_url):
    """
    Task to extract and export analysis and lims data to a file

    :param output_path: where to write AnalysisLimsUpdateDataContext to
    :param sample_provenance_url: new lims sample provenance data source url
    :param lane_provenance_url: new lims lane provenance data source url
    :param provider: new lims provider name
    :param file_provenance_url: current file provenance path
    """

    config.log_level = 'INFO'
    logging.basicConfig(
        level=logging.getLevelName(config.log_level),
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S", stream=sys.stdout)

    ctx = AnalysisLimsUpdateDataContext.load_from_files(lane_provenance_path=lane_provenance_url,
                                                        sample_provenance_path=sample_provenance_url,
                                                        provider=provider,
                                                        file_provenance_path=file_provenance_url)
    ctx.save(output_path)
