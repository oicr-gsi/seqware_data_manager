import logging
import sys

from operations.import_and_join_data import JoinedData


def load_analysis_lims_data(*, output_path: str,
                            sample_provenance_url: str,
                            lane_provenance_url: str,
                            provider_id: str,
                            file_provenance_url: str,
                            id_map_url: str = None):
    """
    Task to extract and export analysis and lims data to a file

    :param output_path: where to write JoinedData object to
    :param sample_provenance_url: new lims sample provenance data source url
    :param lane_provenance_url: new lims lane provenance data source url
    :param provider_id: new lims provider id
    :param file_provenance_url: current file provenance path
    :param id_map_url: csv file with mapping of current LIMS provider + ID to new ID.  CSV format: LIMS Provider,LIMS ID,provenanceId
    """

    log_level = 'INFO'
    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%H:%M:%S',
        stream=sys.stdout)

    ctx = JoinedData.load_from_files(lane_provenance_url=lane_provenance_url,
                                     sample_provenance_url=sample_provenance_url,
                                     provider_id=provider_id,
                                     file_provenance_url=file_provenance_url,
                                     id_map_url=id_map_url)
    ctx.save(output_path)
