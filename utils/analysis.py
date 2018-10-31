import json
import logging
import pathlib
import urllib

import numpy as np
import pandas as pd
import pkg_resources

import utils.pandas
import utils.transformations
from utils.loaders import file_provenance, lane_provenance, sample_provenance

log = logging.getLogger(__name__)


def get_fp_with_lims_provenance(lane_provenance_url,
                                sample_provenance_url,
                                provider_id,
                                file_provenance_url,
                                file_to_lims_provenance_mapping_config_file_path=None):
    if file_to_lims_provenance_mapping_config_file_path is None:
        file_to_lims_provenance_mapping_config_file_path = pathlib.Path(
            pkg_resources.resource_filename('resources',
                                            'default_file_to_lims_provenance_mapping_config.json')).as_uri()

    file_to_lims_provenance_mapping_config = json.load(
        urllib.request.urlopen(file_to_lims_provenance_mapping_config_file_path))

    log.info('Loading data...')
    lp = lane_provenance.load(lane_provenance_url, provider_id)
    sp = sample_provenance.load(sample_provenance_url, provider_id)
    fp = file_provenance.load(file_provenance_url)

    # validate that all data fields are mapped or filtered
    log.info('Joining file and lims provenance using mapping configuration = {}'.format(
        file_to_lims_provenance_mapping_config_file_path))

    file_to_lims_provenance_map = file_to_lims_provenance_mapping_config.get('file_to_lims_provenance_mapping')
    sp_ignore_fields = file_to_lims_provenance_mapping_config.get('sample_provenance_ignore_fields')
    lp_ignore_fields = file_to_lims_provenance_mapping_config.get('lane_provenance_ignore_fields')
    fp_ignore_fields = file_to_lims_provenance_mapping_config.get('file_provenance_ignore_fields')

    # check for unhandled fields
    sp_cols_diff = set(sp.columns) - set(file_to_lims_provenance_map.values()) - set(sp_ignore_fields)
    if sp_cols_diff:
        raise Exception('Missing sample provenance mapping for: {}'.format(sp_cols_diff))
    lp_cols_diff = set(lp.columns) - set(file_to_lims_provenance_map.values()) - set(lp_ignore_fields)
    if lp_cols_diff:
        raise Exception('Missing lane provenance mapping for: {}'.format(lp_cols_diff))
    fp_cols_diff = set(fp.columns) - set(file_to_lims_provenance_map.keys()) - set(fp_ignore_fields)
    if fp_cols_diff:
        raise Exception('Missing file provenance mapping for: {}'.format(fp_cols_diff))

    # add missing columns to fp that are defined in fp_to_provenance map
    fp_missing_cols = list(file_to_lims_provenance_map.keys() - fp.columns)
    if fp_missing_cols:
        fp = fp.reindex(columns=fp.columns.tolist() + fp_missing_cols)

    # generate current to new id mapping
    is_sample_record = fp['Sequencer Run Name'].notnull() & fp['Lane Number'].notnull() & fp['Sample Name'].notnull() & \
                       fp['Study Title'].notnull()

    is_lane_record = fp['Sequencer Run Name'].notnull() & \
                     fp['Lane Number'].notnull() & \
                     fp['Sample Name'].isnull() & \
                     fp['Study Title'].isnull()

    # fp records that are not of type sample or lane provenance
    unknown_record_type = ~(is_sample_record | is_lane_record)
    if unknown_record_type.sum() > 0:
        log.info('Found {} / {} unknown record types'.format(unknown_record_type.sum(), len(fp)))
        # raise Exception('Unknown data type encountered')

    # map fp records to input sample and lane provenance

    def apply_mapping(from_data, to_data, from_fields, to_fields, id_map):
        log.debug('Mapping records using [{}] to [{}]'.format('+'.join(from_fields), '+'.join(to_fields)))
        new_id_map = utils.transformations.unique_join(from_data, to_data,
                                                       ['LIMS Provider', 'LIMS ID'], from_fields,
                                                       ['provenanceId'], to_fields)
        return utils.transformations.merge_id_map(id_map, new_id_map, ['LIMS Provider', 'LIMS ID'])

    id_map = fp.loc[:, ['LIMS Provider', 'LIMS ID']]
    id_map['provenanceId'] = np.nan

    # if has_missing(id_map, 'provenanceId'):
    if any(id_map['provenanceId'].isnull()):
        from_fields = ['LIMS Provider', 'LIMS ID']
        to_fields = ['provider', 'provenanceId']
        id_map = apply_mapping(fp[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['LIMS Provider', 'LIMS ID']
        to_fields = ['provider', 'provenanceId']
        id_map = apply_mapping(fp[is_lane_record], lp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'IUS Tag', 'Sample Name']
        to_fields = ['sequencerRunName', 'laneNumber', 'iusTag', 'sampleName']
        id_map = apply_mapping(fp[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'IUS Tag']
        to_fields = ['sequencerRunName', 'laneNumber', 'iusTag']
        id_map = apply_mapping(fp[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'Sample Name']
        to_fields = ['sequencerRunName', 'laneNumber', 'sampleName']
        id_map = apply_mapping(fp[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'Root Sample Name']
        to_fields = ['sequencerRunName', 'laneNumber', 'rootSampleName']
        id_map = apply_mapping(fp[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'Sample Attributes.geo_external_name']
        to_fields = ['sequencerRunName', 'laneNumber', 'sampleAttributes.geo_external_name']
        id_map = apply_mapping(fp[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number']
        to_fields = ['sequencerRunName', 'laneNumber']
        id_map = apply_mapping(fp[is_lane_record], lp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        log.warning('Unable to map {} file provenance records'.format(
            id_map['provenanceId'].isnull().sum()))

    # check for ambiguous mappings (i.e. fp id maps to multiple sp/lp ids)
    if any(id_map.drop_duplicates().groupby(['LIMS Provider', 'LIMS ID']).size() > 1):
        raise Exception('Ambiguous id mappings found')

    fp_with_lims_provenance = utils.pandas.merge_using_id_map(fp, pd.concat([sp, lp], sort=False),
                                                              ['LIMS Provider', 'LIMS ID'],
                                                              ['provenanceId'],
                                                              id_map.drop_duplicates(keep='first'))

    fp_no_match = fp.loc[fp.index.isin(id_map[id_map['provenanceId'].isnull()].index)]

    if len(fp_no_match) + len(fp_with_lims_provenance) != len(fp):
        raise Exception('')

    return fp_with_lims_provenance


def generate_workflow_run_hierarchy(fp):
    wfr = fp.loc[fp['Workflow Run SWID'].notnull(),
                 ['Workflow Run Input File SWAs', 'Workflow Run SWID']].drop_duplicates('Workflow Run SWID')

    wfr_with_inputs = wfr.loc[wfr['Workflow Run Input File SWAs'].notnull()]
    wfr_no_inputs = wfr.loc[wfr['Workflow Run Input File SWAs'].isnull()]

    wfr_file_in = pd.DataFrame({
        col: np.repeat(wfr_with_inputs[col].values, wfr_with_inputs['Workflow Run Input File SWAs'].str.len())
        for col in wfr_with_inputs.columns.difference(['Workflow Run Input File SWAs'])
    })

    if len(wfr_with_inputs['Workflow Run Input File SWAs'].values) > 0:
        wfr_file_in['Workflow Run Input File SWAs'] = np.concatenate(
            wfr_with_inputs['Workflow Run Input File SWAs'].values)
    else:
        wfr_file_in['Workflow Run Input File SWAs'] = []

    wfr_file_in = wfr_file_in.append(wfr_no_inputs, ignore_index=True, sort=True)
    wfr_file_in['Workflow Run Input File SWAs'] = wfr_file_in['Workflow Run Input File SWAs'].astype('float')

    input_ids_missing_in_fp = set(wfr_file_in['Workflow Run Input File SWAs'].dropna()) - set(fp['File SWID'].dropna())
    if len(input_ids_missing_in_fp) > 0:
        log.warning('Found {} input file ids without matching record'.format(len(input_ids_missing_in_fp)))

    # file + wfr
    file_to_wfr = fp.loc[fp['Workflow Run SWID'].notnull() & fp['File SWID'].notnull(),
                         ['File SWID', 'Workflow Run SWID']].drop_duplicates()

    wfr_hierarchy = pd.merge(file_to_wfr, wfr_file_in, how='left', left_on='File SWID',
                             right_on='Workflow Run Input File SWAs')
    wfr_hierarchy = wfr_hierarchy[['Workflow Run SWID_x', 'Workflow Run SWID_y']]
    wfr_hierarchy.columns = ['parent', 'child']
    wfr_hierarchy = wfr_hierarchy.drop_duplicates()

    return wfr_hierarchy, input_ids_missing_in_fp
