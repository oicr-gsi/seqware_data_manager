import collections
import json
import logging

import numpy as np
import pandas as pd

import utils.pandas
import utils.transformations
from loader import lane_provenance, sample_provenance, file_provenance

log = logging.getLogger(__name__)

default_fp_to_provenance_map_str = """{
    "Study Title": "studyTitle",
    "Sequencer Run Name": "sequencerRunName",
    "Lane Number": "laneNumber",
    "Sequencer Run Platform Name": "sequencerRunPlatformModel",
    "Root Sample Name": "rootSampleName",
    "Parent Sample Name": "parentSampleName",
    "Sample Name": "sampleName",
    "IUS Tag": "iusTag",
    "Sample Attributes.geo_external_name": "sampleAttributes.geo_external_name",
    "Sample Attributes.geo_group_id": "sampleAttributes.geo_group_id",
    "Sample Attributes.geo_group_id_description": "sampleAttributes.geo_group_id_description",
    "Sample Attributes.geo_library_source_template_type": "sampleAttributes.geo_library_source_template_type",
    "Sample Attributes.geo_nanodrop_concentration": "sampleAttributes.geo_nanodrop_concentration",
    "Sample Attributes.geo_organism": "sampleAttributes.geo_organism",      
    "Sample Attributes.geo_prep_kit": "sampleAttributes.geo_prep_kit",
    "Sample Attributes.geo_purpose": "sampleAttributes.geo_purpose",
    "Sample Attributes.geo_qubit_concentration": "sampleAttributes.geo_qubit_concentration",
    "Sample Attributes.geo_receive_date": "sampleAttributes.geo_receive_date",
    "Sample Attributes.geo_run_id_and_position": "sampleAttributes.geo_run_id_and_position",
    "Sample Attributes.geo_str_result": "sampleAttributes.geo_str_result",
    "Sample Attributes.geo_targeted_resequencing": "sampleAttributes.geo_targeted_resequencing",
    "Sample Attributes.geo_template_type": "sampleAttributes.geo_template_type",
    "Sample Attributes.geo_tissue_origin": "sampleAttributes.geo_tissue_origin",
    "Sample Attributes.geo_tissue_preparation": "sampleAttributes.geo_tissue_preparation",
    "Sample Attributes.geo_tissue_region": "sampleAttributes.geo_tissue_region",
    "Sample Attributes.geo_tissue_type": "sampleAttributes.geo_tissue_type",
    "Parent Sample Attributes.geo_external_name": "sampleAttributes.geo_external_name",
    "Parent Sample Attributes.geo_group_id": "sampleAttributes.geo_group_id",
    "Parent Sample Attributes.geo_group_id_description": "sampleAttributes.geo_group_id_description",
    "Parent Sample Attributes.geo_library_source_template_type": "sampleAttributes.geo_library_source_template_type",
    "Parent Sample Attributes.geo_nanodrop_concentration": "sampleAttributes.geo_nanodrop_concentration",
    "Parent Sample Attributes.geo_organism": "sampleAttributes.geo_organism",
    "Parent Sample Attributes.geo_prep_kit": "sampleAttributes.geo_prep_kit",
    "Parent Sample Attributes.geo_purpose": "sampleAttributes.geo_purpose",
    "Parent Sample Attributes.geo_qubit_concentration": "sampleAttributes.geo_qubit_concentration",
    "Parent Sample Attributes.geo_receive_date": "sampleAttributes.geo_receive_date",
    "Parent Sample Attributes.geo_run_id_and_position": "sampleAttributes.geo_run_id_and_position",
    "Parent Sample Attributes.geo_str_result": "sampleAttributes.geo_str_result",
    "Parent Sample Attributes.geo_targeted_resequencing": "sampleAttributes.geo_targeted_resequencing",
    "Parent Sample Attributes.geo_template_type": "sampleAttributes.geo_template_type",
    "Parent Sample Attributes.geo_tissue_origin": "sampleAttributes.geo_tissue_origin",
    "Parent Sample Attributes.geo_tissue_preparation": "sampleAttributes.geo_tissue_preparation",
    "Parent Sample Attributes.geo_tissue_region": "sampleAttributes.geo_tissue_region",
    "Parent Sample Attributes.geo_tissue_type": "sampleAttributes.geo_tissue_type",
    "LIMS Version":"version",
    "LIMS ID": "provenanceId",
    "LIMS Last Modified": "lastModified",
    "LIMS Provider": "provider",
    "Sequencer Run Attributes.instrument_name":"sequencerRunAttributes.instrument_name",
    "Sequencer Run Attributes.run_dir":"sequencerRunAttributes.run_dir",
    "Lane Attributes.pool_name": "laneAttributes.pool_name",
    "Sample Attributes.institute": "sampleAttributes.institute",
    "Sample Attributes.subproject": "sampleAttributes.subproject",
    "Sample Attributes.geo_tube_id": "sampleAttributes.geo_tube_id",
    "Sequencer Run Attributes.run_bases_mask": "sequencerRunAttributes.run_bases_mask"
}
"""

default_sp_ignore_fields = ['skip',
                            'createdDate']

default_lp_ignore_fields = ['skip',
                            'createdDate']

default_fp_ignore_fields = ['Sample Attributes.run_yielded_SE_read',
                            'Sample Attributes.geo_template_id',
                            'Sample Attributes.geo_tube_id',
                            'Sample Attributes.geo_run_id_and_position_and_slot',
                            'Sample Attributes.geo_qpcr_percentage_human',
                            'Parent Sample Attributes.geo_run_id_and_position_and_slot',
                            'Parent Sample Attributes.geo_template_id',
                            'Parent Sample Attributes.run_yielded_SE_read',
                            'Parent Sample Attributes.geo_qpcr_percentage_human',
                            'Parent Sample Attributes.geo_reaction_id',
                            'Parent Sample Attributes.geo_tube_id',
                            'Sequencer Run Attributes.IndxRd_1',
                            'Sequencer Run Attributes.geo_instrument_run_id',
                            'Lane Attributes.geo_lane',
                            'Study Attributes.geo_lab_group_id',
                            'Workflow Run Attributes',
                            'Workflow Attributes',
                            'Workflow Run SWID',
                            'Workflow Version',
                            'File Path',
                            'Experiment Name',
                            'File SWID',
                            'Parent Sample SWID',
                            'Root Sample SWID',
                            'Parent Sample Organism IDs',
                            'Sample Attributes.geo_reaction_id',
                            'Study SWID',
                            'Processing SWID',
                            'Workflow Name',
                            'Workflow Run Status',
                            'File Attributes',
                            'Path Skip',
                            'File Meta-Type',
                            'Skip',
                            'File Size',
                            'Status',
                            'Sequencer Run SWID',
                            'LIMS IUS SWID',
                            'Processing Algorithm',
                            'Processing Attributes',
                            'IUS Attributes',
                            'Status Reason',
                            'Sample Organism ID',
                            'File Md5sum',
                            'Experiment SWID',
                            'Lane SWID',
                            'Processing Status',
                            'Workflow Run Name',
                            'Workflow Run Attributes',
                            'Last Modified',
                            'Lane Name',
                            'Experiment Attributes',
                            'Sample Organism Code',
                            'IUS SWID',
                            'Workflow SWID',
                            'File Description',
                            'Sample SWID',
                            'Workflow Run Input File SWAs',
                            'Sequencer Run Platform ID',
                            'Sample Attributes.skip',
                            'Sequencer Run Attributes.skip',
                            'Parent Sample Attributes.skip',
                            'Lane Attributes.skip'
                            ]


def get_fp_with_lims_provenance(lane_provenance_file_path,
                                sample_provenance_file_path,
                                provider,
                                file_provenance_file_path,
                                fp_to_provenance_map=default_fp_to_provenance_map_str,
                                sp_ignore_fields=default_sp_ignore_fields,
                                lp_ignore_fields=default_lp_ignore_fields,
                                fp_ignore_fields=default_fp_ignore_fields):
    if isinstance(fp_to_provenance_map, str):
        fp_to_provenance_map = json.loads(fp_to_provenance_map, object_pairs_hook=collections.OrderedDict)

    log.info('Loading data...')
    lp = lane_provenance.load(lane_provenance_file_path, provider)
    sp = sample_provenance.load(sample_provenance_file_path, provider)
    fp = file_provenance.load(file_provenance_file_path)

    # validate that all data fields are mapped or filtered
    log.info('Validating data...')

    sp_cols_diff = set(sp.columns) - set(fp_to_provenance_map.values()) - set(sp_ignore_fields)
    if sp_cols_diff:
        raise Exception('Missing sample provenance mapping for: {}'.format(sp_cols_diff))
    lp_cols_diff = set(lp.columns) - set(fp_to_provenance_map.values()) - set(lp_ignore_fields)
    if lp_cols_diff:
        raise Exception('Missing lane provenance mapping for: {}'.format(lp_cols_diff))
    fp_cols_diff = set(fp.columns) - set(fp_to_provenance_map.keys()) - set(fp_ignore_fields)
    if fp_cols_diff:
        raise Exception('Missing file provenance mapping for: {}'.format(fp_cols_diff))

    # join fp and sp/lp
    log.info('Linking data...')

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
        new_id_map = utils.transformations.update_id_map(from_data, to_data,
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


def get_changes(fp_with_lims_provenance, fp_to_provenance_map=default_fp_to_provenance_map_str):
    if isinstance(fp_to_provenance_map, str):
        fp_to_provenance_map = json.loads(fp_to_provenance_map, object_pairs_hook=collections.OrderedDict)

    changes = pd.DataFrame(columns=['field', 'from', 'to'])
    for from_field, to_field in fp_to_provenance_map.items():
        log.debug('Comparing {} to {}'.format(from_field, to_field))
        changes = changes.append(utils.pandas.generate_changes(fp_with_lims_provenance, from_field, to_field))

    # convert to categorical to reduce size
    changes['field'] = changes['field'].astype('category')
    changes['from'] = changes['from'].apply(utils.transformations.convert_to_string).astype('category')
    changes['to'] = changes['to'].apply(utils.transformations.convert_to_string).astype('category')

    # include additional fields required when filtering of changes
    additional_fields = fp_with_lims_provenance.loc[:, ['Workflow Name', 'LIMS Provider', 'Last Modified']]
    additional_fields.columns = ['workflow', 'provider', 'processing_date']
    additional_fields['workflow'] = additional_fields['workflow'].astype('category')
    additional_fields['provider'] = additional_fields['provider'].astype('category')
    additional_fields['processing_date'] = pd.to_datetime(additional_fields['processing_date'])
    changes = changes.reset_index().merge(additional_fields.reset_index(), how='left', on='index').set_index('index')
    return changes


def generate_workflow_run_hierarchy(fp):
    wfr = fp.loc[fp['Workflow Run SWID'].notnull(),
                 ['Workflow Run Input File SWAs', 'Workflow Run SWID']].drop_duplicates(
        'Workflow Run SWID')

    wfr_with_inputs = wfr.loc[wfr['Workflow Run Input File SWAs'].notnull()]
    wfr_no_inputs = wfr.loc[wfr['Workflow Run Input File SWAs'].isnull()]

    wfr_file_in = pd.DataFrame({
        col: np.repeat(wfr_with_inputs[col].values, wfr_with_inputs['Workflow Run Input File SWAs'].str.len())
        for col in wfr_with_inputs.columns.difference(['Workflow Run Input File SWAs'])
    }).assign(
        **{'Workflow Run Input File SWAs': np.concatenate(wfr_with_inputs['Workflow Run Input File SWAs'].values)})
    wfr_file_in = wfr_file_in.append(wfr_no_inputs, ignore_index=True)
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


def get_related_workflow_runs(fp, index, hierarchy):
    direct_wfr_swids = fp.loc[fp.index.isin(index), 'Workflow Run SWID'].drop_duplicates().tolist()

    def get_downstream(xs):
        if len(xs) > 1:
            return get_downstream([xs[0]]) + get_downstream(xs[1:])
        elif len(xs) == 1:
            return [xs[0]] + get_downstream(
                hierarchy.loc[hierarchy['parent'] == xs[0], 'child'].dropna().tolist())
        else:
            return []

    return pd.Series(get_downstream(direct_wfr_swids)).drop_duplicates().astype('int')
