import json
import timeit

import numpy as np
import pandas as pd

import loaders as dl
import utils as u
from context.analysis import DataContext
from utils.common import *
from utils.file import getpath


def get_fpr_with_lims_provenance(fpr_to_provenance_map):
    start_time = timeit.default_timer()
    log.info('Started loading lane provenance from {}'.format(config.lp_path))
    lpj = json.load(getpath(config.lp_path).open())
    lp = pd.io.json.json_normalize(lpj)
    lp.createdDate = pd.to_datetime(lp.createdDate)
    lp.lastModified = pd.to_datetime(lp.lastModified)
    lp = lp.rename(columns={'laneProvenanceId': 'provenanceId'})
    lp['provider'] = config.provider_id

    # file provenance generation adds underscores
    lp['iusTag'] = 'NoIndex'
    lp['sequencerRunPlatformModel'] = lp['sequencerRunPlatformModel'].str.replace(' ', '_')

    log.info(
        'Completed loading {} lane provenance records in {:.1f}s'.format(len(lp), timeit.default_timer() - start_time))

    start_time = timeit.default_timer()
    log.info('Started loading sample provenance from {}'.format(config.sp_path))
    spj = json.load(getpath(config.sp_path).open())
    sp = pd.io.json.json_normalize(spj)
    sp.createdDate = pd.to_datetime(sp.createdDate)
    sp.lastModified = pd.to_datetime(sp.lastModified)
    sp = sp.rename(columns={'sampleProvenanceId': 'provenanceId'})
    sp['provider'] = config.provider_id

    # file provenance generation adds underscores
    sp['sequencerRunPlatformModel'] = sp['sequencerRunPlatformModel'].str.replace(' ', '_')

    log.info(
        'Completed loading {} sample provenance records in {:.1f}s'.format(len(sp),
                                                                           timeit.default_timer() - start_time))

    # load file provenance
    start_time = timeit.default_timer()
    log.info('Started loading file provenance from {}'.format(config.fpr_path))
    fpr = dl.load_file_provenance(config.fpr_path)
    log.info('Completed loading {} file provenance records in {:.1f}s'.format(len(fpr),
                                                                              timeit.default_timer() - start_time))

    # validate that all data fields are mapped or filtered
    log.info('Validating data...')

    sp_cols_diff = set(sp.columns) - set(fpr_to_provenance_map.values()) - set(config.sp_ignore_fields)
    if sp_cols_diff:
        raise Exception('Missing sample provenance mapping for: {}'.format(sp_cols_diff))
    lp_cols_diff = set(lp.columns) - set(fpr_to_provenance_map.values()) - set(config.lp_ignore_fields)
    if lp_cols_diff:
        raise Exception('Missing lane provenance mapping for: {}'.format(lp_cols_diff))
    fpr_cols_diff = set(fpr.columns) - set(fpr_to_provenance_map.keys()) - set(config.fpr_ignore_fields)
    if fpr_cols_diff:
        raise Exception('Missing file provenance mapping for: {}'.format(fpr_cols_diff))

    # join fpr and sp/lp
    log.info('Linking data...')

    # generate current to new id mapping

    is_sample_record = fpr['Sequencer Run Name'].notnull() & \
                       fpr['Lane Number'].notnull() & \
                       fpr['Sample Name'].notnull() & \
                       fpr['Study Title'].notnull()

    is_lane_record = fpr['Sequencer Run Name'].notnull() & \
                     fpr['Lane Number'].notnull() & \
                     fpr['Sample Name'].isnull() & \
                     fpr['Study Title'].isnull()

    # fpr records that are not of type sample or lane provenance
    unknown_record_type = ~(is_sample_record | is_lane_record)
    if unknown_record_type.sum() > 0:
        log.info('Found {} / {} unknown record types'.format(unknown_record_type.sum(), len(fpr)))
        # raise Exception('Unknown data type encountered')

    # map fpr records to input sample and lane provenance

    def apply_mapping(from_data, to_data, from_fields, to_fields, id_map):
        log.debug('Mapping records using [{}] to [{}]'.format('+'.join(from_fields), '+'.join(to_fields)))
        new_id_map = u.update_id_map(from_data, to_data,
                                     ['LIMS Provider', 'LIMS ID'], from_fields,
                                     ['provenanceId'], to_fields)
        return u.merge_id_map(id_map, new_id_map, ['LIMS Provider', 'LIMS ID'])

    id_map = fpr.loc[:, ['LIMS Provider', 'LIMS ID']]
    id_map['provenanceId'] = np.nan

    # if has_missing(id_map, 'provenanceId'):
    if any(id_map['provenanceId'].isnull()):
        from_fields = ['LIMS Provider', 'LIMS ID']
        to_fields = ['provider', 'provenanceId']
        id_map = apply_mapping(fpr[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['LIMS Provider', 'LIMS ID']
        to_fields = ['provider', 'provenanceId']
        id_map = apply_mapping(fpr[is_lane_record], lp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'IUS Tag', 'Sample Name']
        to_fields = ['sequencerRunName', 'laneNumber', 'iusTag', 'sampleName']
        id_map = apply_mapping(fpr[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'IUS Tag']
        to_fields = ['sequencerRunName', 'laneNumber', 'iusTag']
        id_map = apply_mapping(fpr[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'Sample Name']
        to_fields = ['sequencerRunName', 'laneNumber', 'sampleName']
        id_map = apply_mapping(fpr[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'Root Sample Name']
        to_fields = ['sequencerRunName', 'laneNumber', 'rootSampleName']
        id_map = apply_mapping(fpr[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number', 'Sample Attributes.geo_external_name']
        to_fields = ['sequencerRunName', 'laneNumber', 'sampleAttributes.geo_external_name']
        id_map = apply_mapping(fpr[is_sample_record], sp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        from_fields = ['Sequencer Run Name', 'Lane Number']
        to_fields = ['sequencerRunName', 'laneNumber']
        id_map = apply_mapping(fpr[is_lane_record], lp, from_fields, to_fields, id_map)

    if any(id_map['provenanceId'].isnull()):
        log.warning('Unable to map {} file provenance records'.format(
            id_map['provenanceId'].isnull().sum()))

    # check for ambiguous mappings (i.e. fpr id maps to multiple sp/lp ids)
    if any(id_map.drop_duplicates().groupby(['LIMS Provider', 'LIMS ID']).size() > 1):
        raise Exception('Ambiguous id mappings found')

    fpr_with_lims_provenance = u.merge_using_id_map(fpr, pd.concat([sp, lp]), ['LIMS Provider', 'LIMS ID'],
                                                    ['provenanceId'],
                                                    id_map.drop_duplicates(keep='first'))

    fpr_no_match = fpr.loc[fpr.index.isin(id_map[id_map['provenanceId'].isnull()].index)]

    if len(fpr_no_match) + len(fpr_with_lims_provenance) != len(fpr):
        raise Exception('')

    return fpr_with_lims_provenance


def get_changes(fpr_with_lims_provenance, fpr_to_provenance_map):
    changes = pd.DataFrame(columns=['field', 'from', 'to'])
    for from_field, to_field in fpr_to_provenance_map.items():
        log.debug('Comparing {} to {}'.format(from_field, to_field))
        changes = changes.append(u.generate_changes(fpr_with_lims_provenance, from_field, to_field))

    # convert to categorical to reduce size
    changes['field'] = changes['field'].astype('category')
    changes['from'] = changes['from'].apply(u.convert_to_string).astype('category')
    changes['to'] = changes['to'].apply(u.convert_to_string).astype('category')

    # include additional fields required when filtering of changes
    additional_fields = fpr_with_lims_provenance.loc[:, ['Workflow Name', 'LIMS Provider', 'Last Modified']]
    additional_fields.columns = ['workflow', 'provider', 'processing_date']
    additional_fields['workflow'] = additional_fields['workflow'].astype('category')
    additional_fields['provider'] = additional_fields['provider'].astype('category')
    additional_fields['processing_date'] = pd.to_datetime(additional_fields['processing_date'])
    changes = changes.reset_index().merge(additional_fields.reset_index(), how='left', on='index').set_index('index')
    return changes


def generate_workflow_run_hierarchy(fpr):
    wfr = fpr.loc[fpr['Workflow Run SWID'].notnull(),
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

    input_ids_missing_in_fpr = set(wfr_file_in['Workflow Run Input File SWAs'].dropna()) - set(
        fpr['File SWID'].dropna())
    if len(input_ids_missing_in_fpr) > 0:
        log.warn('Found {} input file ids without matching record'.format(len(input_ids_missing_in_fpr)))

    # file + wfr
    file_to_wfr = fpr.loc[fpr['Workflow Run SWID'].notnull() & fpr['File SWID'].notnull(),
                          ['File SWID', 'Workflow Run SWID']].drop_duplicates()

    wfr_hierarchy = pd.merge(file_to_wfr, wfr_file_in, how='left', left_on='File SWID',
                             right_on='Workflow Run Input File SWAs')
    wfr_hierarchy = wfr_hierarchy[['Workflow Run SWID_x', 'Workflow Run SWID_y']]
    wfr_hierarchy.columns = ['parent', 'child']
    wfr_hierarchy = wfr_hierarchy.drop_duplicates()

    return wfr_hierarchy, input_ids_missing_in_fpr


def get_related_workflow_runs(fpr, index, hierarchy):
    direct_wfr_swids = fpr.loc[fpr.index.isin(index), 'Workflow Run SWID'].drop_duplicates().tolist()

    def get_downstream(xs):
        if len(xs) > 1:
            return get_downstream([xs[0]]) + get_downstream(xs[1:])
        elif len(xs) == 1:
            return [xs[0]] + get_downstream(
                hierarchy.loc[hierarchy['parent'] == xs[0], 'child'].dropna().tolist())
        else:
            return []

    return pd.Series(get_downstream(direct_wfr_swids)).drop_duplicates().astype('int')


def load(fpr_to_provenance_map):
    fpr_with_lims_provenance = get_fpr_with_lims_provenance(fpr_to_provenance_map)
    changes = get_changes(fpr_with_lims_provenance, fpr_to_provenance_map)
    wfr_hierarchy, _ = generate_workflow_run_hierarchy(fpr_with_lims_provenance)
    return DataContext(fpr_with_lims_provenance, changes, wfr_hierarchy)