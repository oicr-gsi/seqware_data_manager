import utils.transformations as u


def grouping_logic_okay(data, changes, allowed_field_changes=None, group_by_type=None, group_by_field=None,
                        current_group_fields=None, new_group_fields=None):
    if allowed_field_changes is None:
        allowed_field_changes = ['Sample Attributes.geo_tissue_preparation', 'Sample Attributes.geo_tissue_region']

    if group_by_type is None:
        group_by_type = ['Workflow Name']

    if group_by_field is None:
        group_by_field = 'File SWID'

    if current_group_fields is None:
        current_group_fields = ['Root Sample Name',
                                'Sample Attributes.geo_tissue_origin',
                                'Sample Attributes.geo_tissue_type',
                                'Sample Attributes.geo_tissue_preparation',
                                'Sample Attributes.geo_tissue_region',
                                'Sample Attributes.geo_library_source_template_type',
                                'Sample Attributes.geo_group_id',
                                'Sample Attributes.geo_targeted_resequencing']

    if new_group_fields is None:
        new_group_fields = ['rootSampleName',
                            'sampleAttributes.geo_tissue_origin',
                            'sampleAttributes.geo_tissue_type',
                            'sampleAttributes.geo_tissue_preparation',
                            'sampleAttributes.geo_tissue_region',
                            'sampleAttributes.geo_library_source_template_type',
                            'sampleAttributes.geo_group_id',
                            'sampleAttributes.geo_targeted_resequencing']

    data['current_group'] = data[current_group_fields].applymap(u.convert_to_string).fillna('').astype(str).sum(axis=1)
    data['new_group'] = data[new_group_fields].applymap(u.convert_to_string).fillna('').astype(str).sum(axis=1)

    current_group_key = group_by_type + ['current_group']
    new_group_key = group_by_type + ['new_group']
    current_grouped_data = data.groupby(current_group_key)
    new_grouped_data = data.groupby(new_group_key)

    # check if current group records equals new group records
    okay_current_to_new_group_changes = data.groupby(group_by_type + ['current_group', 'new_group']).apply(
        lambda x: current_grouped_data.get_group(tuple(x[current_group_key].values[0])).index.equals(
            new_grouped_data.get_group(tuple(x[new_group_key].values[0])).index)
    ).reset_index(name='grouping_is_okay')

    # merge data with okay_current_to_new_group_changes
    data = data.reset_index().merge(okay_current_to_new_group_changes,
                                    on=group_by_type + ['current_group', 'new_group']).set_index('index')

    # for each grouping, check all group name changes are valid
    okay_group_changes = data.groupby(group_by_field).apply(lambda df: all(df['grouping_is_okay']))

    # select (by group_by_field) all data records with okay group changes
    okay_data_group_changes = data.loc[data[group_by_field].isin(okay_group_changes[okay_group_changes].index)]

    # return changes that are in okay data grouping changes set and is an allowed field change
    return changes.index.isin(okay_data_group_changes.index) & changes['field'].isin(allowed_field_changes)
