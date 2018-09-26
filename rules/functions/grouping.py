import utils.transformations as u


def grouping_logic_okay(fpr, changes):
    # only tissue prep and region changes are filtering candidates for this rule
    allowed_field_changes = ['Sample Attributes.geo_tissue_preparation', 'Sample Attributes.geo_tissue_region']

    current = ['Root Sample Name',
               'Sample Attributes.geo_tissue_origin',
               'Sample Attributes.geo_tissue_type',
               'Sample Attributes.geo_tissue_preparation',
               'Sample Attributes.geo_tissue_region',
               'Sample Attributes.geo_library_source_template_type',
               'Sample Attributes.geo_group_id',
               'Sample Attributes.geo_targeted_resequencing']

    new = ['rootSampleName',
           'sampleAttributes.geo_tissue_origin',
           'sampleAttributes.geo_tissue_type',
           'sampleAttributes.geo_tissue_preparation',
           'sampleAttributes.geo_tissue_region',
           'sampleAttributes.geo_library_source_template_type',
           'sampleAttributes.geo_group_id',
           'sampleAttributes.geo_targeted_resequencing']

    group = ''
    for i in current:
        if i in fpr.columns:
            group += fpr[i].apply(u.convert_to_string).fillna('')
    fpr = fpr.assign(current_group=group)

    group = ''
    for i in new:
        if i in fpr.columns:
            group += fpr[i].apply(u.convert_to_string).fillna('')
    fpr = fpr.assign(new_group=group)

    fpr_groups = fpr[['current_group', 'new_group']].drop_duplicates()

    splits = fpr[['current_group', 'new_group']].drop_duplicates().groupby('current_group').filter(
        lambda x: len(x) > 1)
    splits2 = splits.loc[splits['current_group'] != splits['new_group']]

    merges = fpr[['current_group', 'new_group']].drop_duplicates().groupby('new_group').filter(
        lambda x: len(x) > 1)
    merges2 = merges.loc[merges['current_group'] != merges['new_group']]

    def is_okay(df):
        if any(df.index.isin(merges2.index)):
            return False
        elif any(df.index.isin(splits2.index)):
            return False
        elif len(df) == 1:
            return True
        elif (df['current_group'].nunique() == 1) & (df['new_group'].nunique() == 1):
            return True
        elif all(df['current_group'] == df['new_group']):
            return True
        else:
            return False

    groupby_field = 'File SWID'
    okay_group_key_changes = fpr.groupby(groupby_field).apply(is_okay)
    okay_fpr_group_changes = fpr.loc[fpr[groupby_field].isin(okay_group_key_changes[okay_group_key_changes].index)]

    return changes.index.isin(okay_fpr_group_changes.index) & changes['field'].isin(allowed_field_changes)
