import pandas as pd


def convert_to_string(x):
    if isinstance(x, list):
        return ','.join(x)
    else:
        return x


def merge_id_map(x, y, keys):
    if any(x.columns != y.columns):
        raise Exception('columns must be equal')

    suffix = '_new'
    target_keys = list(x.columns.difference(keys))
    new_keys = [str(col) + suffix for col in target_keys]

    m = x.reset_index().merge(y, how='left', on=keys, indicator=True, suffixes=['', suffix]).set_index('index')
    if len(x) != len(m):
        raise Exception('input has duplicates')

    m.loc[m[target_keys].isnull().any(axis=1) & ~m[new_keys].isnull().any(axis=1), target_keys] = \
        m.loc[m[target_keys].isnull().any(axis=1) & ~m[new_keys].isnull().any(axis=1), new_keys].values
    m.drop(columns=new_keys + ['_merge'], inplace=True)
    return m


def update_id_map(from_df, to_df, id_map_from_cols, from_cols, id_map_to_cols, to_cols):
    from_keys = from_df.loc[from_df.loc[:, from_cols].notnull().all(axis=1), id_map_from_cols]
    from_keys['join_key'] = from_df.loc[:, from_cols].astype(str).sum(axis=1)

    to_keys = to_df.loc[to_df.loc[:, to_cols].notnull().all(axis=1), id_map_to_cols]
    to_keys['join_key'] = to_df.loc[:, to_cols].astype(str).sum(axis=1)

    merge_from_to = pd.merge(from_keys, to_keys, on='join_key', how='left', indicator=True)
    merge_from_to_no_dups_no_missing = merge_from_to.loc[(merge_from_to['_merge'] == 'both') &
                                                         ~merge_from_to.index.isin(merge_from_to.index.duplicated())]

    new_id_map = merge_from_to_no_dups_no_missing.loc[:, id_map_from_cols + id_map_to_cols]
    new_id_map.drop_duplicates(inplace=True)
    new_id_map.drop_duplicates(id_map_from_cols, keep=False, inplace=True)
    return new_id_map
