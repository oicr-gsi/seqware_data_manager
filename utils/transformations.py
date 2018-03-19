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
    # from_keys = from_df.loc[:, id_map_from_cols]
    # from_keys['join_key'] = from_df.loc[:, from_cols].astype(str).sum(axis=1)
    from_keys = from_df.loc[from_df.loc[:, from_cols].notnull().all(axis=1), id_map_from_cols]
    from_keys['join_key'] = from_df.loc[:, from_cols].astype(str).sum(axis=1)

    # to_keys = to_df.loc[:, id_map_to_cols]
    # to_keys['join_key'] = to_df.loc[:, to_cols].astype(str).sum(axis=1)
    to_keys = to_df.loc[to_df.loc[:, to_cols].notnull().all(axis=1), id_map_to_cols]
    to_keys['join_key'] = to_df.loc[:, to_cols].astype(str).sum(axis=1)

    merge_from_to = pd.merge(from_keys, to_keys, on='join_key', how='left', indicator=True)
    merge_from_to_no_dups_no_missing = merge_from_to.loc[(merge_from_to['_merge'] == 'both') &
                                                         ~merge_from_to.index.isin(merge_from_to.index.duplicated())]

    new_id_map = merge_from_to_no_dups_no_missing.loc[:, id_map_from_cols + id_map_to_cols]
    # new_id_map.columns = id_map.columns
    new_id_map.drop_duplicates(inplace=True)
    new_id_map.drop_duplicates(id_map_from_cols, keep=False, inplace=True)
    # new_id_map.drop(new_id_map.loc[new_id_map.loc[:, ['provider', 'from_id']].duplicated()].index, inplace=True)
    return new_id_map

    # m1 = id_map.reset_index().merge(new_id_map, how='left', on=id_map_from_cols, indicator=True).set_index('index')
    # if len(id_map) != len(m1):
    #     raise Exception('mismatch in length')
    # m1.drop(m1.loc[m1.index.isin(m1.index.duplicated())].index, inplace=True)
    # id_map.loc[id_map[id_map_to_cols].isnull(), id_map_to_cols] = m1.loc[id_map[id_map_to_cols].isnull(), 'to_id_y']