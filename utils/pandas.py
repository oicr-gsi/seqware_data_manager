import pandas as pd
import numpy as np
import warnings

def printw(x):
    with pd.option_context('display.max_rows', -1, 'display.max_columns', 5):
        print('hi')
        print(x)


def increase_width():
    pd.set_option('display.height', 1000)
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


def merge_using_id_map(left, right, left_keys, right_keys, id_map):
    left_id_map = left.reset_index().merge(id_map, how='left', on=left_keys, indicator=True).set_index('index')
    left_id_map.drop(left_id_map[left_id_map['_merge'] == 'left_only'].index, inplace=True)
    left_id_map.drop(columns=['_merge'], inplace=True)

    left_right = left_id_map.reset_index().merge(right, how='left', on=right_keys, indicator=True).set_index('index')
    left_right.drop(left_right[left_right['_merge'] == 'left_only'].index, inplace=True)
    left_right.drop(columns=['_merge'], inplace=True)
    return left_right


def generate_changes(df, from_field, to_field):
    if df.columns.contains(from_field) and df.columns.contains(to_field):
        y = df.loc[(df[from_field] != df[to_field]) &
                   ~(df[from_field].isnull() & df[to_field].isnull()),
                   [from_field, to_field]]
        y.rename(columns={from_field: 'from', to_field: 'to'}, inplace=True)
        y.insert(0, 'field', from_field)
        return y
    elif df.columns.contains(from_field) and not df.columns.contains(to_field):
        # field deletions
        y = df.loc[df[from_field].notnull(), [from_field]]
        y.rename(columns={from_field: 'from'}, inplace=True)
        y['to'] = np.nan
        y.insert(0, 'field', from_field)
        return y
    elif not df.columns.contains(from_field) and df.columns.contains(to_field):
        # field additions
        y = df.loc[df[to_field].notnull(), [to_field]]
        y.rename(columns={to_field: 'to'}, inplace=True)
        y.insert(0, 'field', from_field)
        y.insert(1, 'from', np.nan)
        return y
    else:
        warnings.warn('Fields {} and {} are not present in collection {}'.format(from_field, to_field, df.name))
        return None


#print(u.get_change_summary(fpr, ['Study Title','Workflow Name'], changes_not_allowed))
def get_change_summary(fpr, fpr_cols, c, field_val=None, from_val=None, to_val=None):
    cond = pd.Series(True,index=c.index)
    if field_val is not None:
        cond = cond & (c['field'].astype(str).fillna('') == field_val)
    if from_val is not None:
        cond = cond & (c['from'].astype(str).fillna('') == from_val)
    if to_val is not None:
        cond = cond & (c['to'].astype(str).fillna('') == to_val)
    return fpr.loc[fpr.index.isin(c.loc[cond].index), fpr_cols].astype(str).fillna('').apply(lambda x: '+'.join(x),
                                                                                            axis=1).value_counts()