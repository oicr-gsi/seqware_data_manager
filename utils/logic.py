import numpy as np


def string_match(df, col, pattern):
    if pattern == '*':
        return True
    elif pattern == '' or (type(pattern) == float and np.isnan(pattern)):
        return df[col].isnull() | (df[col] == '');
    elif pattern.startswith('^') and pattern.endswith('$'):
        return df[col].str.match('^' + pattern + '$')
    else:
        return df[col] == pattern


def greater_than_or_equals(df, field, value):
    if value == '*':
        return True
    else:
        return df[field] >= value


def less_than(df, field, value):
    if value == '*':
        return True
    else:
        return df[field] < value