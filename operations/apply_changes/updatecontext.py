import pandas as pd
from sqlalchemy import TIMESTAMP

from models.context import BaseContext
from operations.analyze_changes.rulecontext import RuleContext
from utils.file import get_file_path


class UpdateLimsKeyContext(BaseContext):
    def __init__(self, from_table: pd.DataFrame, to_table: pd.DataFrame):
        self.from_table = from_table
        self.to_table = to_table

    @classmethod
    def generate_updates(cls, ctx: RuleContext):
        cols = ['lims_ius_swid', 'lims_id', 'lims_version', 'lims_last_modified', 'lims_provider']

        from_table_okay = ctx.fpr.loc[~ctx.fpr.index.isin(ctx.changes_blocked.index),
                                      ['LIMS IUS SWID', 'LIMS ID', 'LIMS Version', 'LIMS Last Modified',
                                       'LIMS Provider']]
        from_table_okay.columns = cols

        to_table_okay = ctx.fpr.loc[~ctx.fpr.index.isin(ctx.changes_blocked.index),
                                    ['LIMS IUS SWID', 'provenanceId', 'version', 'lastModified', 'provider']]
        to_table_okay.columns = cols

        no_update_required_mask = (from_table_okay == to_table_okay).all(axis=1)
        from_table_pre = from_table_okay[~no_update_required_mask]
        to_table_pre = to_table_okay[~no_update_required_mask]

        from_table = from_table_pre.drop_duplicates().reset_index(drop=True)
        to_table = to_table_pre.drop_duplicates().reset_index(drop=True)

        from_to_table = pd.merge(from_table, to_table, on='lims_ius_swid', suffixes=['_from', '_to'], how='left',
                                 indicator=True)
        if not (from_to_table['_merge'] == 'both').all():
            raise Exception('Failure')
        if from_to_table['lims_ius_swid'].duplicated().any():
            raise Exception('Failure')

        return cls(from_table, to_table)

    def summarize(self, out_dir):
        self._log.info('No summary')

    def to_sql(self, out_dir):
        current_lims_keys = self.from_table
        new_lims_keys = self.to_table

        if not current_lims_keys.empty and not new_lims_keys.empty:
            sql = ''
            sql += 'BEGIN;\n\n'
            sql += (pd.io.sql.get_schema(current_lims_keys,
                                         dtype={'last_modified': TIMESTAMP(timezone=True)},
                                         name='current_lims_keys') + ';\n\n').replace('TABLE', 'TEMP TABLE')
            sql += 'INSERT INTO "{}" ({}) VALUES\n'.format('current_lims_keys',
                                                           ','.join(current_lims_keys.columns.values))

            values = []
            for i, record in current_lims_keys.iterrows():
                values.append('({})'.format(','.join(['\'{}\''.format(w) for w in record])))
            sql += (',\n'.join(values)) + ';\n\n'

            sql += (pd.io.sql.get_schema(new_lims_keys,
                                         dtype={'last_modified': TIMESTAMP(timezone=True)},
                                         name='new_lims_keys') + ';\n\n').replace('TABLE', 'TEMP TABLE')
            sql += 'INSERT INTO "{}" ({}) VALUES\n'.format('new_lims_keys', ','.join(new_lims_keys.columns.values))
            values = []
            for i, record in new_lims_keys.iterrows():
                values.append('({})'.format(','.join(['\'{}\''.format(w) for w in record])))
            sql += (',\n'.join(values)) + ';\n\n'

            sql += """WITH tmp AS (
            UPDATE lims_key lk 
            SET provider = nlk.lims_provider, 
            id = nlk.lims_id,
            version = nlk.lims_version,
            last_modified = nlk.lims_last_modified,
            update_tstmp = now()
            FROM ius i, current_lims_keys clk, new_lims_keys nlk 
            WHERE i.lims_key_id = lk.lims_key_id AND 
            i.sw_accession = clk.lims_ius_swid AND 
            lk.provider = clk.lims_provider AND 
            lk.id = clk.lims_id AND 
            lk.version = clk.lims_version AND 
            lk.last_modified = clk.lims_last_modified AND 
            i.sw_accession = nlk.lims_ius_swid 
            RETURNING lk.*) 
            SELECT * INTO temporary table updated_lims_keys FROM tmp;\n\n"""

            with get_file_path(out_dir, 'update_lims_keys.sql').open('w') as out:
                out.write(sql)
        else:
            self._log.warning('Nothing to do')
