import pickle
from pathlib import Path

import pandas as pd
from sqlalchemy import TIMESTAMP

from reports import generate_change_summary_report, workflow_run_report
from utils.common import *
from utils.file import getpath, get_file_path


class Context:
    def save(self, file_path):
        ctx_file = Path(file_path)
        with ctx_file.open("wb") as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(file_path):
        ctx_file = Path(file_path)
        with ctx_file.open("rb") as f:
            ctx = pickle.load(f)
            if isinstance(ctx, DataContext):
                return ctx
            else:
                raise Exception('{} is not expected type'.format(file_path))

    def save_to_hdf(self, file_path):
        store = pd.HDFStore(file_path, 'w')
        store.append('fpr', self.fpr)
        store.append('changes', self.changes)
        store.append('hierarchy', self.hierarchy)

    @staticmethod
    def load_from_hdf(file_path):
        store = pd.HDFStore(file_path, 'r')
        return DataContext(store['fpr'], store['changes'], store['hierarchy'])


class DataContext(Context):
    def __init__(self, fpr, changes, hierarchy):
        self.fpr = fpr
        self.changes = changes
        self.hierarchy = hierarchy


class ChangeContext(DataContext):
    def __init__(self, ctx, changes_allowed, changes_not_allowed):
        super().__init__(ctx.fpr, ctx.changes, ctx.hierarchy)
        self.changes_allowed = changes_allowed
        self.changes_not_allowed = changes_not_allowed

    def summarize(self, out_dir):
        generate_change_summary_report(self.fpr, self.changes_not_allowed,
                                       get_file_path(out_dir, 'changes_blocked_by_workflow_run.csv'))

        generate_change_summary_report(self.fpr, self.changes_allowed,
                                       get_file_path(config.out_dir, 'changes_allowed_by_workflow_run.csv'))


class UpdateLimsKeyContext:
    def __init__(self, from_table, to_table):
        # super().__init__(ctx.fpr, ctx.changes, ctx.hierarchy)
        self.from_table = from_table
        self.to_table = to_table

    def summarize(self, out_dir):
        log.info('No summary')

    def to_sql(self, out_dir):
        current_lims_keys = self.from_table
        new_lims_keys = self.from_table

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


class AnnotateWorkflowRunContext:
    def __init__(self, fpr, workflow_run_swids, tags):
        self.fpr = fpr
        self.workflow_run_swids = workflow_run_swids
        self.tags = tags

    def summarize(self, out_dir):
        workflow_run_report(self.fpr, self.workflow_run_swids, out_dir + 'invalid_workflow_runs.csv')

    def to_sql(self, out_dir):
        sql = ''
        sql += 'BEGIN;\n\n'

        sql += 'CREATE TEMP TABLE "new_workflow_run_attributes" ("workflow_run_swid" INTEGER, "tag" TEXT, "value" TEXT);\n\n'
        sql += 'INSERT INTO "new_workflow_run_attributes" (workflow_run_swid,tag,value) VALUES\n'
        values = []
        for workflow_run_swid in self.workflow_run_swids:
            for tag, value in self.tags.items():
                values.append('(\'{}\',\'{}\',\'{}\')'.format(workflow_run_swid, tag, value))
        sql += (',\n'.join(values)) + ';\n\n'

        sql += """WITH tmp AS (
        INSERT INTO workflow_run_attribute (workflow_run_id,tag,value) (
        SELECT wr.workflow_run_id, nwra.tag, nwra.value
        FROM new_workflow_run_attributes nwra 
        LEFT JOIN workflow_run wr ON nwra.workflow_run_swid = wr.sw_accession )
        RETURNING *
        ) SELECT * INTO temporary table inserted_workflow_run_attribute FROM tmp;\n\n"""

        with get_file_path(out_dir, 'update_workflow_run_annotations.sql').open('w') as out:
            out.write(sql)
