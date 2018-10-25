import pandas as pd

from models.context import BaseContext
from operations.analyze_changes.reports.change_summary import workflow_run_report
from utils.file import get_file_path


class AnnotateRecords(BaseContext):
    def __init__(self, fpr, workflow_run_swids, tags):
        self.fpr = fpr  # type: pd.DataFrame
        self.workflow_run_swids = workflow_run_swids  # type: pd.Series
        self.tags = tags  # type: dict

    def summarize(self, out_dir):
        workflow_run_report(self.fpr, self.workflow_run_swids, out_dir + 'invalid_workflow_runs.csv')

    def to_sql(self, out_dir):
        if not self.workflow_run_swids.empty and self.tags:
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
        else:
            self._log.warn('Nothing to do')
