import pandas as pd
from sqlalchemy import TIMESTAMP


def generate_workflow_run_annotations(workflow_run_swids, tags):
    script = ''
    script += 'BEGIN;\n\n'

    script += 'CREATE TEMP TABLE "new_workflow_run_attributes" ("workflow_run_swid" INTEGER, "tag" TEXT, "value" TEXT);\n\n'
    script += 'INSERT INTO "new_workflow_run_attributes" (workflow_run_swid,tag,value) VALUES\n'
    values = []
    for workflow_run_swid in workflow_run_swids:
        for tag, value in tags.items():
            values.append('(\'{}\',\'{}\',\'{}\')'.format(workflow_run_swid, tag, value))
    script += (',\n'.join(values)) + ';\n\n'

    script += """WITH tmp AS (
INSERT INTO workflow_run_attribute (workflow_run_id,tag,value) (
SELECT wr.workflow_run_id, nwra.tag, nwra.value
FROM new_workflow_run_attributes nwra 
LEFT JOIN workflow_run wr ON nwra.workflow_run_swid = wr.sw_accession )
RETURNING *
) SELECT * INTO temporary table inserted_workflow_run_attribute FROM tmp;\n\n"""

    return script


def generate_lims_key_updates(current_lims_keys, new_lims_keys):
    script = ''
    script += 'BEGIN;\n\n'
    script += (pd.io.sql.get_schema(current_lims_keys,
                                    dtype={'last_modified': TIMESTAMP(timezone=True)},
                                    name='current_lims_keys') + ';\n\n').replace('TABLE', 'TEMP TABLE')
    script += 'INSERT INTO "{}" ({}) VALUES\n'.format('current_lims_keys', ','.join(current_lims_keys.columns.values))

    values = []
    for i, record in current_lims_keys.iterrows():
        values.append('({})'.format(','.join(['\'{}\''.format(w) for w in record])))
    script += (',\n'.join(values)) + ';\n\n'

    script += (pd.io.sql.get_schema(new_lims_keys,
                                    dtype={'last_modified': TIMESTAMP(timezone=True)},
                                    name='new_lims_keys') + ';\n\n').replace('TABLE', 'TEMP TABLE')
    script += 'INSERT INTO "{}" ({}) VALUES\n'.format('new_lims_keys', ','.join(new_lims_keys.columns.values))
    values = []
    for i, record in new_lims_keys.iterrows():
        values.append('({})'.format(','.join(['\'{}\''.format(w) for w in record])))
    script += (',\n'.join(values)) + ';\n\n'

    script += """WITH tmp AS (
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

    return script
