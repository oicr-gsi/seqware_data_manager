import pandas as pd


def generate_change_summary_report(fpr, changes, file_path):
    changes_str = (
            changes['field'].astype(str) + ':' +
            '[' + changes['from'].astype(str) + ']->' +
            '[' + changes['to'].astype(str) + ']'
    ).groupby('index').apply(lambda x: '[{}]'.format(','.join(x)))

    records_with_changes = fpr.loc[fpr.index.isin(changes.index)]
    report = pd.concat([
        records_with_changes['Workflow Run SWID'].fillna(0).astype(int),
        (records_with_changes['Workflow Name'] + '-' + records_with_changes['Workflow Version']).rename('Workflow'),
        records_with_changes[['Lane Name', 'Sample Name']],
        changes_str.rename('Change Summary')
    ], axis=1).drop_duplicates()
    report.sort_values(['Workflow Run SWID', 'Lane Name', 'Sample Name'], inplace=True)
    report.to_csv(file_path, index=False)


def workflow_run_report(fpr, workflow_run_swids, file_path):
    report_records = fpr.loc[fpr['Workflow Run SWID'].isin(workflow_run_swids)]
    report_records = pd.concat([report_records['Workflow Run SWID'].fillna(0).astype(int),
                                (report_records['Workflow Name'] + '-' + report_records['Workflow Version'])
                               .rename('Workflow'),
                                report_records[['Lane Name', 'Sample Name']]
                                ], axis=1).drop_duplicates()
    report_records.sort_values(['Workflow Run SWID', 'Lane Name', 'Sample Name'], inplace=True)
    report_records.to_csv(file_path, index=False)
