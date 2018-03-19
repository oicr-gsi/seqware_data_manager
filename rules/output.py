def study_name_change(fpr, changes, study="", workflow="", expected_path=""):

    allowed_field_changes = ['Study Title']

    okay_to_update = fpr.loc[(fpr['studyTitle'] == study) &
                             (fpr['Workflow Name'] == workflow),
                             'File Path'].fillna('').str.contains(expected_path)

    return changes.index.isin(okay_to_update.index) & changes['field'].isin(allowed_field_changes)