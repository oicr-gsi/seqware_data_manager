from context.basecontext import BaseContext
from utils.analysis import get_fp_with_lims_provenance, get_changes, generate_workflow_run_hierarchy
import pandas as pd

class AnalysisLimsUpdateDataContext(BaseContext):

    def __init__(self, fpr, changes, hierarchy):
        self.fpr = fpr
        self.changes = changes
        self.hierarchy = hierarchy

    @classmethod
    def load_from_files(cls, lane_provenance_path, sample_provenance_path, provider, file_provenance_path):
        fpr_with_lims_provenance = get_fp_with_lims_provenance(lane_provenance_path,
                                                               sample_provenance_path,
                                                               provider,
                                                               file_provenance_path)
        changes = get_changes(fpr_with_lims_provenance)
        wfr_hierarchy, _ = generate_workflow_run_hierarchy(fpr_with_lims_provenance)
        return cls(fpr_with_lims_provenance, changes, wfr_hierarchy)

    def filter(self, filters: dict):
        select_mask = pd.Series(True, index=self.fpr.index)
        for key, values in filters.items():
            key_mask = False
            # key = config.filter_config[k]
            for value in values.split(','):
                key_mask = key_mask | (self.fpr[key] == value)
                select_mask = select_mask & key_mask

        filtered_fpr = self.fpr[select_mask]
        filtered_changes = self.changes[self.changes.index.isin(self.fpr.index)]
        return self.__class__(filtered_fpr, filtered_changes, self.hierarchy)