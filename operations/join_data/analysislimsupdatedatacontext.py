import collections
import json
from enum import Enum

import pandas as pd

import utils.analysis
from models.context import BaseContext
from utils.analysis import get_fp_with_lims_provenance, generate_workflow_run_hierarchy


class AnalysisLimsUpdateDataContext(BaseContext):
    class FilterOperation(Enum):
        INCLUDE = 'include'
        EXCLUDE = 'exclude'

    def __init__(self, fpr, fp_to_provenance_map, hierarchy):
        self.fpr = fpr
        self.fp_to_provenance_map = fp_to_provenance_map
        self.hierarchy = hierarchy

    @classmethod
    def load_from_files(cls, lane_provenance_path, sample_provenance_path, provider, file_provenance_path):
        fp_to_provenance_map = utils.analysis.default_fp_to_provenance_map_str
        if isinstance(fp_to_provenance_map, str):
            fp_to_provenance_map = json.loads(fp_to_provenance_map, object_pairs_hook=collections.OrderedDict)

        fpr_with_lims_provenance = get_fp_with_lims_provenance(lane_provenance_path,
                                                               sample_provenance_path,
                                                               provider,
                                                               file_provenance_path)

        wfr_hierarchy, _ = generate_workflow_run_hierarchy(fpr_with_lims_provenance)
        return cls(fpr_with_lims_provenance, fp_to_provenance_map, wfr_hierarchy)

    def filter(self, operation: FilterOperation, filters: dict):
        self._log.info('Applying {} filters: {}'.format(operation.value, filters))

        if not filters:
            return self

        select_mask = pd.Series(True, index=self.fpr.index)
        for key, values in filters.items():
            if isinstance(values, str):
                values = values.split(',')
            elif isinstance(values, list):
                values = values
            else:
                raise ValueError(f'Unsupported filter value type = {type(values)}')

            key_mask = False
            for value in values:
                key_mask = key_mask | (self.fpr[key] == value)
            select_mask = select_mask & key_mask

        if operation == AnalysisLimsUpdateDataContext.FilterOperation.INCLUDE:
            filtered_fpr = self.fpr[select_mask]
        elif operation == AnalysisLimsUpdateDataContext.FilterOperation.EXCLUDE:
            filtered_fpr = self.fpr[~select_mask]
        else:
            raise ValueError(f'Unsupported operation: {operation}')

        return self.__class__(filtered_fpr, self.fp_to_provenance_map, self.hierarchy)

    def apply_include_filters(self, filters: dict):
        return self.filter(AnalysisLimsUpdateDataContext.FilterOperation.INCLUDE, filters)

    def apply_exclude_filters(self, filters: dict):
        return self.filter(AnalysisLimsUpdateDataContext.FilterOperation.EXCLUDE, filters)
