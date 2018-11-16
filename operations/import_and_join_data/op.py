from enum import Enum

import pandas as pd

from models.context import BaseContext
from utils.analysis import get_fp_with_lims_provenance, generate_workflow_run_hierarchy


class JoinedData(BaseContext):
    class FilterOperation(Enum):
        INCLUDE = 'include'
        EXCLUDE = 'exclude'

    def __init__(self, fpr, file_to_lims_provenance_map, hierarchy):
        self.fpr = fpr
        self.file_to_lims_provenance_map = file_to_lims_provenance_map
        self.hierarchy = hierarchy

    @classmethod
    def load_from_files(cls, lane_provenance_url, sample_provenance_url, provider_id, file_provenance_url, id_map_url=None):
        fpr_with_lims_provenance, file_to_lims_provenance_map = get_fp_with_lims_provenance(lane_provenance_url,
                                                                                            sample_provenance_url,
                                                                                            provider_id,
                                                                                            file_provenance_url,
                                                                                            id_map_url=id_map_url)
        wfr_hierarchy, _ = generate_workflow_run_hierarchy(fpr_with_lims_provenance)
        return cls(fpr_with_lims_provenance, file_to_lims_provenance_map, wfr_hierarchy)

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

        if operation == JoinedData.FilterOperation.INCLUDE:
            filtered_fpr = self.fpr[select_mask]
        elif operation == JoinedData.FilterOperation.EXCLUDE:
            filtered_fpr = self.fpr[~select_mask]
        else:
            raise ValueError(f'Unsupported operation: {operation}')

        return self.__class__(filtered_fpr, self.file_to_lims_provenance_map, self.hierarchy)

    def apply_include_filters(self, filters: dict):
        return self.filter(JoinedData.FilterOperation.INCLUDE, filters)

    def apply_exclude_filters(self, filters: dict):
        return self.filter(JoinedData.FilterOperation.EXCLUDE, filters)
