import pandas as pd

import utils
from context.analysislimsupdatedatacontext import AnalysisLimsUpdateDataContext
from context.basecontext import BaseContext


class ChangeContext(BaseContext):

    def __init__(self, ctx: AnalysisLimsUpdateDataContext, changes):
        self._ctx = ctx
        self.changes = changes

    @classmethod
    def generate_changes(cls, ctx: AnalysisLimsUpdateDataContext):
        fp_to_provenance_map = ctx.fp_to_provenance_map

        changes = pd.DataFrame(columns=['field', 'from', 'to'])
        for from_field, to_field in fp_to_provenance_map.items():
            cls._log.debug('Comparing {} to {}'.format(from_field, to_field))
            changes = changes.append(utils.pandas.generate_changes(ctx.fpr, from_field, to_field))

        # convert to categorical to reduce size
        changes['field'] = changes['field'].astype('category')
        changes['from'] = changes['from'].apply(utils.transformations.convert_to_string).astype('category')
        changes['to'] = changes['to'].apply(utils.transformations.convert_to_string).astype('category')

        # include additional fields required when filtering of changes
        additional_fields = ctx.fpr.loc[:, ['Workflow Name', 'LIMS Provider', 'Last Modified']]
        additional_fields.columns = ['workflow', 'provider', 'processing_date']
        additional_fields['workflow'] = additional_fields['workflow'].astype('category')
        additional_fields['provider'] = additional_fields['provider'].astype('category')
        additional_fields['processing_date'] = pd.to_datetime(additional_fields['processing_date'])
        changes = changes.reset_index().merge(additional_fields.reset_index(), how='left', on='index').set_index(
            'index')

        return cls(ctx, changes)

    @property
    def fpr(self):
        return self._ctx.fpr

    @property
    def hierarchy(self):
        return self._ctx.hierarchy
