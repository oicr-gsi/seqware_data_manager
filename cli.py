import defopt

import tasks.change_analysis_lims_provider.cli
import tasks.load_analysis_lims_data.cli

if __name__ == '__main__':
    defopt.run([tasks.load_analysis_lims_data.cli.load_analysis_lims_data,
                tasks.change_analysis_lims_provider.cli.change_analysis_lims_provider], short={}, strict_kwonly=False)
