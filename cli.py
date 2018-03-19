import defopt


def update(fpr_path, sp_path, lp_path):
    """
    Generate updates

    :param str fpr_path: file provenance path
    :param str sp_path:  sample provenance path
    :param str lp_path: lane provenance path
    """
    from context import config
    config.fpr_path=fpr_path
    config.sp_path=sp_path
    config.lp_path=lp_path

    import tasks.migrate_analysis_to_new_lims_provider


if __name__ == '__main__':
    defopt.run([update], short={}, strict_kwonly=False)
