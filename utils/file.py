from pathlib import Path


def getpath(file_path_string):
    path = Path(file_path_string)
    path = path.expanduser()
    return path


def get_file_path(dir_path, file_string):
    return Path(str(getpath(dir_path)) + '/' + file_string)