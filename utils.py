
from pathlib import Path


def find_absolute_path(file_name):
    directory_path = Path('./dataset')
    for file_path in directory_path.glob('**/' + file_name):
        return file_path.absolute().as_posix()

if __name__ == '__main__':
    file_path = find_absolute_path('u.data')
    print(file_path)
