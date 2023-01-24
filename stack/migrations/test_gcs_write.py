from cloudpathlib import AnyPath
from cpg_utils.config import get_config
from cpg_utils.hail_batch import dataset_path

config = get_config()

def write():
    try:
        with AnyPath(dataset_path('mfranklin-infra-test/file.txt')).open('w+') as f:
            f.write('Hello, world!')

        return True
    except:
        return False

def read():
    try:
        with AnyPath(dataset_path('mfranklin-infra-test/file.txt')).open() as f:
            contents = f.readline()
            print(f'Received contents: {contents!r}')
        return True
    except:
        return False


if __name__ == '__main__':
    functions = [('write', write), ('read', read)]
    for optype, function in functions:
        success = 'Can' if function() else 'Cannot'
        print(f'{success} {optype} from main')
