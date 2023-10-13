import lzma
import dill as pickle

def load_pickle(path):
    
        with lzma.open(path, 'rb') as f:
            file = pickle.load(f)
        return file

def save_pickle(path, obj):
    with lzma.open(path, "wb") as f:
        pickle.dump([obj], f)
