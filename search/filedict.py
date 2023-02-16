import os, time
import multiprocessing
import pickle
import bz2


class FileDict(object):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, path):
        self.path = path
        if not os.path.exists(path):
            # create folder
            #mode = 0o666
            os.mkdir(self.path) #, mode)
        assert( os.path.exists(path) )

    def generate_filename(self, _key):
        return os.path.join(self.path, _key)

    def wait_semafore(self, _key):
        sem_fname = os.path.join(self.path,_key+'.sem')
        if not os.path.exists(sem_fname):
            open(sem_fname, 'w').write('1')
        else:
            wait_sleep = True
            sleep_count = 0
            while wait_sleep and sleep_count < 10:
                data = open(sem_fname).read().strip()
                if data != '' and int(data) == 0: # wait
                    open(sem_fname, 'w').write('1')
                    wait_sleep = False
                else: # sleep x time
                    #print('sleeping 0.1 seconds for key = %s ...' % _key)
                    time.sleep(0.02)
                    sleep_count += 1

    def free_semafore(self, _key):
        sem_fname = os.path.join(self.path,_key+'.sem')
        open(sem_fname, 'w').write('0')

    def __contains__(self, key):
        _key = self._keytransform(key)
        fname = self.generate_filename(_key)
        return os.path.exists(fname)

    def __getitem__(self, key):
        _key = self._keytransform(key)
        #self.wait_semafore(_key)
        fname = self.generate_filename(_key)
        try:
            #file = bz2.BZ2File(fname, 'rb')
            #obj = pickle.load(file)
            file = open(fname, 'rb')
            obj = pickle.load(file)
            file.close()
            #print('DEBUG FIND INDEX FILE!!!')
        except:
            return None
        #    self.free_semafore(_key)
        #    return set()
        #self.free_semafore(_key)
        return obj

    def __setitem__(self, key, value):
        _key = self._keytransform(key)
        #self.wait_semafore(_key)
        fname = self.generate_filename(_key)

        #with bz2.BZ2File(fname, 'w') as f: 
        #    pickle.dump(value, f)

        file = open(fname, 'wb')
        obj = pickle.dump(value, file)
        file.close()
        
        #self.free_semafore(_key)


    def __delitem__(self, key):
        _key = self._keytransform(key)
        #self.wait_semafore(_key)
        fname = self.generate_filename(_key)
        os.remove(fname)
        #self.free_semafore(_key)


    def __iter__(self):
        return iter(os.listdir(self.path))
    
    def __len__(self):
        return len(os.listdir(self.path))

    def _keytransform(self, key):
        return str(key)

    def extend_sets(self, ext_dic):
        for k in ext_dic:
            current_vals = self[k]
            if not current_vals:
                current_vals = set()
            current_vals.update( ext_dic[k] )
            #assert( len(current_vals) == l1 + len(vals) )
            self[k] = current_vals

    def update(self, tuple_list):
        for key, val in tuple_list:
            self[key] = val
