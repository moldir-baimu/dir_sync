import os
import sys
import shutil
from hashlib import md5
from pathlib import Path
import logging
import sched, time
import traceback

args = sys.argv[1:]

source_path= args[0]
replica_path= args[1]
interval= args[2]
log_path= args[3]

list_to_remove = []
list_to_copy = []


def get_all_abs_paths(rootdir):

  paths = list()
  for dirpath, dirnames, filenames in os.walk(rootdir):
    for f in filenames:
      paths.append(os.path.abspath(os.path.join(dirpath, f)))
    for d in dirnames:
      paths.append(os.path.abspath(os.path.join(dirpath, d)))

  return paths


def transform_paths(abs_paths, type):
  new_paths = []

  for path in abs_paths:
    if len(path)>0:
      
      try:
        if type == 'source':
          path = path.split(source_path,1)[1]
        elif type == 'replica':
          path = path.split(replica_path,1)[1]
        else:
          print('not a correct type')
      except Exception as e:
        print(str(e))

      new_paths.append(path)
  return new_paths

  
def compare_files(list_a, list_b):

  cleaned_a = transform_paths(list_a, 'source')
  cleaned_b = transform_paths(list_b, 'replica')

  s = set(cleaned_a)
  common_files = s.intersection(cleaned_b)
  # print('common names: ' + str(common_files))

  only_a_files = [item for item in cleaned_a if item not in cleaned_b]
  # print('files only in A: ' + str(only_a_files) + '\n')
  only_b_files = [item for item in cleaned_b if item not in cleaned_a]
  # print('files only in B: ' + str(only_b_files) + '\n')

  return common_files, only_a_files, only_b_files


def remove_objects(remove_list):

  if len(remove_list) == 0:
    return

  for name in remove_list:
    is_file = os.path.isfile(replica_path + name)

    if is_file:
      try:
        os.remove(replica_path + name) 
        logging.info('file has been removed: ' +  replica_path + name)

      except Exception as e:
        print(str(e))
        print(traceback.format_exc())
        pass

    if not is_file and os.path.exists(replica_path + name):
      shutil.rmtree(replica_path + name)
      logging.info('directory has been removed: ' +  replica_path + name)
   

def copy_objects(copy_list):

  if len(copy_list) == 0:
    return

  for name in copy_list:
    
    is_file = os.path.isfile(source_path + name)

    containing_dir = (replica_path + name).rsplit('/',1)[0]
    os.makedirs(containing_dir, exist_ok=True)

    if is_file:
      try:
        shutil.copy(source_path + name, replica_path + name) 
        logging.info('File has been copied: ' +  replica_path + name)

      except Exception as e:
        print(str(e))
        print(traceback.format_exc())

    elif not is_file:
      path = Path(replica_path + name)
      path.mkdir(parents=True)
      logging.info('Directory has been created: ' +  replica_path + name)


def proper_check(names):

  for name in names:

    is_file = os.path.isfile(source_path + name)

    if is_file:
      with open(source_path + name, 'rb') as f1:
        digest1 = md5(f1.read()).hexdigest()

      with open(replica_path + name, 'rb') as f2:
        digest2 = md5(f2.read()).hexdigest()
      
      if digest1 != digest2:
        list_to_remove.append(replica_path + name)
        list_to_copy.append(source_path + name)
    else:
      pass


def sync_all(sc):

  source_files = get_all_abs_paths(source_path)
  # print('source files: \n' + str(source_files) + '\n')
  
  replica_files = get_all_abs_paths(replica_path)
  # print('replica files: \n' + str(replica_files) + '\n')

  common_names, source_only, replica_only = compare_files(source_files, replica_files)
  
  # replica-only - to delete
  # source_only - to copy
  # common - check hash first

  list_to_remove = replica_only
  list_to_copy = source_only
  proper_check(common_names) 

  remove_objects(list_to_remove)
  copy_objects(list_to_copy)

  if len(list_to_remove) == 0 and len(list_to_copy) == 0:
    logging.info('everything is up-to-date')

  sc.enter(3, 1, sync_all, (sc,))


def main():

  logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%d-%m-%y %H:%M:%S',
                    filename=log_path,
                    filemode='a')
  console = logging.StreamHandler()
  console.setLevel(logging.INFO)
  formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
  console.setFormatter(formatter)
  logging.getLogger('').addHandler(console)

  print("getting started \n")

  s = sched.scheduler(time.time, time.sleep)
  s.enter(0, 1, sync_all, (s,))
  s.run()

if __name__ == '__main__':
  main()



