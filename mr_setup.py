#!/usr/bin/python
'''author: Ye Wang'''
'''The MR running environment for MR jobs.

   It sets up the environment for running the MR jobs on a single node,
   including the node_list, the m/r func and the files node needs to process.'''
import config

import commands
from multiprocessing import Pool
import os
import os.path
import time


# The *_on_file functions wrap the class methods inside a function.
# This tweak is because we cannot pass the bounded class method directly
# to apply_async call.
def _map_on_file(mr_obj, file_name):
  t1 = time.time()
  mr_obj.map_on_file(file_name)
  t2 = time.time()
  print 'map on %s took %f ms.\n' % (file_name, (t2 - t1) * 1000)


def _shuffle_on_file(mr_obj, thread_id):
  mr_obj.shuffle_on_file(thread_id)


def _reduce_on_file(mr_obj, thread_id):
  mr_obj.reduce_on_file(thread_id)


class MRSetup(object):
  '''The Basic mapreduce framework. User supplies map_func and reduce_func.
  '''
  def __init__(self,
      mr_obj,
      node_list,
      start_file_id,
      end_file_id,
      sync_dir):
    self.mr_obj = mr_obj
    self.node_list = node_list
    self.num_nodes = len(self.node_list)
    self.start = start_file_id
    self.end = end_file_id
    self.sync_dir = sync_dir

    # Pass the parameters for MR jobs to the MRSetup.
    self.num_processes = mr_obj.num_processes
    self.num_input_files = mr_obj.num_input_files
    self.input_file_dir = mr_obj.input_file_dir
    self.output_file_dir = mr_obj.output_file_dir
    # These parameters must be consistent with the mr_base.py.
    self._TEMP_FILE_DIR = config._TMP_DIR + '/temp_dir/'

  def map_only(self):
    # Make sure the directories actually exist before the program.
    os.system('mkdir -p ' + self.input_file_dir)
    os.system('mkdir -p ' + self.output_file_dir)
    os.system('mkdir -p ' + self._TEMP_FILE_DIR)
    # map phase
    num_processes_per_node = self.num_processes / self.num_nodes
    print 'num_processes_per_node = ', num_processes_per_node
    print 'Available processes for map is %d.' % num_processes_per_node
    pool = Pool(processes = num_processes_per_node)
    for i in range(self.start, self.end + 1):
      file_name = '%05d' % i
      pool.apply_async(_map_on_file, (self.mr_obj, file_name,))
    pool.close()
    pool.join()

  def mr(self):
    # Make sure the directories actually exist before the program.
    clean_up = True
    os.system('mkdir -p ' + self.input_file_dir)
    os.system('mkdir -p ' + self.output_file_dir)
    os.system('mkdir -p ' + self._TEMP_FILE_DIR)
    # map phase
    num_processes_per_node = self.num_processes / self.num_nodes
    print 'Available processes for map is %d.' % num_processes_per_node
    pool = Pool(processes = num_processes_per_node)
    for i in range(self.start, self.end + 1):
      file_name = '%05d' % i
      pool.apply_async(_map_on_file, (self.mr_obj, file_name,))
    pool.close()
    pool.join()

    my_host_name = commands.getoutput('hostname').split('.')[0]
    print 'My host name is ', my_host_name
    # Need to sync among different nodes with multiple nodes.
    if self.num_nodes > 1:
      # Write the sync file.

      # Send the data to the node responsible for it.
      assert self.num_processes % self.num_nodes == 0  
      # Each node is in chare of _NUM_PROCESSES / _NUM_NODES files.
      num_processes_per_node = self.num_processes / self.num_nodes
      for index, host_name in enumerate(self.node_list):
        if host_name not in my_host_name:
          arg_file_list = []
          for thread_id in range(num_processes_per_node * index,
              num_processes_per_node * (index+1)):
            for shard_id in range(self.start, self.end + 1):
              arg_file_list.append(str(shard_id) + '.' + str(thread_id) + '.map ')
          # Tar the file without directory structure.
          tar_file_name = my_host_name + '__' + host_name + '.tar'
          status = os.system ('tar -cf ' +
              self._TEMP_FILE_DIR + os.sep + tar_file_name + ' ' +
              '--directory=' + self._TEMP_FILE_DIR + ' ' +
              ' '.join(arg_file_list))
          print 'tar status is ', status

          status = os.system ('scp ' + self._TEMP_FILE_DIR + os.sep +
              tar_file_name + ' ' + host_name + ':' + self._TEMP_FILE_DIR)
          print 'scp status is ', status
          while status != 0:
            status = os.system ('scp ' + self._TEMP_FILE_DIR + os.sep +
                tar_file_name + ' ' + host_name + ':' + self._TEMP_FILE_DIR)
            print 'scp status is ', status
      # Write done file.
      time.sleep(10)
      os.system('touch ' + self.sync_dir + os.sep + str(my_host_name) + '.mapdone')
      # Wait until all files finishes.
      while True:
        time.sleep(10)
        counter = 0
        sync_file_list = os.listdir(self.sync_dir)
        for sync_file in sync_file_list:
          if sync_file.endswith('.mapdone'):
            counter += 1
        if counter == len(self.node_list):
          print 'Finish syncing before merge! Sleep for another 5 seconds!'
          time.sleep(5)
          break

      # Untar the files sent by other processes.
      print 'Starting to untar the data.'
      for host_name in self.node_list:
        if host_name not in my_host_name:
          tar_file_name = host_name + '__' + my_host_name + '.tar'
          os.system('tar -xf ' + self._TEMP_FILE_DIR + os.sep +
              tar_file_name + ' ' + '--directory=' + self._TEMP_FILE_DIR)

    # Merge task assignment.
    thread_range = (0, 0)
    num_processes_per_node = self.num_processes / self.num_nodes
    for index, host_name in enumerate(self.node_list):
      if host_name in my_host_name:
        thread_range = (num_processes_per_node * index,
            num_processes_per_node * (index + 1) - 1)

    print 'thread_range = %s' % str(thread_range)
    if clean_up:
      os.system('rm ' + self._TEMP_FILE_DIR + '*.tar')

    # merge phase
    pool = Pool(processes = thread_range[1] - thread_range[0] + 1)
    for i in range(thread_range[0], thread_range[1] + 1):
      file_name = i
      pool.apply_async(_shuffle_on_file, (self.mr_obj, file_name,))
    pool.close()
    pool.join()
    if clean_up:
      os.system('rm ' + self._TEMP_FILE_DIR + os.sep + '*.map')

    # reduce phase
    pool = Pool(processes= thread_range[1] - thread_range[0] + 1)
    for i in range(thread_range[0], thread_range[1] + 1):
      file_name = i
      pool.apply_async(_reduce_on_file, (self.mr_obj, file_name,))
    pool.close()
    pool.join()

    # Clean up!
    if clean_up:
      os.system('rm ' + self._TEMP_FILE_DIR + os.sep + '*.merge')


  def single_process(self):
    """Only work on a single process."""
    assert self.mr_obj.num_processes == 1, (
        'For single process mode, only use 1 node and 1 process.')
    # Pre-mapreduce
    t1 = time.time()
    self.mr_obj.pre_mr()
    t2 = time.time()
    print 'Single process pre_mr took %f ms.\n' % ((t2 - t1) * 1000)

    for i in range(self.mr_obj.num_input_files):
      file_name = '%05d' % i
      _map_on_file(self.mr_obj, file_name)

    for i in range(self.mr_obj.num_processes):
      file_name = i
      _shuffle_on_file(self.mr_obj, file_name)

    for i in range(self.mr_obj.num_processes):
      file_name = i
      _reduce_on_file(self.mr_obj, file_name)


  def single_process_map_only(self):
    """Only work on a single process."""
    assert self.mr_obj.num_processes == 1, (
        'For single process mode, only use 1 node and 1 process.')
    # Pre-mapreduce
    t1 = time.time()
    self.mr_obj.pre_mr()
    t2 = time.time()
    print 'Single process pre_mr took %f ms.\n' % ((t2 - t1) * 1000)

    for i in range(self.mr_obj.num_input_files):
      file_name = '%05d' % i
      _map_on_file(self.mr_obj, file_name)


if __name__ == '__main__':
  pass
