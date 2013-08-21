#!/usr/bin/python
'''author: Ye Wang
The base file for MR.'''
import config
import os
import os.path
import sys

class MapReduceBase(object):
  '''The Basic mapreduce framework. User supplies map_func and reduce_func.
  '''
  def __init__(self, num_processes, num_input_files, input_file_dir, output_file_dir):
    self.num_processes = num_processes
    self.num_input_files = num_input_files
    self.input_file_dir = input_file_dir
    self.output_file_dir = output_file_dir

    # Constants definition.
    self._HOME_DIR = config._BASE_DIR
    self._TEMP_FILE_DIR = config._TMP_DIR + '/temp_dir/'
    self._MAP_SUFFIX = '.map'
    self._MERGE_SUFFIX = '.merge'
    self._REDUCE_SUFFIX = '.red'
    self._SEPERATOR = '#:-:#'

    # Create the directories if they do not exist.
    os.system('mkdir -p ' + self._TEMP_FILE_DIR)
    os.system('mkdir -p ' + self.output_file_dir)

  def get_input_path(self, input_dir, input_name):
    input_base_name = int(input_name)
    p1 = os.path.join(input_dir, str(input_base_name))
    p2 = os.path.join(input_dir, '%05d' % input_base_name)
    if p1 == p2:
      return p2
    elif not os.path.exists(p1) and os.path.exists(p2):
      return p2
    elif os.path.exists(p1) and not os.path.exists(p2):
      return p1
    else:
      assert False, 'p1 = %s, p2 = %s.' % (p1, p2)

  def pre_mr(self):
    """This will be run only once before any MR job starts."""
    pass

  def _start(self, partition_id):
    pass

  def map_on_file(self, partition_id):
    '''Operate on file with the name partition_id.
    If the partition_id = "001" and _TEMP_FILE_DIR = '~/home'
    and there are 8 processes, then the intermediate output files
    could be ~/home/1.0.map, ~/home/1.1.map,
    ..., ~/home/1.7.map
    Two formats are accepted for the partition_id. It could be integers
    or integer padded by 0 at the beginning. So both 3, and 00003 can be
    accepted.
    All the partition_id s is padded by 0 at the beginning to form
    5 digits, i.e., 00001, 00007, 00128, 00013. We remove the left 0s in
    the mapper.
    '''
    self._start(partition_id)
    self.input_partition_id = partition_id
    print 'In map, processing ', partition_id
    sys.stdout.flush()
    out_fp_list = []
    for i in range(self.num_processes):
      id_after_removal = str(partition_id).lstrip('0')
      if not id_after_removal:  # 000 case
        id_after_removal = '0'
      tmp_name = id_after_removal +'.'+ str(i) + self._MAP_SUFFIX
      f = open(self._TEMP_FILE_DIR + os.sep + tmp_name, 'w')
      out_fp_list.append(f)

    input_file = open(self.input_file_dir + os.sep + partition_id)
    # User code starts here.
    for line in input_file:
      line = line.strip()  # Remove the '\n' in the line.
      kv_list = self.map_func(line)
      for key, val in kv_list:
        output_str = '%s%s%s\n' % (key, self._SEPERATOR, val)
        pkey = self.hash_func(key)
        out_fp_list[pkey].write(output_str)
    # User code ends here.
    input_file.close()
    for fp in out_fp_list:
      fp.close()
    print 'Finshed mapping ', partition_id
    sys.stdout.flush()

  def hash_func(self, key):
    '''Return a integer smaller than self.num_processes.'''
    return hash(key)%self.num_processes


  def sort_merge_file(self, merge_file):
    # Sort the data in the merge_file. Maybe out-of-core.
    os.system('LC_ALL=C sort ' + ' --output=' + merge_file + '.tmp' + ' ' + merge_file)
    os.system('mv ' + merge_file + '.tmp ' + merge_file)


  def shuffle_on_file(self, thread_id):
    '''Merge the intermediate files having the same id, and sort the data.'''
    print 'In merge, merging ', thread_id
    merge_file = self._TEMP_FILE_DIR + os.sep + str(thread_id) + self._MERGE_SUFFIX

    if os.path.exists(merge_file):  # Remove the existing files.
      os.remove(merge_file)
    os.system('touch ' + merge_file)

    # Concatnate all files ending with .{thread_id}.map.
    for i in range(self.num_input_files):
      map_file_name = str(i) + '.' + str(thread_id) + self._MAP_SUFFIX
      map_file_path = self._TEMP_FILE_DIR + os.sep + map_file_name
      os.system('cat ' + map_file_path + ' >>' + merge_file)
    # Sometimes, we do not need to sort. This decision is left to the actual
    # implementation.
    self.sort_merge_file(merge_file)
    print 'Finshed merging ', thread_id
    sys.stdout.flush()

  def reduce_on_file(self, thread_id):
    '''Reduce func.'''
    print 'In Reduce ', thread_id
    sys.stdout.flush()
    reduce_file = self.output_file_dir + os.sep + str(thread_id) + self._REDUCE_SUFFIX
    out_f = open(reduce_file, 'w')
    merge_file = self._TEMP_FILE_DIR + os.sep + str(thread_id) + self._MERGE_SUFFIX
    input_f = open(merge_file)
    # User code. The file contains reduce tuples from multiple keys.
    # The key/value pairs are sorted.
    self.reduce_func(input_f, out_f)
    # User codes end here.
    print 'Finished Reduce ' + str(thread_id)
    sys.stdout.flush()
    input_f.close()
    out_f.close()

  def map_func(self, line):
    '''Process the line and return a list of (key, val) pairs.
       It must be implemented.'''
    assert False

  def reduce_func(self, input_f, output_f):
    '''Read the input, and write the output_f or somewhere else.
       It must be implemented.'''
    assert False


if __name__ == '__main__':
  assert (sys.argv[1] == 'True' or sys.argv[1] == 'False')
  run_mr_mode = sys.argv[1]
  pass
