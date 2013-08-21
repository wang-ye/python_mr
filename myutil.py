"""Author: Ye Wang. All rights reserved.
   The utility functions for MR related jobs.
"""
import commands
import datetime
import config
import os
import os.path
import subprocess
import time

_FILE_LOCK = config._TMP_DIR + '/file.lock'

def _get_node_list_oakley(num_required_nodes):
  # On oakley, each job should allocate one node. We use multi-threading.
  # available_cores = commands.getoutput('cat $PBS_NODEFILE').split('\n')
  my_host_name = commands.getoutput('hostname').split('.')[0]
  return [my_host_name]


def _get_node_list_ri(num_required_nodes):
  """Return the list of nodes that is available to wangy."""
  allocations = commands.getoutput('squeue -l | grep wangy').split('\n')
  # Now find an allocation for our program. The problem is file locking in the
  # NSF system. We use mkdir on the local head node to ensure the correct
  # locking behaviors.
  # for allocation in allocations:
  print 'Try to lock the files.'
  locking_command = 'ssh head "mkdir ' + _FILE_LOCK + ' "'
  status = os.system(locking_command)
  print 'Lock status is %s (0 means get the lock).' % status
  max_trial = 4
  trial_time = 0
  while status != 0:  # Someone else is doing the allocation.
    time.sleep(3)
    status = os.system(locking_command)
    trial_time += 1
    if (trial_time >= max_trial):
      assert False, 'Cannot get the file lock, exit.'

  # Sleep to avoid allocating to the used allocations in cocurrent case.
  time.sleep(3)

  # Now we have the lock.
  found_allocation = False
  node_list = []
  for allocation in allocations:
    # Parse the hostname.
    node_list = commands.getoutput('scontrol show hostnames ' +
       allocation.split()[-1]).split('\n')
    if len(node_list) < num_required_nodes:
      continue

    found_allocation = True
    error_list = []
    for node in node_list:  # Check whether this node has some processes from wangy.
      computing_commands = ['mpi', 'python', 'tee', 'cat', 'java']
      computing_commands_str = '\|'.join(computing_commands)
      # See whether there are commands computing on that node.
      process_info = commands.getoutput(
        'ssh ' + node + ' \'ps aux | grep wangy | grep "' +
        computing_commands_str + '" | grep -v grep\'')
      if process_info.strip():
        error_list.append(process_info)
        found_allocation = False
        break
    if found_allocation:
      break

  unlocking_command = 'ssh head "rmdir ' + _FILE_LOCK + ' "'
  status = os.system(unlocking_command)
  node_list = node_list[:num_required_nodes]  # Only return what you need.
  if error_list:
    print 'error_list is', error_list
  assert found_allocation and len(node_list), 'Cannot find valid allocation.'

  print 'node_list = %s.' % str(node_list)
  return node_list


def _get_system_name():
  host_name = commands.getoutput('hostname')
  if host_name.endswith('cluster'): # ri node
    return 'RI'
  elif host_name.endswith('osc.edu'):
    return 'OAKLEY'
  else:
    assert False, 'host_name is %s.\n' % host_name


def get_node_list(num_required_nodes):
  #'''Find the nodes we are allocating.'''
  system_name = _get_system_name()
  if system_name == 'RI':
    return _get_node_list_ri(num_required_nodes)
  elif system_name == 'OAKLEY':
    return _get_node_list_oakley(num_required_nodes)


def clean_file_locks():
  os.system('rmdir ' + _FILE_LOCK)


def create_new_file(path):
  """Create a new empty file in the given path."""
  d = os.path.dirname(path)
  if not os.path.exists(d):
    os.makedirs(d)
  # Now remove the old file in the path.
  f = open(path, 'w')
  f.close()


def _calc_range(num_input_files, num_nodes, node_id):
  min_files_per_node = num_input_files / num_nodes
  num_nodes_with_extra_file = num_input_files % num_nodes
  start = end = 0
  if node_id < num_nodes_with_extra_file:
    start = node_id * (min_files_per_node + 1)
    end = start + min_files_per_node
  else:
    start = num_nodes_with_extra_file * (min_files_per_node + 1) + (
      node_id - num_nodes_with_extra_file) * min_files_per_node
    end = start + min_files_per_node - 1
  return start, end


def mr_executor(mr_mode, setup_file, node_list, para_list):
  '''Distribute the work and run mr_code.'''
  _BASE_DIR = config._BASE_DIR
  system_name = _get_system_name()
  num_nodes = len(node_list)
  curr_time = datetime.datetime.now().strftime("%d-%H-%M-%S")
  # The sync directory in the NFS, shared by all nodes.
  sync_dir = _BASE_DIR + '/sync_dir/' + curr_time + '/'
  while os.path.exists(sync_dir):
    curr_time = datetime.datetime.now().strftime("%d-%H-%M-%S")
    sync_dir = _BASE_DIR + '/sync_dir/' + curr_time + '/'
  os.system('mkdir -p ' + sync_dir)

  para_list_str = ' '.join([str(para) for para in para_list])
  num_input_files = int(para_list[2])
  if system_name == 'OAKLEY':
    if mr_mode == 'True':
      proc_list = []
      for node_id in range(num_nodes):
        node_name = node_list[node_id]
        start, end = _calc_range(num_input_files, num_nodes, node_id)
        print str(['python ' + setup_file + ' ' +
            str(start) + ' ' + str(end) + ' ' + sync_dir + ' ' +
            para_list_str + ' | tee ' + config._TMP_DIR + '/tmpstatus.out'])
        os.system(' '.join([
            'python ' + setup_file + ' ' +
            str(start) + ' ' + str(end) + ' ' + sync_dir + ' ' +
            para_list_str + ' | tee ' + config._TMP_DIR + '/tmpstatus.out']))
    else:  # Single node mode. Only send data to a single node.
      print ('python ' +
          '-m cProfile ' + setup_file + ' ' + '0 1 ' + sync_dir + ' ' 
          + para_list_str + ' |  tee ' + config._TMP_DIR + '/tmpstatus.out')
      os.system(' '.join(['python ' +
          '-m cProfile ' + setup_file + ' ' + '0 1 ' + sync_dir + ' ' 
          + para_list_str + ' |  tee ' + config._TMP_DIR + '/tmpstatus.out']))
  elif system_name == 'RI':
    if mr_mode == 'True':
      proc_list = []
      for node_id in range(num_nodes):
        node_name = node_list[node_id]
        start, end = _calc_range(num_input_files, num_nodes, node_id)
        print str(['ssh', str(node_name), 'python ' + setup_file + ' ' +
            str(start) + ' ' + str(end) + ' ' + sync_dir + ' ' +
            para_list_str + ' | tee ' + config._TMP_DIR + '/tmpstatus.out'])

        proc = subprocess.Popen(['ssh', str(node_name),
            'python ' + setup_file + ' ' +
            str(start) + ' ' + str(end) + ' ' + sync_dir + ' ' +
            para_list_str + ' | tee ' + config._TMP_DIR + '/tmpstatus.out'])
        proc_list.append(proc)
      for proc in proc_list:
        proc.wait()
    else:  # Single node mode. Only send data to a single node.
      print ('ssh ' + node_list[0] + ' ' + '"' +  'python ' +
          '-m cProfile ' + setup_file + ' ' + '0 1 ' + sync_dir + ' ' + 
             para_list_str + ' | tee ' + config._TMP_DIR + '/tmpstatus.out')
      proc = subprocess.Popen(['ssh', node_list[0], 'python ' +
          '-m cProfile ' + setup_file + ' ' + '0 1 ' + sync_dir + ' ' +
            para_list_str + ' | tee ' + config._TMP_DIR + '/tmpstatus.out'])
      proc.wait()
  else:
    assert False, 'host_name is %s.\n' % system_name


def single_executor(cmd_str, node_name):
  system_name = _get_system_name()
  return_val = -1
  if system_name == 'OAKLEY':
    print 'Running ', cmd_str
    return_val = os.system(cmd_str)
  elif system_name == 'RI':
    print 'Running ', cmd_str, 'on node', node_name
    return_val = os.system('ssh ' + node_name + ' "' + cmd_str + '"')
  else:
    assert False, 'System name %s is invalid.' % system_name
  return return_val


def shard_renaming(num_output_files, output_file_dir):
  """Rename the output files from '123.red' to '00123'."""
  for shard_id in range(num_output_files):
    source_file = output_file_dir + os.sep + str(shard_id) + '.red'
    dest_file = '%s/%05d' % (output_file_dir, shard_id)
    if os.path.isfile(source_file) and os.path.getsize(source_file) != 0:
      os.system('mv ' + source_file + ' ' + dest_file)


if __name__ == '__main__':
  print get_node_list(1)
