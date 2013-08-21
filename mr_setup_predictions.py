#!/usr/bin/python2.6
import config
import mr_predictions
import mr_setup
import os.path
import sys

_LOCAL_DATA_DIR = config._TMP_DIR + '/wangy/data/'

def file_copier(extra_arg_list):
  assert len(extra_arg_list) == 4
  learned_nn_file_path, user_n_factor_file_path, item_n_factor_file_path, reduction_node = extra_arg_list
  os.system('mkdir -p ' + _LOCAL_DATA_DIR)
  os.system('cp ' + learned_nn_file_path + ' ' + _LOCAL_DATA_DIR +
      os.path.basename(learned_nn_file_path))
  extra_arg_list[0] = _LOCAL_DATA_DIR + os.path.basename(learned_nn_file_path)

  os.system('cp ' + user_n_factor_file_path + ' ' + _LOCAL_DATA_DIR +
      os.path.basename(user_n_factor_file_path))
  extra_arg_list[1] = _LOCAL_DATA_DIR + os.path.basename(user_n_factor_file_path)

  os.system('cp ' + item_n_factor_file_path+ ' ' + _LOCAL_DATA_DIR +
      os.path.basename(item_n_factor_file_path))
  extra_arg_list[2] = _LOCAL_DATA_DIR + os.path.basename(item_n_factor_file_path)


def main(argv):
  start = int(argv[1])
  end = int(argv[2])
  sync_dir = argv[3]
  mr_mode = argv[4]
  num_nodes = int(argv[9])
  node_list = argv[10: (10 + num_nodes)] 
  extra_arg_list = argv[10 + num_nodes:]
  # Replace the extra_arg_list.
  file_copier(extra_arg_list)
  if mr_mode == 'True':
    mr_obj = mr_predictions.MRPredictions(int(argv[5]),
        int(argv[6]), argv[7], argv[8], extra_arg_list)
    setup_obj = mr_setup.MRSetup(mr_obj, node_list, start, end, sync_dir)
    print 'mr_obj for MR is ', mr_obj.__dict__, '. The environment is ', setup_obj.__dict__
    setup_obj.map_only()
  else:
    mr_obj = mr_predictions.MRPredictions(1, 2,
        argv[7], argv[8], extra_arg_list)
    print 'mr_obj for single_process is ', mr_obj.__dict__
    mr_setup.MRSetup(mr_obj, node_list, start, end, sync_dir).single_process()


if __name__ == '__main__':
  print sys.argv
  mr_mode = sys.argv[4] 
  assert (mr_mode == 'True' or mr_mode == 'False')
  # start, end, sync_dir, mr_mode, num_processes, num_input_files, input_file_dir, output_file_dir,
  # num_nodes, (node_list)- - -, extra_arg_list
  main(sys.argv)
