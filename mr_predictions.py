#!/usr/bin/python2.6
# Copyright 2012 Ye Wang. All Rights Reserved.
import config

import commands
import generate_rows
import io_util
import mr_base
import os
import sys
"""Code for creating the partial rows for every rating.
   Only map operation is needed.
   Note that partial rows is a misnormer. It should be full row...
"""
__author__ = 'wangy@cse.ohio-state.edu (Ye Wang)'

_SHARD_SIZE = 500000
_MAX_PARAMETERS = 100

def predict_nn_effect(partial_row, W):
  '''Compute the rating contributed by partial_row.
     W is a list of dicts, where each dict stores the parameter
     values of the corresponding parameter groups.
  '''
  rating = 0.0
  constants = partial_row[0][0]
  parameter_groups = partial_row[1:]
  for i, parameter_group in enumerate(parameter_groups):
    for t in parameter_group:
      col_index  = t[0]
      col_val = t[1]
      if col_index in W[i]:
        wij_val = W[i][col_index]
        rating += col_val * wij_val
  return rating + constants


def predict_factor_effect(partial_row, user_n_factor, item_n_factor):
  '''Only consider factor effect.'''
  rating = 0.0
  rating += partial_row[0][0]
  user = partial_row[0][1]
  item = partial_row[0][2]
  if (user in user_n_factor) and (item in item_n_factor):
    user_factor = user_n_factor[user]
    item_factor = item_n_factor[item]
    for i in range(len(user_factor)):
      rating += user_factor[i] * item_factor[i]
  return rating


def predict_nn_factor_effect(partial_row, user_n_factor, item_n_factor, W):
  '''Both nn and factor effect.'''
  rating = 0.0
  constants = partial_row[0][0]
  rating += constants
  # Apply nn effects.
  if config.interactive:
    if config.use_user or config.use_item:
      parameter_groups = partial_row[1:]
      for i, parameter_group in enumerate(parameter_groups):
        for t in parameter_group:
          col_index  = t[0]
          col_val = t[1]
          if col_index in W[i]:
            wij_val = W[i][col_index]
            rating += col_val * wij_val
    # Apply factor effects.
    if config.use_factor:
      user = partial_row[0][1]
      item = partial_row[0][2]
      if (user in user_n_factor) and (item in item_n_factor):
        user_factor = user_n_factor[user]
        item_factor = item_n_factor[item]
        for i in range(len(user_factor)):
          rating += user_factor[i] * item_factor[i]
  else:  # non-interactive.
    config_dict = io_util.parse_conf(config.local_config_path)
    if config_dict['use_user'] or config_dict['use_item']:
      parameter_groups = partial_row[1:]
      for i, parameter_group in enumerate(parameter_groups):
        for t in parameter_group:
          col_index  = t[0]
          col_val = t[1]
          if col_index in W[i]:
            wij_val = W[i][col_index]
            rating += col_val * wij_val
    # Apply factor effects.
    if config_dict['user_factor']:
      user = partial_row[0][1]
      item = partial_row[0][2]
      if (user in user_n_factor) and (item in item_n_factor):
        user_factor = user_n_factor[user]
        item_factor = item_n_factor[item]
        for i in range(len(user_factor)):
          rating += user_factor[i] * item_factor[i]

  return rating


class MRPredictions(mr_base.MapReduceBase):
  def __init__(self, num_processes, num_input_files,
      input_file_dir, output_file_dir, extra_args):
    super(MRPredictions, self).__init__(num_processes, num_input_files,
        input_file_dir, output_file_dir)
    self.samples = []
    # Extra global nn_parameters.
    assert len(extra_args) == 4, 'len(extra_args) = %d, not 4.' % len(extra_args)
    self.learned_nn_file_path = extra_args[0]
    self.user_n_factor_file_path = extra_args[1]
    self.item_n_factor_file_path = extra_args[2]
    self.destination_node = extra_args[3]
    self.pre_mr_done = False

  def pre_mr(self):
    '''Read the pre-computed data into instance variables.'''
    # Now fill the nn_parameters.
    config_dict = {}
    if not config.interactive:
      config_dict = io_util.parse_conf(config.local_config_path)

    if (config.interactive and (config.use_user or config.use_item)) or (
        not config.interactive and (config_dict['use_user'] or
        config_dict['use_item'])):
      self.nn_parameters = []
      for i in range(_MAX_PARAMETERS):
        self.nn_parameters.append({})

      nn_parameter_f = open(self.learned_nn_file_path)
      for line in nn_parameter_f:
        pg_id, val = line.strip().split()
        fields = pg_id.split(',')
        assert(int(fields[0]) <= _MAX_PARAMETERS)
        pg_group_index = ','.join(fields[1:])
        self.nn_parameters[int(fields[0])][pg_group_index] = float(val)
      nn_parameter_f.close()

      # Read user and item factor files.
    if (config.interactive and (config.use_factor)) or (
        not config.interactive and config_dict['use_factor']):
        self.user_n_factor = io_util.parse_user_n_factors(self.user_n_factor_file_path)
        self.item_n_factor = io_util.parse_item_n_factors(self.item_n_factor_file_path)

  def map_on_file(self, partition_id):
    '''Do prediction each rating.
    '''
    if not self.pre_mr_done:
      self.pre_mr()
      self.pre_mr_done = True

    print 'In map, processing ', partition_id
    # Read the data we need to predict into memory.
    user_n_item_score_list = {}
    predict_input_f = open(self.input_file_dir + os.sep + partition_id)
    for line in predict_input_f:
      row = generate_rows.generate_rows_from_line(line.strip())
      rating = predict_nn_factor_effect(row, self.user_n_factor,
          self.item_n_factor, self.nn_parameters)
      user = row[0][1]
      item = row[0][2]
      if user in user_n_item_score_list:
        user_n_item_score_list[user].append((item, rating))
      else:
        user_n_item_score_list[user] = [(item, rating)]
    predict_input_f.close()

    # First read the user and the items he rates.
    counter = 0
    user_n_predicted_items = []
    for user, item_score_list in user_n_item_score_list.iteritems():
      counter += 1
      if counter % 1000 == 0:
        print counter
        sys.stdout.flush()
      if len(item_score_list) <= 3:
        # Use all items as prediction.
        recmd_items = []
        for item, score in item_score_list:
          recmd_items.append(item)
        user_n_predicted_items.append((user, recmd_items))
      else:
        item_score_list.sort(key = lambda x: -float(x[1]))
        recmd_items = []
        if len(item_score_list) <= 3:
          for t in item_score_list:
            recmd_items.append(t[0])
        else:
          for t in item_score_list[:3]:
            recmd_items.append(t[0])
        user_n_predicted_items.append((user, recmd_items))

    out_shard_path = self.output_file_dir + os.sep + partition_id
    f = open(out_shard_path, 'w')
    for user, recmd_items in user_n_predicted_items:
      f.write('%s, %s\n' % (user, ' '.join(recmd_items)))
    f.close()

    # Copy to destination node.
    my_host_name = commands.getoutput('hostname').split('.')[0]
    print my_host_name
    if my_host_name != self.destination_node:
      print 'Copying data generated by %s.\n' % partition_id
      status = os.system ('scp ' + out_shard_path
           + ' ' + self.destination_node + ':' + self.output_file_dir)
      print 'scp status is ', status
      while status != 0:
        status = os.system ('scp ' + out_shard_path + ' ' + self.destination_node
                            + ':' + self.output_file_dir)
        print 'scp status is ', status

  def sort_merge_file(self, merge_file):
    # Do not sort the merge_file.
    pass

  def reduce_func(self, input_f, output_f):
    '''Parse the line and get top scores.'''
    pass
