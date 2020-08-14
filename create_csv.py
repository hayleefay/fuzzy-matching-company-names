import ast
import pandas as pd
import csv
import os
import pickle
filepath_root = os.getcwd()

input_file = 'output.txt'
output_file = 'matches.csv'

with open(filepath_root + input_file) as f:
    with open(filepath_root + output_file, 'w') as out_file:
        # create csv_writer and write out header
        csv_writer = csv.writer(out_file, delimiter=',', quotechar='"')
        csv_writer.writerow(['company_1', 'company_2', 'standard_1', 'standard_2', 'clean_1', 'clean_2', 'non_dict_1', 'non_dict_2', 'standard_score', 'clean_score', 'non_dict_score'])
        # read first line
        line = f.readline()
        while line:
            # parse line
            line = line.split('\n')[0]
            names, scores = line.split('\t')
            names = ast.literal_eval(names)
            scores = ast.literal_eval(scores)
            new_line = names + scores
                
            # write out
            csv_writer.writerow(new_line)
            # read next line
            line = f.readline()
