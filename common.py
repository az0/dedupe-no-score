"""
By Andrew Ziem
"""

# Python built-in modules
import csv
import logging
import os
import re
from datetime import datetime

# third party modules
from unidecode import unidecode

# globals/constants
data_dir = '.'
train_labeled_csv_fn = os.path.join(data_dir, 'train-labeled.csv')
learned_settings_fn = os.path.join(data_dir, 'settings.dedupe')
training_json_fn = os.path.join(data_dir, 'matching_training.json')


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    try : # python 2/3 string differences
        column = column.decode('utf8')
    except AttributeError:
        pass
    column = unidecode(column)
    column = re.sub('\n', ' ', column)
    column = re.sub('-', '', column)
    column = re.sub('/', ' ', column)
    column = re.sub("'", '', column)
    column = re.sub(",", '', column)
    column = re.sub(":", ' ', column)
    column = re.sub('  +', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column :
        column = None
    return column


def read_data(filename, max_rows = False):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID.
    """

    filename = os.path.abspath(filename) # same in write_linked.write_linked()
    print(f'read_data({filename}, {max_rows})')

    data_d = {}

    with open(filename, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if max_rows and i > max_rows:
                break
            clean_row = dict([(k, preProcess(v)) for (k, v) in row.items()])
            # FIXME: dedupe API requests that the row key be the record key,
            # but we make a fake key.
            row_key = filename + str(i)
            data_d[row_key] = dict(clean_row)
    print(f'read {len(data_d)} records from {filename}')
    assert len(data_d) > 0

    return data_d

