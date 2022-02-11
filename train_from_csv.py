"""
Train the dedupe system on human-labeled pairs

By Andrew Ziem
Some code from https://github.com/dedupeio/dedupe-examples
"""


# Python built-in modules
import logging
from datetime import datetime


# local modules
import common

logger = logging.getLogger()

# constants
max_distinct_count = 200
max_match_count = 200
in_file_1_fn = 'train-o.csv'
in_file_2_fn = 'train-s.csv' 

prepare_training_sample_size = 1500 # 15000=default

def get_naive_linker():
    # Define the fields the linker will pay attention to
    fields = [
            {'field' : 'name', 'type': 'String'},
            {'field' : 'street', 'type': 'Address', 'has missing': True},
            {'field' : 'city', 'type': 'ShortString'},
            {'field' : 'state', 'type': 'Exact'},
            {'field' : 'zip', 'type': 'ShortString'}]

    import dedupe # If dedupe is imported before set_up_logging(), then it messes up the format.
    linker = dedupe.RecordLink(fields)
    return linker

def get_labeled():
    """Return a dictionary of labeled examples for use in mark_pairs()"""
    train_csv = common.read_data(common.train_labeled_csv_fn)

    # see https://dedupe.io/developers/library/en/latest/API-documentation.html#Dedupe.markPairs
    match = []
    distinct = []
    for row_key in train_csv:
        row = train_csv[row_key]
        o_dict = {'name': row['o_name'], 'street': row['o_street'],
                  'city': row['o_city'], 'state': row['o_state'], 'zip': row['o_zip']}
        s_dict = {'name': row['s_name'], 'street': row['s_street'],
                  'city': row['s_city'], 'state': row['s_state'], 'zip': row['s_zip']}
        pair = (o_dict, s_dict)
        if row['type'] == 'distinct':
            distinct.append(pair)
        elif row['type'] == 'match':
            match.append(pair)
        else:
            raise RuntimeError('unknown type : %s ' % row['type'])

    logger.warning(f'Original counts of labeled cases: {len(match):,} match vs. {len(distinct):,} distinct')
    import random
    if max_distinct_count:
        if max_distinct_count > len(distinct):
            logger.warning('max_distinct_count > len(distinct)')
        distinct = random.sample(distinct, k=max_distinct_count)
    if max_match_count:
        if max_match_count > len(match):
            logger.warning('max_match_count  > len(match)')
        match = random.sample(match, k=max_match_count)
    logger.warning(f'Final counts of labeled cases: {len(match):,} match vs. {len(distinct):,} distinct')

    labeled_examples = {'match': match,
                        'distinct': distinct}
    return labeled_examples

def label_dict_to_json(d):
    """Convert training dictionary to JSON training file"""
    import io
    import json
    f = io.StringIO(json.dumps(d, indent=4))
    return f

def go():
    linker = get_naive_linker()

    logger.warning('reading data 1 and 2')
    data_1 = common.read_data(in_file_1_fn)
    data_2 = common.read_data(in_file_2_fn)

    logger.warning('get_labeled()')
    labeled_examples = get_labeled()

    logger.warning(f'prepare_training(sample_size={prepare_training_sample_size})')
    linker.prepare_training(data_1, data_2,
        training_file=label_dict_to_json(labeled_examples), 
        sample_size=prepare_training_sample_size)

    if False:
        logger.warning('Skip console_label() for automation')
    else:
        logger.warning('console_label()')
        import dedupe # If dedupe is imported before set_up_logging(), then it messes up the format.
        dedupe.convenience.console_label(linker)

    logger.warning('train()')
    linker.train()

    logger.warning('write_training()')
    with open(common.training_json_fn, 'w') as tf:
        linker.write_training(tf)

    logger.warning('write_settings()')
    with open(common.learned_settings_fn, 'wb') as sf:
        linker.write_settings(sf)
        
    return linker


if __name__ == "__main__":
    try:
        go()
    except:
        logger.exception('unhandled exception in go()')
    logger.warning('All done') 
