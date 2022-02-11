"""
Link records using settings learned from train_from_csv.py

By Andrew Ziem
Some code from https://github.com/dedupeio/dedupe-examples
"""

# Python built-in modules
import csv
import logging
import os
from timeit import default_timer as timer
from datetime import timedelta

# local module
import common

# constants/globals
logger = logging.getLogger()
data_1_fn = 'link-o.csv'
data_2_fn = 'link-s.csv'
output_file_fn = 'cluster.csv'
link_min_threshold = 0.50

def cluster(linker, data_1, data_2):
    logger.info('cluster()')
    timer_start = timer()
    linked_records = linker.join(data_1, data_2, threshold=link_min_threshold)
    logger.info(f'duration in linker.join(): {timedelta(seconds=timer()-timer_start)}')
    logger.info(f'# duplicate sets {len(linked_records)}')
    return linked_records

def write_linked(linked_records, output_file, in_file_1, in_file_2):
    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    cluster_id = None
    for cluster_id, (cluster, score) in enumerate(linked_records):
        for record_id in cluster:
            cluster_membership[record_id] = (cluster_id, score)

    if cluster_id :
        unique_id = cluster_id + 1
    else :
        unique_id =0

    with open(output_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)

        header_unwritten = True

        for fileno, filename in enumerate((in_file_1, in_file_2)) :
            filename = os.path.abspath(filename) # same in common.read_data()
            with open(filename, encoding='utf-8') as f_input :
                reader = csv.reader(f_input)

                if header_unwritten :
                    heading_row = next(reader)
                    heading_row.insert(0, 'source_file')
                    heading_row.insert(0, 'link_score')
                    heading_row.insert(0, 'cluster_id')
                    writer.writerow(heading_row)
                    header_unwritten = False
                else :
                    next(reader)

                for row_id, row in enumerate(reader):
                    cluster_details = cluster_membership.get(filename + str(row_id))
                    if cluster_details is None :
                        cluster_id = unique_id
                        unique_id += 1
                        score = None
                    else :
                        cluster_id, score = cluster_details
                    row.insert(0, fileno)
                    row.insert(0, score)
                    row.insert(0, cluster_id)
                    writer.writerow(row)


def go(linker = None):
    if not linker:
        import dedupe
        logger.info(f'reading from {common.learned_settings_fn}')
        with open(common.learned_settings_fn, 'rb') as sf :
            linker = dedupe.StaticRecordLink(sf)

    data_1 = common.read_data(data_1_fn)
    data_2 = common.read_data(data_2_fn)
    linked_records = cluster(linker, data_1, data_2)
    write_linked(linked_records, output_file_fn, data_1_fn, data_2_fn)

if __name__ == "__main__":
    go()
    