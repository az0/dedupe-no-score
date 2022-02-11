# dedupe: no scores 

## Data files

* cluster.csv: output of record linkage
* link-o.csv: some records match link-s.csv, but they are not labeled
* link-s.csv: some records match link-o.csv, but they are not labeled
* pip_freeze.txt: output of `pip freeze
* train-labeled.csv: labeled pairs, both matched and distinct
* train-o.csv: for training
* train-s.csv: for training

## Code

* common.py: shared code
* record_linkage.py: use pre-trained model to link records
* train_from_csv.py: script to train model and save it
* train_and_link_in_one_session.py: train and link in one session with no deserialization

## Instructions

First, run `train_from_csv.py`. Then, run `record_linkage.py`.

Alternatively, run `train_and_link_in_one_session.py`.


## Cross reference

https://github.com/dedupeio/dedupe/issues/963