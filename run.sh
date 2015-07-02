rm -rf ./job/*.job
python ./aggregate_tables.py DropAllTables --workers 20 --local-scheduler
python ./aggregate_tables.py AggregateTableRun --workers 10 --local-scheduler