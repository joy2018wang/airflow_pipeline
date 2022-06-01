from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checklist = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checklist = checklist
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.checklist == []:
           self.log.info(f"no query to check")
           return
                        
        for task in self.checklist:
            sq = task['test_sql']
            expected = task['expected_result']
            compare = task['comparisonfun']
            records = redshift.get_records(sq)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality for {sq} check failed. returned no results")
            num_records = records[0][0]
            if compare(expected, num_records):
                raise ValueError(f"Data quality for {sq} check failed. Invalid result")
            self.log.info(f"Data quality on table {sq} check passed")