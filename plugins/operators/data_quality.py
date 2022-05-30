from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tablelist = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tablelist = tablelist
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.tablelist == []:
           self.log.info(f"no table to check")
           return
                        
        for tb in self.tablelist: 
            records = redshift.get_records(f"SELECT COUNT(*) FROM {tb}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {tb} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {tb} contained 0 rows")
            self.log.info(f"Data quality on table {tb} check passed with {records[0][0]} records")