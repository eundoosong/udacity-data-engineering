from airflow.models import BaseOperator


class DummyOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(DummyOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        pass
