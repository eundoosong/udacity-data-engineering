from airflow.models import BaseOperator
from datetime import timedelta
from airflow.utils.decorators import apply_defaults


class CommonOperator(BaseOperator):
    """
    Common operator provides common parameter
    """
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CommonOperator, self).__init__(
            email_on_retry=False,
            retries=3,
            retry_delay=timedelta(minutes=5),
            *args,
            **kwargs)

    def execute(self, context):
        self._execute(context)

    def _execute(self, context):
        raise NotImplementedError
