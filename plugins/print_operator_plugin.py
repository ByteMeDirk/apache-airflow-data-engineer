from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class PrintOperator(BaseOperator):
    """
    An operator that simply prints a message.
    """

    @apply_defaults
    def __init__(self, message, *args, **kwargs):
        super(PrintOperator, self).__init__(*args, **kwargs)
        self.message = message

    def execute(self, context):
        print(self.message)


class PrintOperatorPlugin(AirflowPlugin):
    name = "print_operator_plugin"
    operators = [PrintOperator]
