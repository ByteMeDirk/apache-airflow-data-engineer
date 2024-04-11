import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class BusinessUnitOperator(BaseOperator):
    """
    An operator to handle Business Unit resource URIs.
    """

    @apply_defaults
    def __init__(self, business_unit, *args, **kwargs):
        super(BusinessUnitOperator, self).__init__(*args, **kwargs)
        self.business_unit = business_unit

    def execute(self, context) -> dict:
        """
        Get the resource URIs for the given business unit.

        Args:
            context (dict): The context that is passed to the operator.

        Returns:
            dict: The resource URIs for the given business unit.
        """
        bu_resource_uris = {
            "analytics_squad_1": {
                "bu_id": "1",
                "environment_id": {
                    "dev": "11",
                    "qa": "12",
                    "prod": "13",
                },
            },
            "pos_squad_1": {
                "bu_id": "2",
                "environment_id": {
                    "dev": "21",
                    "qa": "22",
                    "prod": "23",
                },
            },
            "sales_squad_1": {
                "bu_id": "3",
                "environment_id": {
                    "dev": "31",
                    "qa": "32",
                    "prod": "33",
                },
            },
        }

        bu_info = bu_resource_uris.get(self.business_unit)
        if bu_info:
            s3_uris = {
                "inbound": {
                    "dev": f"s3://ib-bu-{bu_info['bu_id']}-{bu_info['environment_id']['dev']}",
                    "qa": f"s3://ib-bu-{bu_info['bu_id']}-{bu_info['environment_id']['qa']}",
                    "prod": f"s3://ib-bu-{bu_info['bu_id']}-{bu_info['environment_id']['prod']}",
                },
                "transformation": {
                    "dev": f"s3://tr-bu-{bu_info['bu_id']}-{bu_info['environment_id']['dev']}",
                    "qa": f"s3://tr-bu-{bu_info['bu_id']}-{bu_info['environment_id']['qa']}",
                    "prod": f"s3://tr-bu-{bu_info['bu_id']}-{bu_info['environment_id']['prod']}",
                },
                "outbound": {
                    "dev": f"s3://ob-bu-{bu_info['bu_id']}-{bu_info['environment_id']['dev']}",
                    "qa": f"s3://ob-bu-{bu_info['bu_id']}-{bu_info['environment_id']['qa']}",
                    "prod": f"s3://ob-bu-{bu_info['bu_id']}-{bu_info['environment_id']['prod']}",
                },
            }
            logging.info(f"Resource URIs for {self.business_unit}: {s3_uris}")
            return s3_uris

        else:
            logging.warning(f"No resource information found for {self.business_unit}")
            return {}


class BusinessUnitOperatorPlugin(AirflowPlugin):
    """
    A plugin to register the BusinessUnitOperator.
    """
    name = "business_unit_operator_plugin"
    operators = [BusinessUnitOperator]
