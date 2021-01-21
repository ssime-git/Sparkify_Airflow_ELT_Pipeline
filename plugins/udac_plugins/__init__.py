from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import udac_plugins.udac_operators
import udac_plugins.udac_helpers

#from udac_plugins.udac_operators.stage_redshift import StageToRedshiftOperator
#from udac_plugins.udac_operators.load_fact import LoadFactOperator
#from udac_plugins.udac_operators.load_dimension import LoadDimensionOperator
#from udac_plugins.udac_operators.data_quality import DataQualityOperator
#from udac.plugins.udac_helpers import SqlQueries

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        udac_plugins.udac_operators.StageToRedshiftOperator,
        udac_plugins.udac_operators.LoadFactOperator,
        udac_plugins.udac_operators.LoadDimensionOperator,
        udac_plugins.udac_operators.DataQualityOperator,
        udac_plugins.udac_operators.CreateTableOperator
    ]
    helpers = [
        udac_plugins.udac_helpers.SqlQueries
    ]
