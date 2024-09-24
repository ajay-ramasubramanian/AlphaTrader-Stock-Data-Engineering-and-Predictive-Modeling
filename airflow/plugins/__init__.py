from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class SpotifyPlugin(AirflowPlugin):
    name = "spotify_plugin"
    operators = [
        operators.LoadDimOperator,
        operators.LoadFactOperator,
        operators.LoadTransformationOperator,
    ]
   