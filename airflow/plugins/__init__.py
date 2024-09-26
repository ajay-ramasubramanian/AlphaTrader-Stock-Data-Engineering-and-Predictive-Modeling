# from __future__ import division, absolute_import, print_function

# from airflow.plugins_manager import AirflowPlugin

# import operators

# # Defining the plugin class
# class SpotifyPlugin(AirflowPlugin):
#     name = "spotify_plugin"
#     operators = [
#         operators.LoadDimOperator,
#         operators.LoadFactOperator,
#         operators.LoadTransformationOperator,
#     ]
   

from load_dim_operator import LoadDimOperator
from load_fact_operator import LoadFactOperator
from load_transformation_operator import LoadTransformationOperator

__all__ = [
    'LoadDimOperator',
    'LoadFactOperator',
    'LoadTransformationOperator'
]

