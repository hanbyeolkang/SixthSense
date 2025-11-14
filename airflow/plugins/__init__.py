"""
Airflow Custom Plugins
영화진흥위원회(KOBIS) API 및 AWS S3 연동 플러그인
"""

from airflow.plugins_manager import AirflowPlugin


class KobisPlugin(AirflowPlugin):
    name = "kobis_plugin"