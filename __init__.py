from airflow.plugins_manager import AirflowPlugin
from mongo_plugin.hooks.mongo_hook import MongoHook
from mongo_plugin.operators.s3_to_mongo_operator import S3ToMongoOperator
from mongo_plugin.operators.mongo_to_s3_operator import MongoToS3Operator
from mongo_plugin.operators.mongo_aggregation_operator import MongoAggregationOperator
from mongo_plugin.operators.mongo_export_to_s3_operator import MongoExportToS3Operator


class MongoPlugin(AirflowPlugin):
    name = "MongoPlugin"
    operators = [MongoToS3Operator, S3ToMongoOperator, MongoAggregationOperator, MongoExportToS3Operator]
    hooks = [MongoHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
