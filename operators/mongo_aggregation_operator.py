from mongo_plugin.hooks.mongo_hook import MongoHook
from airflow.models import BaseOperator


class MongoAggregationOperator(BaseOperator):
    """
    Mongo -> S3
    :param mongo_conn_id:           The source mongo connection id.
    :type mongo_conn_id:            string
    :param mongo_collection:        The source mongo collection.
    :type mongo_collection:         string
    :param mongo_database:          The source mongo database.
    :type mongo_database:           string
    :param mongo_query:             The specified mongo query.
    :type mongo_query:              string
    """

    def __init__(self,
                 mongo_conn_id,
                 mongo_collection,
                 mongo_database,
                 mongo_query,
                 *args, **kwargs):
        super(MongoAggregationOperator, self).__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        self.mongo_query = mongo_query
        if not isinstance(self.mongo_query, list):
            raise TypeError('Mongo aggregation query must be of type list')


        # KWARGS
        self.replace = kwargs.pop('replace', False)

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        mongo_conn = MongoHook(self.mongo_conn_id).get_conn()

        # Grab collection and execute query according to whether or not it is a pipeline
        collection = mongo_conn.get_database(self.mongo_db).get_collection(self.mongo_collection)
        collection.aggregate(self.mongo_query)
