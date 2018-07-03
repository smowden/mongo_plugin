import json
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults
from tempfile import NamedTemporaryFile
from airflow.hooks.S3_hook import S3Hook
import os


def make_mongo_export_command(connection, collection, out, query=None, fields=None):
    """
    :param query:
    :param fields:
    :return:
    """
    fields_param = "" if not fields == 0 else "-f '{fields}'".format(fields=",".join(fields))
    if query is None:
        query = {}

    mongo_export_cmd = """\
    mongoexport --uri {connection}\
    -c {collection}\
    -q '{query}'\
    --out {out}\
    {fields_param}\
    """.format(
        connection=connection,
        collection=collection,
        query=json.dumps(query),
        out=out,
        fields_param=fields_param
    )



    return mongo_export_cmd


class MongoExportToS3Operator(BashOperator):
    @apply_defaults
    def __init__(
            self,
            mongo_connection,
            mongo_collection,
            s3_conn_id,
            s3_bucket,
            s3_key,
            replace=False,
            mongo_query=None,
            mongo_fields=None,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(BashOperator, self).__init__(*args, **kwargs)
        self.tmp_file = NamedTemporaryFile()
        self.bash_command = make_mongo_export_command(
            connection=mongo_connection,
            collection=mongo_collection,
            out=self.tmp_file.name,
            query=mongo_query,
            fields=mongo_fields
        )
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        # S3 Settings
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.replace = replace

    def execute(self, context):
        super().execute(context)
        size_mb = os.path.getsize(self.tmp_file.name) / pow(1024, 2)
        s3_conn = S3Hook(self.s3_conn_id)
        self.log.info("uploading file %s to s3, size %f mb" % (self.tmp_file.name, size_mb))
        s3_conn.load_file(self.tmp_file.name, self.s3_key, bucket_name=self.s3_bucket, replace=self.replace)
        self.log.info("file upload finished")
        self.tmp_file.close()
