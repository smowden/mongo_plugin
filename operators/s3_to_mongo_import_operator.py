import json
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults
from tempfile import NamedTemporaryFile
from airflow.hooks.S3_hook import S3Hook
from mongo_plugin.hooks.mongo_hook import MongoHook
import os


def make_mongo_import_command(
        uri,
        collection,
        file,
        mode='upsert',
        type='json',
        query=None,
        fields=None,
        columns_have_types=False,
        upsert_fields=None,
        extra_params=[]
):
    """
    :param query:
    :param fields:
    :return:
    """
    fields_param = ""
    upsert_fields_param = ""
    columns_have_types_param = ""

    if fields is not None:
        fields_param = "-f '{fields}'".format(fields=",".join(fields))

    if upsert_fields is not None:
        upsert_fields_param = "--upsertFields '{upsert_fields}'".format(upsert_fields=",".join(upsert_fields))

    if columns_have_types:
        columns_have_types_param = "--columnsHaveTypes"

    mongo_import_cmd = """\
    mongoimport --uri {uri}\
    -c {collection}\
    --type {type}\
    --file {file}\
    --mode {mode}\
    {fields_param}\
    {columns_have_types}\
    {upsert_fields_param}\
    {extra_params}\
    """.format(
        uri=uri,
        collection=collection,
        type=type,
        file=file,
        mode=mode,
        fields_param=fields_param,
        upsert_fields_param=upsert_fields_param,
        columns_have_types=columns_have_types_param,
        extra_params=" ".join(["--{param}".format(param=param) for param in extra_params])
    )



    return mongo_import_cmd


class S3ToMongoImportOperator(BashOperator):
    @apply_defaults
    def __init__(
            self,
            mongo_collection,
            s3_bucket,
            s3_key,
            s3_conn_id='aws_default',
            mongo_conn_id='mongo_default',
            mongo_fields=None,
            mongo_upsert_fields=None,
            mongo_import_type='json',
            mongo_columns_have_types=False,
            mongo_extra_params=[],
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        mongo_uri = MongoHook(mongo_conn_id).get_uri()
        super(BashOperator, self).__init__(*args, **kwargs)
        self.tmp_file = NamedTemporaryFile()
        self.bash_command = make_mongo_import_command(
            uri=mongo_uri,
            collection=mongo_collection,
            fields=mongo_fields,
            upsert_fields=mongo_upsert_fields,
            type=mongo_import_type,
            columns_have_types=mongo_columns_have_types,
            file=self.tmp_file.name,
            extra_params=mongo_extra_params
        )
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        # S3 Settings
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        self.log.info("downloading file {s3_key} from {s3_bucket}"
                      .format(s3_key=self.s3_key, s3_bucket=self.s3_bucket))
        s3_conn = S3Hook()
        s3_key_object = s3_conn.get_key(
            key=self.s3_key,
            bucket_name=self.s3_bucket
        )
        s3_key_object.download_file(self.tmp_file.name)

        super().execute(context)

        size_mb = os.path.getsize(self.tmp_file.name) / pow(1024, 2)
        self.log.info("imported file %s to mongo, size %f mb" % (self.tmp_file.name, size_mb))
        self.tmp_file.close()
