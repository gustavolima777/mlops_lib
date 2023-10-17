import textwrap
import sys
import boto3
import time
import os
import pandas as pd
import numpy as np
from utils.utils import get_random_string
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor


class CloudUtils:
    def __init__(self,
                 team_id,
                 project_path='project_example/',
                 region='us-east-1',
                 pd_cursor=True):
        
        self.session = self.get_boto3_session()
        self.s3 = self.session.client('s3')
        self.team_id = team_id
        self.bucket = [bucket['Name'] 
                           for bucket in self.s3.list_buckets()['Buckets'] 
                               if self.team_id in bucket['Name']][0]
        
        self.s3_staging_dir = 's3://' + self.bucket + '/'
        self.project_path = project_path
        self.tables_path = self.s3_staging_dir + self.project_path + 'tables/'
        self.sagemaker_artefacts_path = self.s3_staging_dir + self.project_path + 'sagemaker/'
        self.region = region
        self.pyathena_connector = self.get_pyathena_connector(pd_cursor)
        self.glue = self.session.client('glue')
        self.sagemaker = self.session.client('sagemaker')
  
    def get_boto3_session(self):
        return boto3.Session()

    def get_pyathena_connector(self, pd_cursor):
        conn = connect(
            s3_staging_dir=self.s3_staging_dir,
            region_name=self.region,
            cursor_class=PandasCursor if pd_cursor else None)
        return conn.cursor() if pd_cursor else conn

    def write_s3(self, df, file_path, save_index=False, int_to_str=False):
        file_type = file_path.split('.')[-1]

        if file_type == 'parquet':
            save_method = df.to_parquet
        elif file_type == 'csv':
            save_method = df.to_csv
        elif file_type == 'xlsx':
            save_method = df.to_excel
        else:
            raise f'File Type ({file_type}) not allowed'

        local_key_path = self.local_path + file_path
        self.save_file_pandas(df, save_method, local_key_path, save_index,
                              int_to_str)

        key_path = self.project_path + file_path
        self.s3.upload_file(local_key_path, self.bucket_name, key_path)

        print(f'- Save to S3\n{file_type} File: {file_path}\nShape: '
              + f'{df.shape}')

    @staticmethod
    def save_file_pandas(df, save_method, file_path, save_index, int_to_str):
        if int_to_str:
            save_method(file_path, index=save_index,
                        quoting=csv.QUOTE_NONNUMERIC)
        else:
            save_method(file_path, index=save_index)

    def delete_files_from_s3(self, prefix):
        s3 = boto3.resource('s3')
        bucket_s3 = s3.Bucket(self.bucket_name)
        try:
            for i in bucket_s3.objects.filter(Prefix=prefix):
                print(f'deleting file {i.key}')
                bucket_s3.delete_objects(Delete={'Objects': [{'Key': i.key}]})
                print(f'file {i.key.split("/")[-1]} deleted')
        except Exception as ex:
            print(str(ex))

    def get_df_from_athena_query(self, file, cursor=False):
        query = open(file, 'r')

        if cursor:
            df = self.pyathena_connector.execute(query.read()).as_pandas()
        else:
            df = pd.read_sql(query.read(), self.pyathena_connector)
        query.close()

        return df

    def get_df_from_file(self, file, use_s3, method, **kwargs):
        if use_s3:
            file_path = self.s3_staging_dir + self.project_path + file
        else:
            file_path = self.local_path + file

        return method(file_path, **kwargs)
    
    def delete_glue_job(self,job_name):
        try:
            delete_response = self.glue.delete_job(JobName=job_name)
            if (delete_response['ResponseMetadata']['HTTPStatusCode'] == 200):
                print(f"-Deleted job:{job_name} completed successfully")
            else:
                print("-Job is not deleted lock the aws glue jobs to debug")
        except Exception as delete_error:
                print("An delete error occurred:", delete_error)
    
    def run_spark_sql(self,query_sql,spark_session):
        return spark_session.sql(query_sql)
    
    def process_glue_job(self,query_sql,glue_arn,get_data = False,advanced_resourses = False,max_time_running_job_mins = 10):
        
        job_name = f'spark-to-s3-parquet-{get_random_string()}'
        print(f"-Start spark job name:{job_name}, staging_dir:{self.s3_staging_dir}")
        
        scripts_path = self.s3_staging_dir+self.project_path+"spark-to-parquet-scripts/"
        parquet_path = self.s3_staging_dir+self.project_path+f"{job_name}/"
        
        resources_ = {'NumberOfWorkers':30,
                      'WorkerType':'G.4X'} if advanced_resourses else {'NumberOfWorkers':10,
                                                                       'WorkerType':'G.1X'}
        
        spark_script = textwrap.dedent("""
            import sys
            from awsglue.transforms import *
            from awsglue.utils import getResolvedOptions
            from pyspark.context import SparkContext
            from awsglue.context import GlueContext
            from awsglue.job import Job
            from awsglue.dynamicframe import DynamicFrame

            sc = SparkContext.getOrCreate()
            glueContext = GlueContext(sc)
            spark = glueContext.spark_session
            job = Job(glueContext)

            df = spark.sql('''%s''')
            df_dinamico=DynamicFrame.fromDF(df, glueContext, "df_dinamico")
            df_dinamico_ = df_dinamico.coalesce(1)

            write_parquet_frame = glueContext.write_dynamic_frame.from_options(
                frame=df_dinamico_,
                connection_type="s3",
                format="parquet",
                connection_options={
                    "path": "%s",
                    "partitionKeys": []
                },
                format_options={"compression": "snappy"},
                transformation_ctx="write_parquet_frame",
            )

            job.commit()
            """) % (query_sql,parquet_path)
        
        print("-Generate python script in s3")
        with open(f'{job_name}.py', 'w') as f:
            f.write(spark_script)
        
        self.s3.upload_file(f'{job_name}.py',
                              self.bucket_name,
                              f'{self.project_path}spark-to-parquet-scripts/{job_name}.py')
        os.remove(f'{job_name}.py')
        
        print("-Inializing job parameters")
        job_script = {
                        'Name': job_name,
                        'Role': glue_arn,
                        'Timeout':max_time_running_job_mins,
                        'GlueVersion':'3.0',
                        'Command': {
                            'Name': 'glueetl',
                            'ScriptLocation': f'{scripts_path}{job_name}.py',
                            'PythonVersion': '3'
                                    },
                        'DefaultArguments': {
                            "--enable-glue-datacatalog": "true"}
        }
        
        job_run = {
                    'JobName': job_name,
                    'Timeout':max_time_running_job_mins
        }
        
        try:
            print("-Creating job")
            self.glue.create_job(**job_script,**resources_)
            try:
                print("-Executing job")
                response_job_run = self.glue.start_job_run(**job_run,**resources_)
                i_time = 0
                status_repeticao = 'RUNNING'
                max_time_running_job_segs = max_time_running_job_mins*60
                while status_repeticao == "RUNNING":

                    job_response = self.glue.get_job_run(JobName=job_name,
                                                            RunId=response_job_run['JobRunId']
                                                        )
                    
                    status = job_response['JobRun']['JobRunState']

                    if (status == "FAILED") or (status == 'CANCELLED'):
                        print(f"-Something in job is wrong: {job_response['JobRun']['ErrorMessage']}")
                        self.delete_glue_job(job_name)
                        break

                    elif status =='SUCCEEDED':
                        print("-Job finished")
                        status_repeticao = "SUCCEEDED"
                    else:
                        time.sleep(30)
                        i_time += 30
                        print(f"-Executing job: {i_time}s")
                        if i_time > max_time_running_job_segs:
                            print("-Something in job is wrong: Max time in running job is exceded") 
                            self.delete_glue_job(job_name)
                            print(f"-Deleting job locks py scritp in :{scripts_path}{job_name}.py")
                            break
                print(f"-Spark job finished, paths:\n-parquet_path = {parquet_path}\n-scripts_path = {scripts_path}{job_name}.py")
                self.delete_glue_job(job_name)
                if (get_data):
                    return pd.read_parquet(parquet_path)
                
            except Exception as job_run_error:
                print("An error occurred:", job_run_error)            
                self.delete_glue_job(job_name)
                
        except Exception as error:
            print("An error occurred when trying create spark job:", error)