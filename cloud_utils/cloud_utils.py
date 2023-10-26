import textwrap
import sys
import boto3
import time
import timeit
import os
import multiprocess
import pandas as pd
import numpy as np
import pyarrow
from pyarrow import concat_tables
from pyarrow.parquet import ParquetDataset,read_table,read_schema
from utils.utils import get_random_string, convert_timestamps_to_string
from data_utils.dataquality_utils import DataQualityUtils 

class CloudUtils:
    def __init__(self,
                 team_id,
                 project_path='project_example/',
                 region='us-east-1',
                 athena_workgroup = 'gg_consumo_voomp'):
        
        self.session = self.get_boto3_session()
        self.s3 = self.session.client('s3')
        self.glue = self.session.client('glue')
        self.athena = self.session.client('athena')
        self.athena_workgroup = athena_workgroup
        self.sagemaker = self.session.client('sagemaker')
        self.team_id = team_id
        self.bucket = [bucket['Name'] 
                           for bucket in self.s3.list_buckets()['Buckets'] 
                               if self.team_id in bucket['Name']][0]
        
        self.s3_staging_dir = 's3://' + self.bucket + '/'
        self.project_path = project_path
        self.tables_path = self.s3_staging_dir + self.project_path + '/tables/'
        self.data_path = self.s3_staging_dir + self.project_path + 'data/'
        self.sagemaker_artefacts_path = self.s3_staging_dir + self.project_path + 'sagemaker/'
        self.region = region

        print(f"Bucket {self.bucket} is sucessfull selected")
  
    def get_boto3_session(self):
        return boto3.Session()

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
    
    def process_glue_job(self,
                         query_sql,
                         glue_arn,
                         get_data = False,
                         advanced_resourses = False,
                         max_time_running_job_mins = 10):
        
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
            
    def process_athena_query(self,
                             sql_query,
                             max_time_running_job_mins=10):
    
        listOfStatus = ['SUCCEEDED', 'FAILED', 'CANCELLED']
        listOfInitialStatus = ['RUNNING', 'QUEUED']
        
        print('Starting Query Execution:')        
        response = self.athena.start_query_execution(
            QueryString = sql_query,
            WorkGroup = self.athena_workgroup
        )

        queryExecutionId = response['QueryExecutionId']
        print(f'Query id = {queryExecutionId}')
        
        execution_path = self.athena.get_query_execution(QueryExecutionId = queryExecutionId)['QueryExecution']['ResultConfiguration']['OutputLocation'] 
        status = self.athena.get_query_execution(QueryExecutionId = queryExecutionId)['QueryExecution']['Status']['State']
        i_time = 0
        
        while status in listOfInitialStatus:
            status = self.athena.get_query_execution(QueryExecutionId = queryExecutionId)['QueryExecution']['Status']['State']
            if status in listOfStatus:
                if status == 'SUCCEEDED':
                    print(f'Query Succeeded! {i_time}mins')
                    return queryExecutionId,execution_path
                    try:
                        results = self.athena.get_query_runtime_statistics(QueryExecutionId = queryExecutionId)['QueryRuntimeStatistics']['Rows']
                        print(f'-Query output rows:{results["OutputRows"]}\n-Query output bytes:{results["OutputBytes"]/100000}mbs')
                    except:
                        print('No Rows in query execution')
                elif status == 'FAILED':
                    print('Query Failed!')
                elif status == 'CANCELLED':
                    print('Query Cancelled!')
                    break
            else:
                time.sleep(10)
                i_time += 0.167
                print(f"-Executing query: {i_time}mins")
            if i_time > max_time_running_job_mins:
                print("- Query timeout {}") 
                self.athena.stop_query_execution(queryExecutionId)
                break
            
    @staticmethod
    def parquet_to_pandas(file):
        
        schema = read_schema('s3://'+file,memory_map=True)
        schema_dict = {name:str(pa_dtype) 
                           for name, pa_dtype in zip(schema.names, schema.types)}
        
        table = read_table('s3://'+file, 
                           schema=pyarrow.schema(convert_timestamps_to_string(schema_dict)))
        
        df = table.to_pandas()
        return df
    
    def s3_to_pandas(self,parquet_path):
        
        parquet_files = ParquetDataset(parquet_path).files
        p = multiprocess.Pool(multiprocess.cpu_count())
        concat = pd.concat(list(p.map(self.parquet_to_pandas,parquet_files)))
        return concat

    def execute_process_athena_query(self,
                                     sql_query,
                                     pandas_dataframe = False,
                                     max_time_running_job_mins=10,
                                     ):
    
        t_start = timeit.default_timer()
        print('-Start Execution')
        
        random_query_path = get_random_string(10)
        if (pandas_dataframe==False):
            
            self.process_athena_query(sql_query,
                                 max_time_running_job_mins)
            
            diff_time = timeit.default_timer()-t_start
            print(f"Elapsed time: {round((diff_time)/60)}m{round(diff_time%60)}s")
            
        else:
            
            sql_query_adjust = textwrap.dedent(f"""UNLOAD ({sql_query})
            TO '{self.data_path+random_query_path}' 
            WITH (format = 'PARQUET',compression = 'SNAPPY')""")
        
            self.process_athena_query(sql_query_adjust,
                                 max_time_running_job_mins)
            
            concat = self.s3_to_pandas(f'{self.data_path+random_query_path}')
            diff_time = timeit.default_timer()-t_start
            print(f"Pandas dataframe sucessfully created \n-Data path: {self.data_path+random_query_path}")
            print(f"Elapsed time: {round((diff_time)/60)}m{round(diff_time%60)}s")
            return concat
        
    def s3_to_athena_table(self,
                           table_name,
                           database,
                           s3_parquet_path,
                           is_unique_parquet=True,
                           drop_table = False,
                           get_dataquality = False):
        
        print("Make sure that when using a path the files have the same schema!!!")
        
        if is_unique_parquet:
            schema = read_schema(s3_parquet_path,memory_map=True)
            if get_dataquality:
                data = read_table(s3_parquet_path)
        else:
            pyarrow_tables = []
            parquet_files = ParquetDataset(s3_parquet_path).files
            
            if get_dataquality:
                for file in parquet_files:
                    table = read_table('s3://'+file)
                    pyarrow_tables.append(table)
                    
                concat = concat_tables(pyarrow_tables)
                schema = concat.schema
                data = DataQualityUtils(concat)
            else:
                schema = read_table('s3://'+parquet_files[0]).schema

        schema = pd.DataFrame(({"column": name, "d_type": str(pa_dtype)} 
                           for name, pa_dtype in zip(schema.names, schema.types)))
        
        schema['column'] = schema['column'].apply(lambda x: x.strip().replace(' ','_'))
        
        schema=", ".join(schema\
        .assign(tipo = lambda x: np.select(
                                            [
                                                x.d_type == 'string',
                                                x.d_type == 'int64',
                                                x.d_type == 'int32', 
                                                x.d_type.str.contains('decimal'),
                                                x.d_type == 'bool', 
                                                x.d_type.str.contains('timestamp')
                                            ],
                                            [
                                                'string',
                                                'bigint',
                                                'int',
                                                'decimal',
                                                'boolean',
                                                'timestamp'
                                            ],
                                            default = x.d_type))\
        .assign(sql = lambda x: x.column + " " + x.tipo).sql)
            
        query = textwrap.dedent(f'''
        CREATE EXTERNAL TABLE {database}.{table_name}
        ( {schema} )
        STORED AS PARQUET
        LOCATION '{s3_parquet_path}'
        TBLPROPERTIES('parquet.compression'='SNAPPY');
        ''')
        
        delete_query = textwrap.dedent(f'''
        DROP TABLE IF EXISTS {database}.{table_name};                                      
                                       ''')
        if drop_table:
            print("-Start delete Table Query")
            self.process_athena_query(delete_query)
            
        print("-Start create table Query")
        self.process_athena_query(query)
        print(f'-Input path = {s3_parquet_path}\nTable Name = {database}.{table_name}')
        print("-Start validy table Query")
        query_id,execution_path = self.process_athena_query(f'select count(*) from {database}.{table_name}')
        
        print(f'Total rows in created table = {pd.read_csv(execution_path).iloc[0,0]}')
        
        if get_dataquality:
            return data.get_data_infos()
        
    def csv_txt_to_parquet(self,
                           file_path,
                           delimiter = ';'
                           ):
        
        bucket_file = file_path.replace("s3://","").split('/')[0]
        my_bucket = self.session.resource('s3').Bucket(bucket_file)
        prefix = '/'.join(file_path.replace('s3://','').split('/')[1:])
        text_files_list = []
        
        for objects in my_bucket.objects.filter(Prefix=prefix):
            if objects.key.endswith('txt') or objects.key.endswith('csv'):
                text_files_list.append(objects.key)
        list_text_df = []        
        try:
            for txt_file in text_files_list:            
                df = pd.read_csv(bucket_file+'/'+txt_file,delimiter=delimiter)
                list_text_df.append(df)
            df = pd.concat(list_text_df)
        except Exception as read_csv_error:
            print("An error occurred:", read_csv_error)
        
        r_string = get_random_string(10)
        random_path_table = self.tables_path+r_string+'/'
        df.to_parquet(random_path_table+r_string+'.parquet',index=False)
        print(f'Parquet saved in {random_path_table}')
