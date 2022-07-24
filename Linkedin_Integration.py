
import json
import os
import subprocess
from datetime import datetime, timedelta
from io import StringIO
import time
import numpy as np
import pandas as pd
import psycopg2
import requests
import boto3
import math
from datetime import datetime, timedelta
from config import global_config


class Linkedin:
    def __init__(self):
        self.global_config = global_config.GlobalConfig()
        self.date=datetime.now()-timedelta(2) 
        self.day = (('{:02d}'.format(self.date.day)))
        self.month = '{:02d}'.format(self.date.month)
        print(self.month)
        self.year =  self.date.year
        self.date_today=datetime.now()
        self.epoch=time.mktime(self.date.timetuple())*1000
        self.epoch_time=str((math.trunc(self.epoch)))
        self.date1=datetime.now()-timedelta(1)
        self.epoch1=time.mktime(self.date1.timetuple())*1000
        self.epoch_time1=str((math.trunc(self.epoch1)))
        self.start_date = self.date.strftime('%Y/%m/%d')
        self.today=self.date_today.strftime('%Y-%m-%d')
        self.date_num=self.date.strftime("%Y%m%d")



        self.redshift_cred = self.global_config.get_redshift_credentials()
        self.date = None
        # Connection to iSOCRATES redshift
        self.redshift_con = psycopg2.connect(dbname=self.redshift_cred['redshift_dbname'],
                                             host=self.redshift_cred['redshift_host'],
                                             port=self.redshift_cred['redshift_port'],
                                             user=self.redshift_cred['redshift_user'],
                                             password=self.redshift_cred['redshift_password'])
        self.redshift_cur = self.redshift_con.cursor()
        # self.server_path = self.global_config.base_dir_for_all + 'dma_data/'

    """ This function returns the aws s3 access key """

    def get_aws_access_key_id(self):
        cmd = "sudo aws configure get aws_access_key_id"
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        return output.decode("utf-8").replace("\n", "")

    """ This function returns the secret key """

    def get_aws_secret_access_key(self):
        cmd = "sudo aws configure get aws_secret_access_key"
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        return output.decode("utf-8").replace("\n", "")
    """ This function is to execute the commands """

    def execute_command_with_subprocess(self, cmd):
        try:
            subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as exc:
            self.global_config.write_to_log(exc, 'Error')
            raise Exception(exc)
        except Exception as e:
            self.global_config.write_to_log(e, 'Error')
            raise Exception(e)
    def get_query_config_file(self):
        query_config_response = self.global_config.s3_resource.Object(self.global_config.get_bucket_name(),
                                                                          self.global_config.get_query_config_file())
        self.query_string = query_config_response.get()['Body'].read()
        self.query_string = json.loads(self.query_string)
        return self.query_string 
    def get_client_config_file(self):
        self.global_config.write_to_log("Reading client config file")
        client_config_response = self.global_config.s3_resource.Object(self.global_config.get_bucket_name(),
                                                                           self.global_config.get_client_config_file())
        self.client_config = client_config_response.get()['Body'].read()
        self.client_config = json.loads(self.client_config)
        return self.client_config
    def get_follower_data_push_to_s3(self):
        data1=self.get_query_config_file()
        data2=self.get_client_config_file()
        for client_id in data1:
            self.client_id=client_id
            self.redshift_table_name = data1[self.client_id]['redshift_table_name']
            self.linkedin_api =  data1[self.client_id]['url_api']
            self.s3_raw_path = data1[self.client_id]['s3_raw_file_path']
            self.s3_file_path = data1[self.client_id]['s3_file_path']
        for details in data2:
            self.access_token = data2[details]['access_token']
            self.datasource_id = data2[details]['datasource_id']
            
        response=requests.get(self.linkedin_api.format(self.access_token,self.epoch_time,self.epoch_time1)).json()
        elements=(response['elements'])
        if len(elements)== 0:
            print('no data from linkedin')
            exit()
    
        else:
            new = pd.DataFrame(response["elements"])

            for i in new['followerGains']:
                my_dict = i
                df = pd.DataFrame(my_dict, index=[0])
                df["date"] = self.start_date
                df['day']= self.day
                df['month'] = self.month
                print(df['month'])
                df['year'] = self.year
                df['datenum'] = self.date_num
                df['client_id'] = self.client_id
                df['datasource_id'] = self.datasource_id
                df=df[['date', 'organicFollowerGain', 'paidFollowerGain','year','month','day','datenum','client_id','datasource_id']]
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf8')
                csv_buffer.seek(0)
                csv_buffer1 = StringIO()
                df.to_csv(csv_buffer1,index=False,encoding='utf8')
                self.global_config.s3_client.put_object(Body=csv_buffer.getvalue(),Bucket=self.global_config.get_bucket_name(),Key=self.s3_file_path)
                self.global_config.s3_client.put_object(Body=csv_buffer1.getvalue(),Bucket=self.global_config.get_bucket_name(),Key=self.s3_raw_path.format(self.today))
    def push_fact_data_to_redshift(self):
        try:
            # To delete data if any in redshift for the given date
            query = "delete from isocrates_madtechbi.{} where datenum={}".format(self.redshift_table_name,
                                                                      self.date_num)
            print(query)
            self.redshift_cur.execute(query)
            self.redshift_con.commit()
            query="copy isocrates_madtechbi.{} from 's3://{}/{}' credentials 'aws_access_key_id={};aws_secret_access_key={}' IGNOREHEADER 1 " \
                    "csv;".format(self.redshift_table_name,self.global_config.get_bucket_name(),self.s3_file_path,self.get_aws_access_key_id(),self.get_aws_secret_access_key()) 
            print(query)
            self.redshift_cur.execute(query)
            self.redshift_con.commit()
        except psycopg2.ProgrammingError as e:
            print("Error while performing delete_copy on table - {}".format(self.global_config.redshift_table_name))
            print(e)
            self.redshift_con.rollback()
            self.redshift_cur.close()
            self.redshift_con.close()
            raise e
    def delete_s3_file(self):
        try:
            date_30_ago = datetime.now() - timedelta(31)
            date_30_days_ago=date_30_ago.strftime('%Y-%m-%d')
            s3_client = boto3.client('s3',
                              aws_access_key_id=self.get_aws_access_key_id(),
                              aws_secret_access_key=self.get_aws_secret_access_key()
                              )
            s3_client.delete_object(Bucket=self.global_config.get_bucket_name(),Key=self.s3_raw_path.format(date_30_days_ago))
        except Exception as e:
            self.global_config.write_to_log("Error while deleting the file from s3", "Error")
            self.global_config.write_to_log(e)
            self.global_config.write_to_log(e.args)
            print(e)
            raise e


    def start(self):
        self.get_follower_data_push_to_s3()
        #self.push_fact_data_to_redshift()
        #self.delete_s3_file()
obj = Linkedin()
obj.start()



