import logging
from datetime import datetime,timedelta
import math
import boto3


class GlobalConfig:
    """ This function is used to initialise the variables """

    def __init__(self):
        self.profile_name = 'sisense'
        self.access_token="AQXvwiXnDMptytKiUlc56FjJbkrdKicaQaJF-Xk01chojl4XrAYeTt2AzYqWUe6BBx4vWxc7nGnaiwR-9LKNA6X6SgexdMS6WXhfuixjres2GJk5zbegKi1eUvHWbzn6OyaK4euAEB71-CvAFCSlPnHhNoS-Qet9YqJ35WcF7Oy_ePbFVzxW91rzjJjOAc_PIAwWnjZfNNk32tEUudAjR2olshZRz-i4zrqitwRUtyIRZTUAOsi3YvE8iAgaE54lx6X9nSlr3vCC2xVO5j2SZlco6EfLRDkXFigXwSAF-zTf4fEZyio3BeXv8HA6eTaVZ6mqs-4m1wzhkckZw_9rC2dEY_0XJw"
        self.s3_raw_path='Linkedin/Raw_file/followers_data{}.csv'
        self.s3_file_path='Linkedin/followers/followers_data.csv'
        self.dev = boto3.session.Session(profile_name=self.profile_name, region_name='us-east-1')
        self.s3_resource = self.dev.resource('s3')
        self.s3_client = self.dev.client('s3')
        self.redshift_table_name='followers_linkd'
    """ This function returns the client configuration file stored in S3"""
    
    
    def get_client_config_file(self):
         return 'Linkedin/Configurations/client_config.json'
    
    
    def get_query_config_file(self):
         return 'Linkedin/Configurations/query.json'
    
    """ This function returns the s3 bucket name"""
    
    def get_bucket_name(self):
        return 'mediatrader'
    """ This function is to write log details to log file"""
 
    @staticmethod
    def write_to_log(data, _type='info'):
        if _type == 'error':
            logging.error(data)
        else:
            logging.info(data)



    def get_linkedin_api(self):
        return "https://api.linkedin.com/v2/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:27249773&oauth2_access_token={}&timeIntervals.timeGranularityType:DAY&timeIntervals.timeRange.start={}&timeIntervals.timeRange.end={}"

    """ This function returns the redshift connection details """
    def get_redshift_credentials(self):
        return {"redshift_dbname": "mdtechbi_prod",
                "redshift_host": "isocrates-production.c2css3rohega.us-east-1.redshift.amazonaws.com",
                "redshift_port": "5439",
                "redshift_user": "isocrates_madtechbi",
                "redshift_password": "MADTechWisely2022"}


