import sqlalchemy as SA 
import psycopg2
import boto3
import io
import uuid
from io import StringIO
import logging as log
import pandas as pd
log.basicConfig( level=log.INFO)

class InfoTrackTest:

    def write_red_shift(self,input_df):
        host = 'redshift-cluster-1.cspglingyc6w.ap-southeast-2.redshift.amazonaws.com'
        username = 'awsuser'
        password = 'Happyt3ddy'
        port = 5439
        url = "{d}+{driver}://{u}:{p}@{h}:{port}/{db}".\
                    format(d="redshift",
                    driver='psycopg2',
                    u=username,
                    p=password,
                    h=host, 
                    port=port,
                    db='infotrack_test')
        engine = SA.create_engine(url)
        cnn = engine.connect()
        
        df2 = input_df.groupby(['FirstName','MiddleName','LastName','DateOfBirth','PlaceOfBirth'], as_index=False).last()


        # df2['uuid'] = df2.apply(lambda _: uuid.uuid4(), axis=1)

        df2['uuid'] = df2.index.to_series().map(lambda x: uuid.uuid4())        

        log.info("Sending this frame to RedShift:",df2)



        df2.to_sql('test_data', cnn, index=False, if_exists='replace')
        

    def reads3(self):

        bucket_name = 'infotrack-tech-challenge'
        region_name='ap-southeast-2'
        aws_access_key_id='AKIAQQWQQYEAXVFKL4MU'
        aws_secret_access_key='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'

        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        object_key = 'TechTest_Companies.xlsx'
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        data = obj['Body'].read()
        input_df = pd.read_excel(io.BytesIO(data))


        return input_df

def main():
    obj = InfoTrackTest()

    obj.write_red_shift(obj.reads3())

if __name__ == "__main__":
    main()