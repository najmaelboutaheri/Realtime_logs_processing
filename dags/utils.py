import boto3
import logging
import json 

logger=logging.getLogger(__name__)
def get_secret(secret_name, region_name='eu-north-1'):
    """ This function retrives secrets from aws secret manager """
    session=boto3.session.Session()
    client=session.client(service_name='secretsmanager',region_name=region_name)
    try: 
        response=client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"secret retrieval error:{e}")
        raise