
import os

 

SECURITY_PROTOCOL="SASL_SSL"    ## This is a security protocol.It allow a client server to communicate with each other securly for eg.suppose kafka is a server and IOT device is a client so client and server established a secure connection so we need to do this for that we use a `SASL` 
SSL_MACHENISM="PLAIN"   ## Ye line PLAIN SASL mechanism set karti hai, jo simple username/password authentication use karta hai.
API_KEY = os.getenv('API_KEY',None)   ##  This retrieves the API_KEY environment variable. If the variable is not set, it defaults to None. This key is used for authentication with the Kafka server.
ENDPOINT_SCHEMA_URL  = os.getenv('ENDPOINT_SCHEMA_URL',None)  ## yaha pe confluent se URL leke paste krna hai
API_SECRET_KEY = os.getenv('API_SECRET_KEY',None)
BOOTSTRAP_SERVER = os.getenv('BOOTSTRAP_SERVER',None)  ##  This retrieves the BOOTSTRAP_SERVER environment variable, which specifies the Kafka server address used to bootstrap the initial connection. If not set, it defaults to None.
# SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL',None)
# SSL_MACHENISM = os.getenv('SSL_MACHENISM',None)
SCHEMA_REGISTRY_API_KEY = os.getenv('SCHEMA_REGISTRY_API_KEY',None)    ## here you have to paste schema registry API_KEY (in the confluent you have to create)
SCHEMA_REGISTRY_API_SECRET = os.getenv('SCHEMA_REGISTRY_API_SECRET',None)   ##   ## here you have to paste schema registry API_SECRET (in the confluent you have to create)


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    print(sasl_conf)
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }

if __name__ == '__main__':
    sasl_conf()

