
import pandas as pd
import json

class Generic:

    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

    @staticmethod
    def dict_to_object(data: dict, ctx):
        print(data, ctx)
        return Generic(record=data)


    def to_dict(self):
        return self.__dict__

    @classmethod
    ## if you just passed the file path it will give you the data 
    ## So `get_object` is a function which is going to read the every record and its returns the prepared record 
    ## Reads the CSV file in chunks of 10 rows using pd.read_csv.
    ## For each chunk, iterates over the rows and converts them to Generic objects.  
    ## i am reading 10 records at a time then i will be preparing a record and returning the records in the 
       # form of yield so that everything will be run directly
       ## What It Does: The get_object method reads a CSV file in small chunks, converts each row into a Generic object, and yields these objects one by one.
    def get_object(cls, file_path):
        chunk_df = pd.read_csv(file_path, chunksize=10)
        n_row = 0
        for df in chunk_df:
            for data in df.values:
                generic = Generic(dict(zip(df.columns, list(map(str,data)))))
                # cars.append(car)
                # print(n_row)
                n_row += 1
                yield generic

    @classmethod
    def export_schema_to_create_confluent_schema(cls, file_path):
        columns = next(pd.read_csv(file_path, chunksize=10)).columns

        schema = dict()
        schema.update({
                    "type": "record",
                    "namespace": "com.mycorp.mynamespace",
                    "name": "sampleRecord",
                    "doc": "Sample schema to help you get started.",
                    })

        fields = []    
        for column in columns:
            fields.append(
                        {
                        "name": f"{column}",
                        "type": "string",
                        "doc": "The string type."  
                        }
            )

        schema.update({"fields":fields})


    
        json.dump(schema,open("schema.json","w"))
        schema = json.dumps(schema)

        print(schema)
        return schema
        

    @classmethod
    ## Generates a JSON schema for producing and consuming data based on the CSV file's columns.
    ## file_path: Path to the CSV file.
    ## Returns: The schema as a JSON string.
    def get_schema_to_produce_consume_data(cls, file_path):
        columns = next(pd.read_csv(file_path, chunksize=10)).columns   ## Read the file with the help of pandas
      ## chunksize=10 means its break the file into 10-10 records
        

      ## i read the file with chunks with 10 records then i get the columns name
      ## Reads the first chunk of the CSV file to get the column names.
      ## Constructs a schema dictionary with $id, $schema, additionalProperties, description, properties, title, and type.
      ## Returns the schema as a JSON string.

        schema = dict()
        schema.update({
            "$id": "http://example.com/myURI.schema.json",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "additionalProperties": False,
            "description": "Sample schema to help you get started.",
            "properties": dict(),
            "title": "SampleRecord",
            "type": "object"})
        for column in columns:
            schema["properties"].update(
                {
                    f"{column}": {
                        "description": f"generic {column} ",  ## this is a description and this is a type so by default to keep simple we decide to kept every column as a string  
                        "type": "string"
                    }
                }
            )
        
    
        schema = json.dumps(schema)

        print(schema)
        return schema
        

    def __str__(self):
        return f"{self.__dict__}"


def instance_to_dict(instance: Generic, ctx):
    return instance.to_dict()
