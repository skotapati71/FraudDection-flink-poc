import os
import json
from pyflink.table import EnvironmentSettings, TableEnvironment

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda

# 1. Creates a Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

is_local = {
    True if os.environ.get("IS_LOCAL") else False
}

if is_local:
    # only for local, overwrite variable to properties and pass in your jars delimited by a semicolon (;)
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"  # local

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///" + CURRENT_DIR + "/lib/flink-sql-connector-kinesis-4.3.0-1.19.jar",
    )

def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))

def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]

# ,WATERMARK FOR txn_datetime AS txn_datetime - INTERVAL '5' SECOND
def create_source_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                account_id INT,
                txn_amount DOUBLE,
                txn_datetime TIMESTAMP(3),
                proctime AS PROCTIME(),
                WATERMARK FOR txn_datetime AS txn_datetime - INTERVAL 5 SECOND  
              )
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(
        table_name, stream_name, region, stream_initpos
    )

def create_sink_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                account_id INT,
                txn_amount DOUBLE,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3)
              ) WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(
        table_name, stream_name, region, stream_initpos
    )

def create_print_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                account_id INT,
                txn_amount DOUBLE,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3)
              ) WITH (
                'connector' = 'print'
              ) """.format(
        table_name, stream_name, region, stream_initpos
    )

def main():
    # Application Property Keys
    input_property_group_key = "consumer.config.0"
    producer_property_group_key = "producer.config.0"

    input_stream_key = "input.stream.name"
    input_region_key = "aws.region"
    input_starting_position_key = "flink.stream.initpos"

    output_stream_key = "output.stream.name"
    output_region_key = "aws.region"

    # tables
    input_table_name = "input_table"
    output_table_name = "output_table"

    # get application properties
    props = get_application_properties()

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, producer_property_group_key)

    input_stream = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]
    stream_initpos = input_property_map[input_starting_position_key]

    output_stream = output_property_map[output_stream_key]
    output_region = output_property_map[output_region_key]

    # 2. Creates a source table from a Kinesis Data Stream
    table_env.execute_sql(
        create_source_table(input_table_name, input_stream, input_region, stream_initpos)
    )

    # 3. Creates a sink table writing to a Kinesis Data Stream
    table_env.execute_sql(
        create_print_table(output_table_name, output_stream, output_region, stream_initpos)
    )

    #
    # table_env.execute_sql(
    #     create_print_table(output_table_name, output_stream, output_region, stream_initpos)
    # )

    # process tumbling window aggregate if total txn amt > 500
    query = """
            SELECT account_id, SUM(txn_amount) AS total_txn_amt, 
            TUMBLE_START(proctime, INTERVAL '15' SECONDS) AS window_start,
            TUMBLE_END(proctime, INTERVAL '15' SECONDS) AS window_end
            FROM {0}
            GROUP BY account_id, TUMBLE(proctime, INTERVAL '15' SECONDS)
            HAVING SUM(txn_amount) > 3000
            """.format(input_table_name)

    flagged_accounts = table_env.sql_query(query)
    #print(flagged_accounts)

    table_result = flagged_accounts.execute_insert("output_table")

    if is_local:
        table_result.wait()
    else:
        # get job status through TableResult
        print(table_result.get_job_client().get_job_status())

if __name__ == "__main__":
    main()
