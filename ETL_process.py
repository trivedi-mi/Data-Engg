import boto3
import json
import base64
import psycopg2
import configparser
import argparse
import sys
from datetime import datetime
import requests
import subprocess

class ETL_Process():
    """Class for performing ETL Process"""

    def __init__(self, endpoint_url, queue_name, wait_time, max_messages):
        """Constructor to get Postgres credentials"""

        # Instantiate the config parser
        config = configparser.ConfigParser()

        # Read the config file
        config.read('postgres.ini')

        # Get config details
        self.__username = config.get('postgres', 'username')
        self.__password = config.get('postgres', 'password')
        self.__host = config.get('postgres', 'host')
        self.__database = config.get('postgres', 'database')

        # Get argument values
        self.__endpoint_url = endpoint_url
        self.__queue_name = queue_name
        self.__wait_time = wait_time
        self.__max_messages = max_messages

        # Return from the constructor
        return

    def base64_encode(self, string_parameter, action = "encode"):
        """Function to encode or decode string using base64"""

        # Check if action is encoding or decoding
        if action == "encode":
            # Encode string to ASCII value
            bytes_to_encode = string_parameter.encode("utf-8")
            # Encode the bytes to base64
            encoded_bytes = base64.b64encode(bytes_to_encode)

# Convert the encoded bytes to a string
            encoded_string = encoded_bytes.decode("utf-8")

            # Return the encoded string
            return encoded_string

        # Else decode the encrypted string
        elif action == "decode":
            # Decode base64 encrypted string
            decoded_string = base64.b64decode(string_parameter).decode('utf-8')

            # Return the decoded string
            return decoded_string

    def get_messages(self):
        command = f"awslocal sqs receive-message --queue-url {self.__endpoint_url}/{self.__queue_name}"

    # Execute the command
   
        output = subprocess.check_output(command, shell=True)
        response = json.loads(output)
        """Function to receive messages from SQS Queue"""

        # Instantiate SQS Client
        #sqs_client = boto3.client("sqs", endpoint_url = self.__endpoint_url)

        # Receive messages from queue
        try:
            # response = sqs_client.receive_message(
            #     QueueUrl= self.__endpoint_url + '/' + self.__queue_name,
            #     MaxNumberOfMessages=self.__max_messages,
            #     WaitTimeSeconds=self.__wait_time
            
            print(response)
            
        except Exception as exceptions:
            # Print error while parsing parameters
            print("Error - " + str(exceptions))

            # Exit from program
            sys.exit()

        # Get messages from SQS
        messages = response['Messages']
        
        # Return the messages
        return messages

    def transform_data(self, messages):
        """Function to transform PII data"""

        # Decalre empty message list
        message_list = []

        try:
            # Check if "messages" list is empty
            if len(messages) == 0:
                # Raise IndexError
                raise IndexError("Message list is empty")
                
        except IndexError as index_error:
            # Print the message is empty
            print("Error - " + str(index_error))

            # Exit from program
            sys.exit()

        # Declare message counter variable
        message_count = 0

        # Iterate through the messages
        for message in messages:
            # Increment the message counter
            message_count += 1

            # Get "Body" of the message into JSON/Dictionary format
            message_body = json.loads(message['Body'])

            # Get "ip" and "device_id" of message
            try:
                ip = message_body['ip']
                device_id = message_body['device_id']
            except Exception as exception:
                # Print message is invalid
                print("Error - Message " + str(message_count) + " is invalid - " + str(exception) + " is not available in queue")

                # Continue to next message
                continue

            # Encode "ip" and "device_id"
            base64_ip = self.base64_encode(ip)
            base64_device_id = self.base64_encode(device_id)

            # Replace "ip" and "device_id" with encoded values
            message_body['ip'] = base64_ip
            message_body['device_id'] = base64_device_id
            
            # Append data to message list
            message_list.append(message_body)

        # Return the message list
        return message_list
    

    def load_data_postgre(self, message_list):
        """Function to load data to postgres"""

        # Check if "message_list" is empty
        try:
            if len(message_list) == 0:
                # Raise Type Error
                raise TypeError
        except TypeError as type_error:
            # Print the "message_list" is empty
            print("Error - " + str(type_error))

            # Exit from program
            sys.exit()

        # Connect to Postgres
        postgres_conn =  psycopg2.connect(
                        host="localhost",
                        port="5432",
                        database="postgres",
                        user="postgres",
                        password="postgres"
                        )

        # Create a Cursor
        cursor = postgres_conn.cursor()
        def get_first(number):
            dot_index = number.find(".")

# Extract the substring before the dot
            first_number_str = number[:dot_index]

# Convert the extracted substring to an integer
            first_number_int = int(first_number_str)

            return first_number_int

        

        # Iterate through messages
        for message_json in message_list:
            # Replaced 'None Type' values with 'None' string
            message_json['locale'] = 'None' if message_json['locale'] == None else message_json['locale']
            # Set 'create_date' field as current date
            message_json['create_date'] = datetime.now().strftime("%Y-%m-%d")
            app_v=message_json['app_version']
            message_json['app_version']=get_first(app_v)
           

            # Convert dictionary values to list
            values = list(message_json.values())
            print(values)

            # Execute the insert query
            cursor.execute("INSERT INTO user_logins ( \
                user_id, \
                app_version, \
                device_type, \
                masked_ip, \
                locale, \
                masked_device_id, \
                create_date \
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)", values)

            # Commit data to Postgres
            postgres_conn.commit()

        # Close connection to Postgres
        postgres_conn.close()

        # Return from the function
        return


def main():
    """The Main Function"""

    # Instantiate the argparser
    parser = argparse.ArgumentParser(
        prog = "Extract Transform Load - Process",
        description = "Program extracts data from SQS queue - \
                       Transforms PIIs in the data - \
                       Loads the processed data into Postgres",
        epilog = "Please raise an issue for code modifications"
    )

    # Add arguments
    parser.add_argument('-e', '--endpoint-url', required = True ,help = "Pass the endpoint URL here")
    parser.add_argument('-q', '--queue-name', required = True ,help = "Pass the queue URL here")
    parser.add_argument('-t', '--wait-time', type = int, default = 10, help = "Pass the wait time here")
    parser.add_argument('-m', '--max-messages', type = int, default = 10, help = "Pass the max messages to be pulled from SQS queue here")

    # Parse the arguments
    args = vars(parser.parse_args())

    # Get value for each argument
    endpoint_url = args['endpoint_url']
    queue_name = args['queue_name']
    wait_time = args['wait_time']
    max_messages = args['max_messages']

    # Invoke an object for the class
    etl_process_object = ETL_Process(endpoint_url, queue_name, wait_time, max_messages)

    # Extract messages from SQS Queue
    print("Fetching messages from SQS Queue...")
    messages = etl_process_object.get_messages()
    print(messages)

    # Transform IIPs from the messages
    print("Masking PIIs from the messages...")
    message_list = etl_process_object.transform_data(messages)

    # Load data to Postgres
    print("Loading messages to Postgres...")
    etl_process_object.load_data_postgre(message_list)

    # Return from the main function
    return


# Calling the main function
if __name__ == "__main__":
    main()