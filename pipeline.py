from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import os

from dotenv import load_dotenv

'''
TODO:
- Implement error handling for dealing with exceptions thrown for all methods
- Look into asynchronous programming for the extract_batch_data method to avoid capacity issues
'''


class ETLPipeline:
    def __init__(self):
        self.API_URL = 'https://api.blockchair.com/bitcoin/dashboards/blocks/{}'
        load_dotenv('keys.env')
        self.MONGO_USERNAME, self.MONGO_PWD = os.environ.get('USERNAME'), os.environ.get('PASSWORD')
        self.MONGO_URI = (f"mongodb+srv://{self.MONGO_USERNAME}:{self.MONGO_PWD}@blockchainviz.xvegyms.mongodb.net"
                          f"/?retryWrites=true&w=majority&appName=BlockchainViz")
        self.total_request_cost = 0.0
        self.total_batches_processed = 0
        self.total_blocks_processed = 0

    def _create_batch_query(self, block_numbers, batch_size=10):
        """
        Create a batch query for the API
        :param block_numbers: List of block numbers to get data for
        :param batch_size: Size of the batch
        :return: Batch query
        """
        # Create a generator to return the data in batches
        for i in range(0, len(block_numbers), batch_size):
            yield block_numbers[i:i + batch_size]

    def _get_request_cost(self, api_response):
        """
        Get the cost of the request from the API response
        :param api_response: API response as a JSON
        :return float: Cost of the API request
        """
        return float(api_response['context']['request_cost'])

    def extract_batch_data(self, block_numbers):
        """
        Get block data from the API
        :param block_numbers: List of block numbers to get data for
        :return: Block data as a dictionary
        """
        for batch_query in self._create_batch_query(block_numbers):
            try:
                # Format the batch query into API string
                block_num_str = ",".join([str(i) for i in batch_query])
                url = self.API_URL.format(block_num_str)

                # Get batch data from the API
                response = requests.get(url).json()
                self.total_request_cost += self._get_request_cost(response)
                block_response = response['data']

                # Extract singular block data from the response -> lazy execution to avoid capacity issues
                for block_num in block_response:

                    # Error handling for missing block data
                    if "block" in block_response[block_num]:
                        yield block_response[block_num]['block']

                    else:
                        # TODO: Log missing block data
                        print(f"Block {block_num} data is missing")
                        continue

                    self.total_blocks_processed += 1

            # Error handling if API request doesn't go through
            except Exception as e:
                print('Batch request failed for blocks: ', batch_query)
                continue

            self.total_batches_processed += 1

    def _print_process_info(self):
        """
        Print details of the process completed
        :return: None
        """
        print("\n")
        print("---------------- Process Report ----------------")
        print("\n")
        print("Total Request Cost: ", self.total_request_cost)
        print("Batches Processed: ", self.total_batches_processed)
        print("Total Blocks Processed: ", self.total_blocks_processed)
        print("\n")
        print("------------------------------------------------")

    def _create_db_connection(self):
        """
        Create a connection to the MongoDB database
        :return: Connection object
        """
        try:
            client = MongoClient(self.MONGO_URI, server_api=ServerApi('1'))

            # Send a ping to confirm a successful connection
            try:
                client.admin.command('ping')
                print("Pinged your deployment. You successfully connected to MongoDB!")
            except Exception as e:
                raise e

            return client

        except Exception as e:
            raise e
            return 500

    def process_block(self, block_data):
        """
        Process the item and save it to the database
        :param block_data: Block data to process
        :return: Status of the operation
        """
        try:
            # Create custom data object with processed data
            data = {
                "id": block_data['id'],
                "timestamp": block_data['time'],
                "number": int(block_data['id']),
                "size": int(block_data['size']),
                "difficulty": int(block_data['difficulty']),
                "bits": int(block_data['bits']),
                "transaction_count": int(block_data['transaction_count']),
                "input_usd": float(block_data['input_total_usd']),
                "output_usd": float(block_data['output_total_usd']),
                "fee_usd": float(block_data['fee_total_usd']),
                "fee_perkb_usd": float(block_data['fee_per_kb_usd']),
                "reward_usd": float(block_data['reward_usd'])
            }
            return data

        except Exception as e:
            raise e
            return 500

    # TODO: Implement the store_batch_data method to store the data in the database
    def store_batch_data(self, data):
        """
        Store the data in the database
        :param data: Data to store -> should be list of dictionaries
        :return: Status of the operation
        """
        # Make sure the data is a list
        if not isinstance(data, list):
            return 500

        try:
            # Insert data to the database
            self._create_db_connection()['BlockchainViz']['block_data'].insert_many(data)

            return 200

        except Exception as e:
            raise e
            return 500

    def run_pipeline(self, block_start, block_end):
        """
        Run the ETL pipeline
        :param block_start: Start block number
        :param block_end: End block number (exclusive)
        :return: Status code of the operation
        """
        blocks = [i for i in range(block_start, block_end)]
        MAX_REQUEST_SIZE = 1440

        print("Starting the ETL pipeline...")
        try:
            pipeline = ETLPipeline()

            # Check if the total cost of the request is within the limit
            if pipeline.total_request_cost > MAX_REQUEST_SIZE:
                return 500

            # Process the block data singularly and store in batches
            else:
                # Create query to get block data
                gen = pipeline.extract_batch_data(blocks)

                batch_data = []
                for block_data in gen:
                    data = pipeline.process_block(block_data)
                    batch_data.append(data)

                    # If DB batch size is reached, store the data
                    if len(batch_data) >= MAX_REQUEST_SIZE:
                        # Insert batch_data to the database
                        pipeline.store_batch_data(batch_data)

                        # Reset for the next batch
                        batch_data = []

                # If there are any remaining data, store it in the database
                if batch_data:
                    pipeline.store_batch_data(batch_data)

                # Print process info
                pipeline._print_process_info()

                return 200

        except Exception as e:
            raise e
            return 500


def main():
    pipeline = ETLPipeline()
    pipeline.run_pipeline(500481, 500581)


if __name__ == '__main__':
    main()
