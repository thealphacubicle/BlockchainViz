import json
import pymongo
import requests

class ETLPipeline:
    def __init__(self):
        self.API_URL = 'https://api.blockchair.com/bitcoin/dashboards/blocks/{}'

    def extract_batch_data(self, block_numbers):
        """
        Get block data from the API
        :param block_numbers: List of block numbers to get data for
        :return: Block data as a dictionary
        """
        block_num_str = ",".join([str(i) for i in block_numbers])
        url = self.API_URL.format(block_num_str)

        try:
            # Get batch data from the API
            response = requests.get(url).json()['data']

            # Extract singular block data from the response -> lazy execution to avoid capacity issues
            for block_num in response:

                # Error handling for missing block data
                if "block" in response[block_num]:
                    yield response[block_num]['block']

                else:
                    # TODO: Log missing block data
                    print(f"Block {block_num} data is missing")
                    continue

        except Exception as e:
            print(e)
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
            return 500

    # TODO: Implement the store_batch_data method to store the data in the database
    def store_batch_data(self, data):
        """
        Store the data in the database
        :param data: Data to store
        :return: Status of the operation
        """
        try:
            # Insert data to the database
            pass

            return 200

        except Exception as e:
            return 500

    def run_pipeline(self):
        """
        Run the ETL pipeline
        :return: Status code of the operation
        """
        blocks = [200000, 200001, 200002, 200003]
        MAX_BATCH_SIZE = 1440  # 1 day worth of blocks

        try:
            pipeline = ETLPipeline()

            # Create query to get block data
            gen = pipeline.extract_batch_data(blocks)

            # Process the block data singularly and store in batches
            batch_data = []
            for block_data in gen:
                data = pipeline.process_block(block_data)
                batch_data.append(data)

                # If DB batch size is reached, store the data
                if len(batch_data) >= MAX_BATCH_SIZE:
                    # TODO: Insert batch_data to the database
                    pipeline.store_batch_data(batch_data)

                    # Reset for the next batch
                    batch_data = []

            # If there are any remaining data, store it in the database
            if batch_data:
                pipeline.store_batch_data(batch_data)

            return 200

        except Exception as e:
            print(e)
            return 500






## Main function
def main():
    pipeline = ETLPipeline()
    pipeline.run_pipeline()

if __name__ == '__main__':
    main()