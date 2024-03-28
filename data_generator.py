from faker import Faker
import random
import os
import time
import argparse
import pandas as pd

class DataGenerator:
    def __init__(self, schema):
        self.faker = Faker()
        self.schema = schema

    def generate_record(self):
        if self.schema == '1':
            return {
                "id": self.faker.uuid4(),
                "name": self.faker.name(),
                "age": random.randint(8, 100),
                "email": self.faker.email(),
                "amount": random.uniform(10, 1000)
            }
        elif self.schema == '2':
            return {
                "id": self.faker.uuid4(),
                "name": self.faker.name(),
                "age": random.randint(8, 100),
                "email": self.faker.email(),
                "zipcode": self.faker.zipcode(),
                "timestamp": self.faker.date_time()
            }
        else:
            return {
                "id": self.faker.uuid4(),
                "product_name": random.choice(["A", "B", "C", "D", "E", "F"]),
                "quantity": random.randint(1, 100),
                "price": random.uniform(0.1, 100),
                "date_ordered": self.faker.date_time()
            }

class FileManager:
    @staticmethod
    def get_file_size(file_path):
        if os.path.exists(file_path):
            return os.path.getsize(file_path)
        return 0

    @staticmethod
    def generate_output_path(original_output_path):
        filename, _ = os.path.splitext(original_output_path)
        return f"{filename}_{time.strftime('%Y%m%d%H%M%S')}"

class DataWriter:
    @staticmethod
    def write_to_csv(data, output_path):
        data.to_csv(output_path, mode='a', index=False, header=not os.path.exists(output_path))

    @staticmethod
    def write_to_json(data, output_path):
        data.to_json(output_path, orient='records', lines=True, mode='a', date_format='iso')

    @staticmethod
    def write_to_parquet(data, output_path):
        data.to_parquet(output_path, index=False, mode='append')

class Configuration:
    def __init__(self, args):
        self.num_records = args.num_records
        self.schema = args.schema
        self.output_path = args.output_path
        self.output_format = args.output_format
        self.max_file_size_mb = args.max_file_size_mb

    def should_rotate_file(self, total_file_size):
        return total_file_size >= self.max_file_size_mb * 1024 * 1024

def generate_data(config):
    try:
        while True:
            data = []
            total_file_size = FileManager.get_file_size(config.output_path)
            data_generator = DataGenerator(config.schema)

            for _ in range(config.num_records):
                record = data_generator.generate_record()
                data.append(record)

            dataframe = pd.DataFrame(data)
            if config.output_format == 'csv':
                if config.should_rotate_file(total_file_size):
                    config.output_path = FileManager.generate_output_path(config.output_path) + '.csv'
                DataWriter.write_to_csv(dataframe, config.output_path)

            elif config.output_format == 'json':
                if config.should_rotate_file(total_file_size):
                    config.output_path = FileManager.generate_output_path(config.output_path) + '.json'
                DataWriter.write_to_json(dataframe, config.output_path)

            elif config.output_format == 'parquet':
                if config.should_rotate_file(total_file_size):
                    config.output_path = FileManager.generate_output_path(config.output_path) + '.parquet'
                DataWriter.write_to_parquet(dataframe, config.output_path)

            time.sleep(5)  # Sleep for 5 seconds between iterations

    except KeyboardInterrupt:
        print("KeyboardInterrup: Stopping the data generation.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate the data and save it to a file")
    parser.add_argument('--num_records', type=int, default=1000, help="Maximum number of records to be generated")
    parser.add_argument('--schema', type=str, default='1', choices=['1', '2', '3'], help="Schema format in which the records needs to be generated.")
    parser.add_argument('--output_path', type=str, default='output', help="Path to save the output file")
    parser.add_argument('--output_format', type=str, default='csv', choices=['csv', 'json', 'parquet'], help="output file format in which the file needs to be stored")
    parser.add_argument('--max_file_size_mb', type=int, default=1, help="Maximum size of the file")

    args = parser.parse_args()
    config = Configuration(args)
    generate_data(config)
