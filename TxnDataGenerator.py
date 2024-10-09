import json
import random
import datetime, time
import dataclasses
import boto3
STREAM_NAME = "txn_in_stream"
REGION_NAME = "us-east-1"

@dataclasses.dataclass
class Transaction:
    account_id: int
    txn_amount: float
    txn_datetime: str

    @classmethod
    def create(cls):
        account_id = random.randint(1000000001, 1000000010)
        txn_amount = round(random.randint(100, 10000) * random.random(), 2)
        txn_datetime = datetime.datetime.now().isoformat()

        return cls(account_id, txn_amount, txn_datetime)

class TransactionGenerator:
    def __init__(self, kinesis_client, stream_name):
        self.kinesis_client = kinesis_client
        self.stream_name = stream_name

    def send_record(self):
        txn = Transaction.create()
        print(txn)
        response = self.kinesis_client.put_record(
            StreamName=self.stream_name,
            Data=json.dumps(txn.__dict__),
            PartitionKey="partitionkey"
        )


if __name__ == "__main__":
    print("Starting Transaction Generator")
    kinesis_client = boto3.client('kinesis', region_name=REGION_NAME)
    txn_generator = TransactionGenerator(kinesis_client, STREAM_NAME)

    while True:
        txn_generator.send_record()
        time.sleep(1)