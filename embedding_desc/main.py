import os
from get_data import get_data
from evaluate_description import evaluate_description
from add_embeddings import add_embeddings
from send_data import send_data
import os, io, logging, boto3, warnings
from dotenv import load_dotenv


warnings.filterwarnings("ignore", category=DeprecationWarning) 
load_dotenv(override=True)

aws_region_name = os.getenv("aws_region_name")
bucket_name = os.getenv("bucket_name")

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("filtering-script")
log_stringio = io.StringIO()
handler = logging.StreamHandler(log_stringio)
handler.setFormatter(formatter)
log.addHandler(handler)

databaseName='rehaniAI'
collectionName='properties'

def main():
    print("Getting data from database!")
    df = get_data()
    print("Length of DataFrame:", len(df))
    df = evaluate_description(df)
    df = add_embeddings(df)

    send_data(df, databaseName, collectionName, log)
    s3 = boto3.client("s3", region_name=aws_region_name)
    s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key=f"logs/embedding-script/logs.txt")

if __name__ == "__main__":
    main()
