import os, sys, json, math
from send_data import send_data
from add_more_calculations import add_more_calculations
from add_lat_long import add_lat_long
from add_embeddings import add_embeddings
import os, io, logging, boto3, warnings
from dotenv import load_dotenv
import concurrent.futures
from get_city_data import get_city_data

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


from filters.ethiopianProperties_filter import ethiopianProperties_filter
from filters.houseInRwanda_filter import houseInRwanda_filter
from filters.seso_filter import seso_filter
from filters.buyrentkenya_filter import buyrentkenya_filter
from filters.ghanaPropertyCentre_filter import ghanaPropertyCentre_filter
from filters.kenyaPropertyCentre_filter import kenyaPropertyCentre_filter
from filters.prophunt_filter import prophunt_filter
from filters.propertypro_co_ke_filter import propertypro_co_ke_filter
from filters.propertypro_co_ug_filter import propertypro_co_ug_filter
from filters.airbnb_filter import airbnb_filter
from filters.lamudi_filter import lamudi_filter
from filters.nigeriaPropertyCentre_filter import nigeriaPropertyCentre_filter
from filters.mubawab_filter import mubawab_filter
from filters.property24_filter import property24_filter
from filters.property24_co_ke_filter import property24_co_ke_filter
from filters.propertypro_co_zw_filter import propertypro_co_zw_filter
from filters.propertypro_ng_filter import propertypro_ng_filter
from filters.real_estate_tanzania_filter import real_estate_tanzania_filter
from filters.booking_filter import booking_filter
from filters.global_remax_filter import global_remax_filter
from filters.jiji_co_ke_filter import jiji_co_ke_filter
from filters.jiji_ug_filter import jiji_ug_filter
from filters.jiji_co_tz_filter import jiji_co_tz_filter
from filters.jiji_ng_filter import jiji_ng_filter
from filters.jiji_com_et_filter import jiji_com_et_filter
from filters.jiji_com_gh_filter import jiji_com_gh_filter

# Get the parent directory (project root)
project_root = os.path.dirname(os.path.abspath(__file__))

# Add the project root to the Python path
sys.path.insert(0, project_root)

finalDatabaseName='rehaniAI'
collectionName='properties'
with open(os.path.join(project_root, 'columns.json'), 'r') as json_file:
    column_dict = json.load(json_file)


def process_embeddings(df_chunk):
    return add_embeddings(df_chunk)

def send_data_thread(df_chunk):
    send_data(df_chunk, finalDatabaseName, collectionName, log)

def main():
    try:
        # filters = [
        #     ethiopianProperties_filter, houseInRwanda_filter, seso_filter, buyrentkenya_filter,
        #     ghanaPropertyCentre_filter, kenyaPropertyCentre_filter, prophunt_filter, 
        #     real_estate_tanzania_filter, airbnb_filter, lamudi_filter, 
        #     nigeriaPropertyCentre_filter, mubawab_filter, 
        #     propertypro_co_zw_filter, propertypro_ng_filter, propertypro_co_ke_filter, propertypro_co_ug_filter, 
        #     booking_filter, global_remax_filter, 
        #     jiji_co_ke_filter, jiji_ug_filter, jiji_co_tz_filter, jiji_ng_filter, jiji_com_et_filter, jiji_com_gh_filter
        # ]

        filters = [
            seso_filter, buyrentkenya_filter, kenyaPropertyCentre_filter, propertypro_co_ke_filter, jiji_co_ke_filter, airbnb_filter,
            ethiopianProperties_filter, houseInRwanda_filter, 
            ghanaPropertyCentre_filter, prophunt_filter, 
            real_estate_tanzania_filter, lamudi_filter, 
            nigeriaPropertyCentre_filter, mubawab_filter, 
            propertypro_co_zw_filter, propertypro_ng_filter, propertypro_co_ug_filter, 
            booking_filter, global_remax_filter, 
            jiji_ug_filter, jiji_co_tz_filter, jiji_ng_filter, jiji_com_et_filter, jiji_com_gh_filter
        ]

        # filters = [
        #     propertypro_co_zw_filter, propertypro_co_ug_filter, propertypro_ng_filter, mubawab_filter,
        #     booking_filter, global_remax_filter, 
        #     jiji_ug_filter, jiji_co_tz_filter, jiji_ng_filter, jiji_com_et_filter, jiji_com_gh_filter
        # ]

        chunk_size = 400
        batch_size = 50
        num_threads = 8
        
        for idx, filter_func in enumerate(filters, start=1):
            df = filter_func(log)
            df = df.rename(columns=column_dict)
            df = df.sort_values(by='locationCountry').reset_index(drop=True)
            log.info(f"Starting to process DataFrame {idx} serially!")
            df = add_lat_long(df, log)
            log.info(f"Adding neighborhoods by processing latitude/longitude for DataFrame {idx} serially!")
            df = add_more_calculations(df, log)

            cityGDP = []
            population = []
            populationGrowthRate = []

            for item in df['consolidatedCity'].values:
                city_data = get_city_data(item)

                if city_data:
                    cityGDP.append(city_data.get('gdpPerCapita', None))
                    population.append(city_data.get('population', None))
                    populationGrowthRate.append(city_data.get('populationGrowthRate', None))
                else:
                    cityGDP.append(None)
                    population.append(None)
                    populationGrowthRate.append(None)

            df['City GDP per Capita'] = cityGDP
            df['City Population'] = population
            df['City Population Growth Rate'] = populationGrowthRate

            num_chunks = math.ceil(len(df) / chunk_size)
            chunks_per_thread = math.ceil(num_chunks / num_threads)
            futures = []
            for i in range(0, num_chunks, chunks_per_thread):
                chunk_indices = range(i * chunk_size, min((i + chunks_per_thread) * chunk_size, len(df)))
                chunk = df.iloc[chunk_indices].copy()

                log.info(f"Adding embeddings for records {chunk_indices[0] + 1} to {chunk_indices[-1] + 1}")
                chunk = add_embeddings(chunk)

                with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                    # Divide the chunk into batches of batch_size
                    for j in range(0, len(chunk), batch_size):
                        sub_chunk = chunk.iloc[j:j+batch_size]
                        futures.append(executor.submit(send_data, sub_chunk, finalDatabaseName, collectionName, log))

            # Wait for all futures to complete before moving to the next step
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    log.error(f"An error occurred: {e}")

    except Exception as e:
        log.error(f"Script failed due to an error: {e}", exc_info=True)

    finally:
        try:
            s3 = boto3.client("s3", region_name=aws_region_name)
            s3.put_object(Body=log_stringio.getvalue(), Bucket=bucket_name, Key=f"logs/filtering-script/logs.txt")
            log.info("Log file successfully uploaded to S3.")
        except Exception as s3_error:
            log.error(f"Failed to upload log file to S3: {s3_error}", exc_info=True)


if __name__ == "__main__":
    main()
