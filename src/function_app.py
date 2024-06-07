import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient

import logging
import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from modules.movie_scrapping import parallel_scrapping

load_dotenv()


def generate_months(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        if start_date > end_date:
            raise Exception("Start date must before end date")

        date_array = []

        # Iterate through the range of dates in 30-day intervals
        current_date = start_date
        while current_date <= end_date:
            date_array.append(current_date)
            current_date += timedelta(days=30)

        # Add extra current_date if it not reach to end_date
        if current_date - timedelta(days=30) != end_date:
            date_array.append(end_date)

        return date_array
                
    except ValueError as e:
        logging.info(f"Incorrect start date or end date format! Make sure enter Year-Month-Date format")

def generate_urls(start_date, end_date):
    
    base_url_format = r"https://www.imdb.com/search/title/?title_type=feature&release_date={},{}&sort=num_votes,desc&count=250"

    date_range = generate_months(start_date, end_date)
    
    #convert datetime to string type before format base_url_format
    return [base_url_format.format(date_range[idx-1].strftime('%Y-%m-%d'), date_range[idx].strftime('%Y-%m-%d')) for idx in range(1,len(date_range))]

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

app = func.FunctionApp()
@app.route(route="MovieScrapping", auth_level=func.AuthLevel.ANONYMOUS)
def MovieScapping(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    start_date = req.params.get('start_date')
    end_date = req.params.get('end_date')
    nthreads = req.params.get('nthreads')
    limit_each_month = req.params.get('limit_each_month')

    start_date = start_date if start_date is not None else '2023-01-01'
    end_date = end_date if end_date is not None else '2023-03-01'

    nthreads = int(nthreads) if nthreads is not None else 2
    limit_each_month = int(limit_each_month) if limit_each_month is not None else 3

    try:
         
        connection_string = os.getenv("CONNECTION_STRING")
        key=os.getenv("KEY")
        datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string, key)
        file_system_client = datalake_service_client.get_file_system_client("bronze")
        directory_client = file_system_client.get_directory_client("bronze")

        start_time = time.time()

        create_folder('./scraped_data')
        urls_base_on_date = generate_urls(start_date, end_date)
        parallel_scrapping(context, urls_base_on_date, directory_client, limit_each_month, nthreads, 250)
        end_time = time.time()
        elapsed_time = round(end_time - start_time)
               
        response_mess=f"This HTTP triggered scrapping function executed successfully, it took total {elapsed_time}s to scrape movies from {start_date} to {end_date} with {nthreads} threads\n \
                            Each month contains {limit_each_month} movies"

        return func.HttpResponse(response_mess, status_code=200)
    
    except Exception as e:
        response_mess=f"Error happend in the scrapping process: {e}"
        logging.info(response_mess)
        return func.HttpResponse(response_mess,status_code=500)