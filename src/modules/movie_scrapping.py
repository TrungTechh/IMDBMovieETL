import re
from bs4 import BeautifulSoup
# import numpy as np
from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC

from typing import List

from threading import Thread
import logging
import json
from datetime import datetime, timedelta


def get_current_time_str():
    """
    Get the current time in string type
    """
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%d_%m_%Y_%H_%M")
    return formatted_datetime

def get_start_and_end_date(url):
    """
    Get the start_date and end_date from the url using regex
    :param url: the url string
    """
    match = re.search(r'release_date=(\d{4}-\d{2}-\d{2}),(\d{4}-\d{2}-\d{2})', url)
    start_date = match.group(1) 
    end_date = match.group(2) 

    return start_date, end_date

def get_chrome_driver():
    """
    Get the start_date and end_date from the url using regex
    :param url: the url string
    """
    logging.info("Initializing chrome driver")

    try:
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.198 Safari/537.36 Edg/95.0.1020.30"

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument(f"user-agent={user_agent}")

        driver = webdriver.Chrome(options=chrome_options)
        logging.info("Chrome driver initialized successfully")
        return driver
    except:
        logging.error("Error initializing Chrome driver")

def generate_chrome_drivers(nchromes):
    """Generate multiple chrome drivers for multi threads
    :param nchromes: the number of chrome drivers to generate
    """
    chrome_drivers = []

    for _ in range(nchromes):
        driver = get_chrome_driver()
        chrome_drivers.append(driver)

    return chrome_drivers

def get_page_movie_source(driver, href_url):
    """
    Get source html of a movie
    :param driver: the selenium driver
    :param href_url: url of the movie
    """
    driver.get(href_url)
    WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-testid='shoveler-items-container']")))
    source = driver.page_source

    return source

def scrape_movie(driver, movie_tag):
    """
    Scrape information of a movie base on its tag
    :param driver: the selenium driver
    :param movie_tag: the div tag of movie in landing page
    """
    try:
        # =================
        # = SET UP DRIVER =
        # =================
        hrefMovie = movie_tag.find('div',class_ = ['ipc-title', 'ipc-title--base'])
        hrefMovie = hrefMovie.find('a')['href']
        href_link = 'https://www.imdb.com/' + str(hrefMovie)

        href_source = get_page_movie_source(driver, href_link)
        href_soup = BeautifulSoup(href_source, "html.parser")

        # ===============================================================
        # = name, release_date, countries, language, locations, company =
        # ===============================================================
        name_tag = movie_tag.find('h3')
        name = name_tag.get_text().split(' ')[-1]

        details = href_soup.select('div[data-testid="title-details-section"] a[class="ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"]')
        release_date = ''
        countries = []
        language = []
        locations = []
        company = []
        if details:
            for d in details:
                if 'releaseinfo' in d['href']:
                    release_date = d.text
                elif 'country_of_origin' in d['href']:
                    countries.append(d.text)
                elif 'primary_language' in d['href']:
                    language.append(d.text)
                elif 'locations' in d['href']:
                    locations.append(d.text)
                elif 'company' in d['href']:
                    company.append(d.text)
        countries = ', '.join(countries)
        language = ', '.join(language)
        company = ', '.join(company)
        locations = ', '.join(locations)
        
        # ===================
        # = GENRE ATTRIBUTE =
        # ===================
        genre_tag = href_soup.find_all('div', class_="ipc-chip-list__scroller")
        genre = []
        if genre_tag[0]:
            for tag in genre_tag[0].contents:
                genre.append(tag.get_text())
        else:
            genre = ""
        genre = ', '.join(genre)


        # =============================
        # = IMDB SCORE AND VOTE COUNT =
        # =============================
        imdb_score_tag = href_soup.find('div', attrs={"data-testid" : 'hero-rating-bar__aggregate-rating'}) \
                            .find_all('span')
        imdb_score = None
        if imdb_score_tag:
            imdb_score = imdb_score_tag[1].get_text()

        imdb_rate_tag = movie_tag.find('span', class_=['ipc-rating-star--base', 'ipc-rating-star--imdb'])
        imdb_rate_string = imdb_rate_tag.get_text()

        vote_count_pattern = r'\(([^)]+)\)'
        vote_count = ""
        if re.search(vote_count_pattern, imdb_rate_string):
            vote_count = re.search(vote_count_pattern, imdb_rate_string).group()[1:-1]
        

        # ===========================================
        # = RUNTIME ATTRIBUTE =
        # ===========================================
        runtime_tag = href_soup.find('li', attrs={"data-testid" : 'title-techspec_runtime'}).find('div')

        runtime = None
        if runtime_tag:
            runtime = runtime_tag.get_text().split('<!-- -->')[0]

        
        # ========================================
        # = WRITER, DIRECTOR, STARS, CERTIFICATE =
        # ========================================
        general_section_tag = href_soup.find('section', attrs={"class" : ['ipc-page-section--baseAlt']})

        div_tag_contain_generals = list(general_section_tag.children)[1]
        li_tag_contain_details = div_tag_contain_generals.find_all("li")
        certificate = li_tag_contain_details[1].get_text()

        div_tag_contain_details = list(general_section_tag.children)[2]
        ul_tag_contain_details = div_tag_contain_details.find('ul', class_ = ['ipc-metadata-list'])
        li_tags_list_details = list(ul_tag_contain_details.children)
        director = li_tags_list_details[0].find('a').get_text()
        writter = li_tags_list_details[1].find('a').get_text()
        stars = ", ".join([a_tag.get_text() for a_tag in li_tags_list_details[2].find('div', recursive = False).find_all('a')])


        # ====================
        # = BUDGET AND GROSS =
        # ====================
        box_office_div_tag = href_soup.find('section', attrs={"data-testid" : ['BoxOffice']}) \
                                    .find('div', attrs={"data-testid" : ['title-boxoffice-section']})
        box_office_span_texts = [tag for tag in box_office_div_tag.find_all('span') if tag is not None]
        budget = box_office_span_texts[1].get_text()
        gross_global = box_office_span_texts[-1].get_text()
        
        movie_infor = {
            "name": name , 
            "release_date": release_date, 
            "genres": genre, 
            "certificate": certificate, 
            "runtime": runtime, "imdb_score": imdb_score, "director": director, "writter": writter, 
            "stars": stars, 
            "budget": budget, "gross_global": gross_global, 
            "countries": countries,"language": language, "locations": locations, "company": company,
            "url": href_link
        }

        return movie_infor
    
    except Exception as e:
        logging.info("An error occur when scape a movie", e)
    
def scrape_movies_for_a_month(driver, url, limit_each_month, directory_client, num_movies_each_expanse=250):
    """
    Scrape information of all movies in a month
    """
    logging.info(f"The process's scrapping {url}...")
    movies_of_this_month = []
    
    try:
        driver.get(url)

        # **************************************************************************
        # * Because imdb prevent first, we only get the result as of the second    *
        # * ===> Create a fake click                                               *
        # * ===> Click expanse until reach the limit                               *
        # **************************************************************************
        total_movies = num_movies_each_expanse #track total number of movies in current page
        if limit_each_month > num_movies_each_expanse:
            view_more_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '.ipc-see-more__button'))
            )

            # Fake click
            try:
                view_more_button.click()
            except:
                pass
            
            #click until reach the limit
            while total_movies < limit_each_month:
                try:
                    view_more_button.click()
                except:
                    total_movies = -1
                    break
                total_movies += num_movies_each_expanse
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        movie_div_list = soup.find_all('div', class_ = 'ipc-metadata-list-summary-item__c')
        
        if limit_each_month < len(movie_div_list):
            movie_div_list = movie_div_list[:limit_each_month-1]

        for movie_div in movie_div_list:
            movies_of_this_month.append(scrape_movie(driver, movie_div))

        start_date, end_date = get_start_and_end_date(url)
        file_name="{},{}_{}.json".format(start_date, end_date, get_current_time_str())
        file_content=json.dumps(movies_of_this_month, ensure_ascii=False, indent=4)

        # with open(file_name, 'w',encoding='utf-8') as json_file:
        #     json_file.write(file_content)

        if directory_client is not None:
            file_client = directory_client.create_file(file_name)

            file_client.upload_data(file_content, overwrite=True)

    except Exception as e:
        logging.info(f"An error occur when scrap for a month: {e}")

    return movies_of_this_month

def scrape_movies_in_date_range(context, urls, driver, limit_each_month, num_movies_each_expanse, movies, directory_client):
    context.thread_local_storage.invocation_id = context.invocation_id
    for url in urls:
        movies_list = scrape_movies_for_a_month(driver, url, limit_each_month, directory_client, num_movies_each_expanse)
        if len(movies_list) > 0:
            movies.extend(movies_list)
    logging.info("movies len: " + str(len(movies)))
    return movies

def parallel_scrapping(context, urls, directory_client, limit_each_month=500,nthreads=2, num_movies_each_expanse=250):
    movies = []
    threads = []
    chrome_drivers = generate_chrome_drivers(nthreads)
    for idx, chrome_driver in enumerate(chrome_drivers):
        urls_each_thread = urls[idx::nthreads]
        t = Thread(target=scrape_movies_in_date_range, args=(context, urls_each_thread,chrome_driver,limit_each_month, num_movies_each_expanse, movies, directory_client,))
        threads.append(t)
        t.start()

    # wait for the threads to finish
    for thread in threads:
        logging.info("Thread is processing")
        thread.join()

    # with open(f"reconciled_data/{get_current_time_str()}_reconciled_properies.json", 'w',encoding='utf-8') as json_file:
    #     json.dump(store, json_file, ensure_ascii=False, indent=4)

    for cd in chrome_drivers:
        cd.quit()
