import argparse
import configparser
import json
import csv
import os.path

from datetime import datetime
from time import sleep

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromiumService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from selenium.webdriver.common.action_chains import ActionChains  # for mouse over

import redis


def searcher(search_keyword, save_to_redis, redis_param):
    start_time = datetime.now()

    # webdriver initial
    driver = webdriver.Chrome(service=ChromiumService(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()))
    driver.implicitly_wait(15)

    # go to main page
    driver.get("https://www.profesia.sk/")
    title = driver.title
    assert title == "PROFESIA.SK | Práca, zamestnanie, ponuka práce, brigády, voľné pracovné miesta"

    # cookieee Decline
    cookiebot = driver.find_element(by=By.ID, value="CybotCookiebotDialogBodyButtonDecline")
    cookiebot.click()

    # typeing search phrase and clic on search button
    offer_search_link = driver.find_element(by=By.ID, value="offer-search-link")
    search_tag_box = driver.find_element(by=By.ID, value="offerCriteriaSuggesterInputId")

    search_tag_box.send_keys(search_keyword + "\n")
    offer_search_link.click()

    # checking for results
    message = driver.find_elements(by=By.CLASS_NAME, value="col-xs-8")
    if message != []:
        pass
    else:
        print("There were no results found for '{}'".format(search_keyword))
        driver.quit()
        exit()

    # Checking for locatiun
    message = driver.find_element(by=By.CLASS_NAME, value="col-xs-8")
    value = message.text
    expected = "Ponuky práce"
    assert value[:12] == expected.upper(), f'{value} {expected.upper()}'

    host = redis_param["host"]
    port = redis_param["port"]
    db = redis_param["db"]
    r = None

    if save_to_redis:
        pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
        r = redis.Redis(connection_pool=pool)
        # create or append search words
        r.sadd("search_words", search_keyword)

    # start crawling
    stop_iter = True
    while stop_iter:

        # hold on to the element so that the JS works on the page
        cennik_btn = driver.find_element(by=By.XPATH, value="//a[@title='Cenník']")
        a = ActionChains(driver)
        a.scroll_to_element(cennik_btn).pause(1).perform()

        # get an element with a list of vacancies
        job_rows_active = driver.find_elements(by=By.XPATH, value="//main[@class='col-sm-6']//li[@class='list-row']")
        for i in job_rows_active:
            job_title = i.find_element(by=By.XPATH, value="h2").text
            offer_id = i.find_element(by=By.XPATH, value="h2/a").get_attribute('id')
            offer_link = i.find_element(by=By.XPATH, value="h2/a").get_attribute('href')
            employer = i.find_element(by=By.XPATH, value="span[@class='employer']").text
            job_location = i.find_element(by=By.XPATH, value="span[@class='job-location']").text
            salary = None
            salary_block_state = i.find_elements(by=By.XPATH, value="span[@class='label-group']")
            if salary_block_state != []:
                salary_block = i.find_element(by=By.XPATH, value="span[@class='label-group']")
                salary_state = salary_block.find_elements(by=By.XPATH, value="a[@data-dimension7]")
                if salary_state != []:
                    salary = salary_block.find_element(by=By.XPATH, value="a[@data-dimension7]").text
            info = i.find_element(by=By.XPATH, value="//span[@class='info']").text
            info = info.split()
            data_line = {
                "offer_id": offer_id,
                "offer_link": offer_link,
                "employer": employer,
                "job_title": job_title,
                "job_location": job_location,
                "salary": salary,
                "info": info
            }

            if save_to_redis:
                if r.sadd(search_keyword, data_line["offer_id"]):
                    r.set(data_line["offer_id"], json.dumps(data_line))
                #  checking that you already have the data
                else:
                    if info[0] == "Aktualizované":
                        pass
                    else:
                        stop_iter = False
                        break
            else:
                # File to save data
                file_name = str(start_time.date()) + " " + search_keyword + ".csv"
                field_names = [
                    "offer_id",
                    "offer_link",
                    "employer",
                    "job_title",
                    "job_location",
                    "salary",
                    "info"
                ]
                if not os.path.isfile(file_name):
                    with open(file_name, "w", newline='') as csv_file:
                        writer = csv.DictWriter(csv_file, fieldnames=field_names)
                        writer.writeheader()
                with open(file_name, "a", newline='') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=field_names)
                    writer.writerow(data_line)

        # touch navigation buttons
        b = ActionChains(driver)
        navigation_keys = driver.find_elements(by=By.XPATH, value='//ul[@class="pagination"]//a')
        for i in navigation_keys:
            b.move_to_element(i).pause(0).perform()
            a.scroll_to_element(cennik_btn).pause(0).perform()

        # check if this is the last page or not
        key_next_check = driver.find_elements(by=By.XPATH, value="//a[@class='next']")
        if key_next_check != []:
            key_next = driver.find_element(by=By.XPATH, value="//a[@class='next']")
            b.move_to_element(key_next).pause(2).click().perform()
        else:
            break

    driver.quit()


def main():
    print("""
        #########################################
        #          WEBSITE: PROFESIA.SK         #
        ######################################### 
        """)
    parser = argparse.ArgumentParser(description='profesia.sk crawler')
    parser.add_argument(
        "-r",
        action='store_true',
        help="writing to redis, not to a file"
    )
    parser.add_argument(
        "-a",
        action='store_true',
        help="do not stop work after the first loop, refresh the page waiting for new data"
    )
    parser.add_argument(
        "-p",
        nargs="?",
        default="",
        help="search keyword, try 'python' or 'selenium'",
    )
    args = parser.parse_args()

    # check redis config
    redis_param = {
        "host": "",
        "port": "",
        "db": ""
    }
    if args.r:
        path = "settings.ini"
        config = configparser.ConfigParser()
        if not os.path.isfile(path):
            print("{} file not found".format(path))
            config.add_section("redis_config")
            config.set("redis_config", "host", "localhost")
            config.set("redis_config", "port", "6379")
            config.set("redis_config", "db", "0")
            print("creating...")
            with open(path, "w") as config_file:
                config.write(config_file)
            print("please check your connection {} file and restart the program".format(path))
            exit()
        else:
            config.read(path)
            redis_param["host"] = config.get("redis_config", "host")
            redis_param["port"] = config.get("redis_config", "port")
            redis_param["db"] = config.get("redis_config", "db")

    # other parameters
    if args.a is False and args.r is False and args.p == "":
        print("no arguments no problem, try -h")

    #  start the process
    while True:
        searcher(search_keyword=args.p, save_to_redis=args.r, redis_param=redis_param)

        if args.a:
            sleep(6)
            continue
        else:
            break


if __name__ == "__main__":
    main()
