import os
import time
import asyncio

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures.thread import ThreadPoolExecutor

s = 0
executor = ThreadPoolExecutor(10)


def scrape(url, *, loop):
    loop.run_in_executor(executor, scraper, url)


def scraper(url):
    options = Options()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", os.getcwd())
    driver = webdriver.Firefox(options=options)
    driver.get(url)

    first_page_ch = [0, 4, 6, 7, 12]
    second_page_ch = [0, 0, 0, 0, 0, 0]
    third_page_ch = [17]
    fourth_page_ch = [0, 0, 0, 3, 0]
    fifth_page_ch = [0, 0, 0, 0, 0]
    sixth_page_ch = [0, 0, 4, 4, 0]
    seventh_page_ch = [0, 0, 0, 0]
    eighth_page_ch = [0, 0, 0, 0, 0]
    nine_page_ch = [0, 0, 3, 0, 0]
    tenth_page_ch = [0, 0, 0, 0]
    eleventh_page_ch = [0, 0, 0, 0, 0, 0]
    twelfth_page_ch = [0, 0, 0, 0, 3, 0]
    thirteenth_page_ch = [0, 0, 0, 0, 0, 0, 0]
    fourteenth_page_ch = [0, 0, 0]
    fifteenth_page_ch = [0, 3, 0, 0, 0, 0]
    sixteen_page_ch = [0, 3, 0]
    seventeenth_page_ch = [0, 0]
    eighteenth_page_ch = [0, 0, 0, 0]
    nineteenth_page_ch = [0, 1, 4, 9, 14]
    last_page_ch_radio = [0, 1, 2, 6, 10]
    last_page_ch_checkbox = [2, 3, 5, 12]

    try:
        WebDriverWait(driver, 300).until(EC.presence_of_element_located((By.ID, "outerframeContainer")))
        driver.find_element(By.ID, 'ls-button-submit').click()

        WebDriverWait(driver, 300).until(EC.presence_of_element_located((By.ID, "outerframeContainer")))
        driver.find_element(By.ID, 'ls-button-submit').click()

        check(driver, first_page_ch)
        table(driver, second_page_ch)
        check(driver, third_page_ch)
        table(driver, fourth_page_ch)
        table(driver, fifth_page_ch)
        table(driver, sixth_page_ch)
        table(driver, seventh_page_ch)
        table(driver, eighth_page_ch)
        table(driver, nine_page_ch)
        table(driver, tenth_page_ch)
        table(driver, eleventh_page_ch)
        table(driver, twelfth_page_ch)
        table(driver, thirteenth_page_ch)
        table(driver, fourteenth_page_ch)
        table(driver, fifteenth_page_ch)
        table(driver, sixteen_page_ch)
        table(driver, seventeenth_page_ch)
        table(driver, eighteenth_page_ch)
        radio(driver, nineteenth_page_ch)

        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "outerframeContainer")))
        page_radio = driver.find_elements(By.CLASS_NAME, 'radio-label')
        page_checkbox = driver.find_elements(By.CLASS_NAME, 'checkbox-label')
        for x in last_page_ch_radio:
            page_radio[x].click()
        for x in last_page_ch_checkbox:
            page_checkbox[x].click()
        driver.find_element(By.ID, 'ls-button-submit').click()

        # s += 1
        # print(f'Создано заявок {s}')

    finally:
        time.sleep(1)
        driver.quit()


def check(driver, ch):
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "outerframeContainer")))
    page = driver.find_elements(By.CLASS_NAME, 'checkbox-label')
    for x in ch:
        page[x].click()
    driver.find_element(By.ID, 'ls-button-submit').click()


def table(driver, ch):
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "outerframeContainer")))
    page = driver.find_elements(By.CLASS_NAME, 'ls-label-xs-visibility')
    page_selector = 0
    for x in ch:
        page[page_selector + x].click()
        page_selector += 5
    driver.find_element(By.ID, 'ls-button-submit').click()


def radio(driver, ch):
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "outerframeContainer")))
    page = driver.find_elements(By.CLASS_NAME, 'radio-label')
    for x in ch:
        page[x].click()
    driver.find_element(By.ID, 'ls-button-submit').click()


async def main():
    while True:
        loop = asyncio.get_event_loop()
        for url in ["https://sort.mguu.ru/r/e8ym6l"] * 10:
            scrape(url, loop=loop)
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))


if __name__ == '__main__':
    asyncio.run(main())
