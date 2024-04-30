import logging
from multiprocessing import Manager
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError
import os
from io import BytesIO

import requests
from bs4 import BeautifulSoup
import pandas as pd
import PyPDF2
import time


# Check the homepage for URLs that are of the same domain
def is_same_domain(url, domain):
    if url.startswith('http') and not domain in url:
        logger.info('The URL is not of the same domain: ' + url)
        return False
    if url.startswith('javascript') or url.startswith('mailto'):
        return False
    else:
        return True


# Function to scrape the given URL for the word "blockchain"
def scrape_url(url):
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.Timeout as e:
        logger.error('The request timed out for the following link: ' + url)
        shared_list.append((url, 'Errore', '-', '-'))
        return e
    except requests.exceptions.MissingSchema as e:
        logger.error('The URL is not valid: ' + url)
        shared_list.append((url, 'Errore', '-', '-'))
        return e
    except requests.exceptions.RequestException as e:
        logger.error('There was an error while scraping the following link: ' + url)
        shared_list.append((url, 'Errore', '-', '-'))
        return e
    except Exception as e:
        logger.error('There was an error while scraping the following link: ' + url)
        shared_list.append((url, 'Errore', '-', '-'))
        return e
    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        body = soup.find('body').get_text()
    except AttributeError as e:
        logger.error('There was an error while scraping the following link: ' + url)
        shared_list.append((url, 'Errore', '-', '-'))
        return
    except Exception as e:
        logger.error('There was an error while scraping the following link: ' + url)
        shared_list.append((url, 'Errore', '-', '-'))
        return
    if 'blockchain' in body or 'Blockchain' in body:
        logger.info('The word "blockchain" was found in the homepage:' + url)
        shared_list.append((url, 'Sì', '-', '-'))
        return True
    else:
        scrape_related_links(url, response)
        return

# Function to scrape related links of the same domain
def scrape_related_links(url, sourceResponse):
    soup = BeautifulSoup(sourceResponse.text, 'html.parser')
    domain = urlparse(url).netloc
    if domain.startswith('www.'):
        domain = domain.replace('www.', '')
    logger.info('Scraping related links of the same domain: ' + domain)
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        if href and href.endswith('.pdf'):
            try:
                response = requests.get(href)
            except requests.exceptions.Timeout as e:
                logger.error('The request timed out for the following PDF: ' + href)
                shared_list.append((url, 'No', 'Errore', href))
                return e
            except requests.exceptions.MissingSchema as e:
                logger.error('The URL is not valid: ' + url)
                shared_list.append((url, 'No', 'Errore', href))
                return e
            except requests.exceptions.RequestException as e:
                logger.error('There was an error while scraping the following PDF: ' + href)
                shared_list.append((url, 'No', 'Errore', href))
                return e
            except Exception as e:
                logger.error('There was an error while scraping the following PDF: ' + url)
                shared_list.append((url, 'Errore', '-', '-'))
                return e
            logger.info('Scraping the PDF: ' + href)
            pdf_file = BytesIO(response.content)
            scrape_pdf(pdf_file, url, href)
        if href and not href.startswith('#') and is_same_domain(href, domain):
            if href.startswith('http') is False:
                if href.startswith('/') is False:
                    href = '/' + href
                href = url + href
            try:
                response = requests.get(href)
            except requests.exceptions.Timeout as e:
                logger.error('The request timed out for the following link: ' + href)
                shared_list.append((url, 'No', 'Errore', href))
                return e
            except requests.exceptions.RequestException as e:
                logger.error('There was an error while scraping the following link: ' + href)
                shared_list.append((url, 'No', 'Errore', href))
                return e
            except Exception as e:
                logger.error('There was an error while scraping the following link: ' + url)
                shared_list.append((url, 'Errore', '-', '-'))
                return e
            else:
                soup = BeautifulSoup(response.text, 'html.parser')
                try:
                    body = soup.find('body').get_text()
                except AttributeError as e:
                    logger.error('There was an error while scraping the following link: ' + url)
                    shared_list.append((url, 'Errore', '-', '-'))
                    return
                except Exception as e:
                    logger.error('There was an error while scraping the following link: ' + url)
                    shared_list.append((url, 'Errore', '-', '-'))
                    return
                if 'blockchain' in body or 'Blockchain' in body:
                    logger.info('The word "blockchain" was found in the related link:' + href)
                    shared_list.append((url, 'No', 'Sì', href))
                    return True
                else:
                    logger.info('The word "blockchain" was not found in the related link:' + href)
                    continue
    logger.info('The word "blockchain" was not found in any related links.')
    shared_list.append((url, 'No', 'No', '-'))
    return False

# If the link is a PDF, scrape the text from the PDF
def scrape_pdf(pdf, homepage_url, url):
    try:
        pdf = PyPDF2.PdfReader(pdf)
        text = ''
        for page in pdf.pages:
            text += page.extract_text()
        if 'blockchain' in text or 'Blockchain' in text:
            logger.info('The word "blockchain" was found in the related PDF:' + url)
            shared_list.append((homepage_url, 'No', 'Sì', url))
            return
        else:
            logger.info('The word "blockchain" was not found in the related PDF:' + url)
            return
    except Exception as e:
        logger.error('There was an error while parsing the PDF: ' + url + '. Error: ' + str(e))
        shared_list.append((homepage_url, 'No', 'Errore', url))
        return e



def process_row(args):
    index, row, list = args
    homepage_url = row['Website']
    logger.info('Scraping the homepage: ' + homepage_url)
    if homepage_url.startswith('http') is False:
        homepage_url = 'http://' + homepage_url
    scrape_url(homepage_url)
    print('List length:', len(shared_list))

if __name__ == '__main__':
        # Set up the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    log_filename = 'azioni_' + time.strftime('%Y%m%d%H%M%S') + '.log'
    handler = logging.FileHandler(log_filename)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter('PID:  %(process)s - %(asctime)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Read the excel file
    data = pd.read_excel('input.xlsx')
    logger.info('The input file was read successfully.')


    # Create a shared list to store the results
    manager = Manager()
    shared_list = manager.list()

    # Multiprocessing
    with ProcessPoolExecutor() as executor:

        # Create a list to hold the Future objects
        futures = []

        # Submit the function to the executor
        for args in [(index, row, shared_list) for index, row in data.iterrows()]:
            future = executor.submit(process_row, args)
            futures.append(future)

        # Collect the results, enforcing a timeout
        for future in futures:
            try:
                future.result(timeout=10)  # Adjust the timeout as needed
            except TimeoutError:
                logger.error('A scraping process timed out.')
            except Exception as e:
                logger.error('An error occurred in process with PID: ' + str(os.getpid()) + '. Error: ' + str(e))

    final_list = list(shared_list)
    # Save the output to an excel file
    logger.info('Saving the output to an excel file.')
    output = pd.DataFrame(final_list, columns = ['Webpage', 'Trovata in Home', 'Trovata in link', 'Link' ])
    output.to_excel('output.xlsx')
    logger.info('The output was saved successfully.')
    