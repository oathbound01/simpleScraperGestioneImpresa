import logging
import concurrent.futures

import requests
from bs4 import BeautifulSoup
import pandas as pd
import PyPDF2


# Make an excel document containing the URL where the word "blockchain" was found
# and a field saing whether the word was found or not or if there was an error
output = pd.DataFrame(list, columns = ['Webpage', 'Trovata in Home', 'Trovata in link', 'Link' ])
logger = logging.getLogger(__name__)
logging.basicConfig(filename='azioni.log', level=logging.INFO)
# Read the excel file
data = pd.read_excel('input.xlsx')

# Check the homepage for URLs that are of the same domain
def is_same_domain(url, domain):
    if url.startswith('http') and domain in url:
        return True
    return False


# Function to scrape the given URL for the word "blockchain"
def scrape_url(url):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        logger.error('There was an error while scraping the following link: ' + url)
        output.append(url, 'Errore', '-', '-')
        return e
    soup = BeautifulSoup(response.text, 'html.parser')
    body = soup.find('body').get_text()
    if 'blockchain' in body:
        logger.info('The word "blockchain" was found in the homepage:' + url)
        output.append(url, 'Sì', '-', '-')
        return True
    else:
        scrape_related_links(url, response)
        return

# Function to scrape related links of the same domain
def scrape_related_links(url, sourceResponse):
    soup = BeautifulSoup(sourceResponse.text, 'html.parser')
    domain = url.split('//')[1].split('/')[0]
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        if href and is_same_domain(href, domain):
            try:
                response = requests.get(href)
            except requests.exceptions.RequestException as e:
                logger.error('There was an error while scraping the following link: ' + href)
                output.append(url, 'No', 'Errore', href)
                return e
            if response.headers['Content-Type'] == 'application/pdf':
                scrape_pdf(response.content, url, href)
            else:
                soup = BeautifulSoup(response.text, 'html.parser')
                body = soup.find('body').get_text()
                if 'blockchain' in body:
                    logger.info('The word "blockchain" was found in the related link:' + href)
                    output.append(url, 'No', 'Sì', href)
                    return True
                else:
                    logger.info('The word "blockchain" was not found in the related link:' + href)
                    continue
    logger.info('The word "blockchain" was not found in any related links.')
    output.append(url, 'No', 'No', '-')
    return False


# If the link is a PDF, scrape the text from the PDF
def scrape_pdf(pdf, homepage_url, url):
    pdf = PyPDF2.PdfFileReader(pdf)
    text = ''
    for page in pdf.pages:
        text += page.extract_text()
    if 'blockchain' in text:
        logger.info('The word "blockchain" was found in the related link:' + url)
        output.append(homepage_url, 'No', 'Sì', url)
        return
    else:
        logger.info('The word "blockchain" was not found in the related link:' + url)
        return



def process_row(row):
    homepage_url = data['Website'][0]
    domain = homepage_url.split('//')[1].split('/')[0]
    scrape_url(homepage_url)

# Create a ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Map the function to the data
    executor.map(process_row, data.iterrows())

# Save the output to an excel file
output.to_excel('output.xlsx')
