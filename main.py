import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://www.ccaward.com"
START_URL = f"{BASE_URL}/award-winners/"
OUTPUT_FILE = 'award_winners.csv'

# Initialize CSV file
if not os.path.exists(OUTPUT_FILE):
    df = pd.DataFrame(columns=['country', 'state', 'city', 'company_name', 'type_of_business', 'social_media_links', 'address', 'phone', 'website', 'google_reviews'])
    df.to_csv(OUTPUT_FILE, index=False)

def get_soup(url):
    logging.info(f"Fetching URL: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.text, 'html.parser')

def extract_links(soup, css_class):
    links = []
    for a in soup.find_all('a',class_=css_class):
        h3_tag = a.find('h3')
        if h3_tag:
            links.append((a['href'], h3_tag.text.strip()))
        else:
            logging.warning(f"No h3 tag found in link: {a['href']}")
    logging.info(f"Extracted {len(links)} links with class '{css_class}'")
    return links

def extract_business_links(soup):
    links = []
    for h3 in soup.find_all('h3', class_='winner-heading'):
        if h3:
            link = h3.find('a')['href']
            links.append(link)
        else:
            logging.warning(f"No h3 tag found")
    logging.info(f"Extracted {len(links)} links with class")
    return links

def extract_company_data(soup):
    data = {}
    data['company_name'] = soup.find('h1').text.strip()
    logging.info(f"Extracted company name: {data['company_name']}")
    
    business_type_tag = soup.find('h2')
    business_type = business_type_tag.contents[0].strip() if business_type_tag else ''
    data['type_of_business'] = business_type
    logging.info(f"Extracted type of business: {data['type_of_business']}")
    
    social_links = soup.find('div', class_='winner-section__hero__details__social')
    if social_links:
        data['social_media_links'] = ', '.join([a['href'] for a in social_links.find_all('a')])
    else:
        data['social_media_links'] = ''
    logging.info(f"Extracted social media links: {data['social_media_links']}")
    
    address = soup.find('a', class_='winner-section__hero__details__footer__address')
    if address:
        data['address'] = address.find('address').text.strip()
    else:
        data['address'] = ''
    logging.info(f"Extracted address: {data['address']}")
    
    phone = soup.find('a', class_='winner-section__hero__details__footer__phone')
    if phone:
        data['phone'] = phone.text.strip()
    else:
        data['phone'] = ''
    logging.info(f"Extracted phone: {data['phone']}")
    
    website = soup.find('a', class_='winner-section__hero__details__footer__url')
    if website:
        data['website'] = website.text.strip()
    else:
        data['website'] = ''
    logging.info(f"Extracted website: {data['website']}")
    
    reviews = soup.find('div', class_='winner-section__hero__details__footer__google-reviews__rating')
    if reviews:
        data['google_reviews'] = reviews.find('strong').text.strip()
    else:
        data['google_reviews'] = ''
    logging.info(f"Extracted Google reviews: {data['google_reviews']}")
    
    return data

def load_existing_data():
    return pd.read_csv(OUTPUT_FILE)

def save_data(new_data):
    if not os.path.exists(OUTPUT_FILE):
        df = pd.DataFrame(columns=['country', 'state', 'city', 'company_name', 'type_of_business', 'social_media_links', 'address', 'phone', 'website', 'google_reviews'])
    else:
        df = pd.read_csv(OUTPUT_FILE)
    
    new_data_df = pd.DataFrame(new_data)
    df = pd.concat([df, new_data_df], ignore_index=True)
    df.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Saved data to {OUTPUT_FILE}")

def scrape():
    logging.info("Starting scraper")
    existing_data = load_existing_data()
    existing_cities = set(existing_data['city'])

    soup = get_soup(START_URL)
    country_links = extract_links(soup, 'blogs-item-link')
    
    for country_link, country_name in country_links:
        country_soup = get_soup(country_link)
        
        state_links = extract_links(country_soup, 'blogs-item-link')
        
        for state_link, state_name in state_links:
            state_soup = get_soup(state_link)
            
            city_links = extract_links(state_soup, 'blogs-item-link')
            
            for city_link, city_name in city_links:
                if city_name in existing_cities:
                    logging.info(f"Skipping already scraped city: {city_name}")
                    continue
                
                city_soup = get_soup(city_link)
                
                company_links = extract_business_links(city_soup)
                
                city_data = []
                for company_link in company_links:
                    company_soup = get_soup(company_link)
                    try:
                        company_data = extract_company_data(company_soup)
                        company_data['country'] = country_name
                        company_data['state'] = state_name
                        company_data['city'] = city_name
                        city_data.append(company_data)
                        logging.info(f"Extracted data for company: {company_data['company_name']} in {city_name}, {state_name}, {country_name}")
                    except Exception as e:
                        logging.error(f"Error extracting company data: {e}")
                        continue
                    
                    # Sleep to avoid overloading the server
                    time.sleep(1)
                
                if city_data:
                    save_data(city_data)
                    existing_cities.add(city_name)
    
    logging.info("Scraping completed")

if __name__ == "__main__":
    try:
        scrape()
    except Exception as e:
        logging.error(f"Scraping failed: {e}")
