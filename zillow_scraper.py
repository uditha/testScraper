import requests
import json
import time
import logging
from datetime import datetime
import pandas as pd
import os
from typing import Dict, List, Optional
from pathlib import Path

class ZillowScraper:
    def __init__(self):
        # Create directories first
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.base_dir, 'data')
        self.logs_dir = os.path.join(self.base_dir, 'logs')
        
        # Create directories
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # Now setup logging
        self.setup_logging()
        self.existing_zpids = set()
        self.load_existing_data()
        
        # Working headers and cookies
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,fr;q=0.7,az;q=0.6',
            'content-type': 'application/json',
            'origin': 'https://www.zillow.com',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
        }
        
        self.cookies = {
            'zguid': '24|%249c2787c6-c251-4b18-872d-e27676fa14bb',
            '_ga': 'GA1.2.1240521408.1728343901',
            'zjs_anonymous_id': '%229c2787c6-c251-4b18-872d-e27676fa14bb%22',
            'zjs_user_id': 'null',
            'zg_anonymous_id': '%221338505b-b3e0-41d3-a985-c7b3a91e3aae%22',
            '_pxvid': '4ea717c9-8504-11ef-a560-a4d7af390e7f',
            '_gcl_au': '1.1.677761391.1728343911',
            '_scid': 'P64JKgWmPXPHwnkG5q17mWCzxaR7N67-',
            '_tt_enable_cookie': '1',
            '_ttp': '3T_6YSJBPj0RGxshYmaZdX2AtjJ',
            '_fbp': 'fb.1.1728343912836.158241638969145371',
            '_pin_unauth': 'dWlkPVpXVm1NRGxpWXpBdE4yWTBaUzAwTVdKaUxUbGtaRE10WXpsaVlUUTNNak16TXpGbA',
            '_ScCbts': '%5B%5D',
            '_sctr': '1%7C1729449000000',
            'zgsession': '1|11584fe2-62a5-47d4-99af-641d2204d1a8',
            '_gid': 'GA1.2.1559948367.1729612602'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.cookies.update(self.cookies)

    def setup_logging(self):
        """Configure logging"""
        log_file = os.path.join(self.logs_dir, f'zillow_scraper_{datetime.now().strftime("%Y%m%d")}.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def load_existing_data(self):
        """Load existing data to avoid duplicates"""
        try:
            file_path = os.path.join(self.data_dir, 'zillow_data.xlsx')
            if os.path.exists(file_path):
                df = pd.read_excel(file_path)
                self.existing_zpids = set(df['ZPID'].astype(str).unique())
                logging.info(f"Loaded {len(self.existing_zpids)} existing ZPIDs")
        except Exception as e:
            logging.error(f"Error loading existing data: {e}")
            self.existing_zpids = set()

    # def get_search_results(self) -> List[Dict]:
    #     """Get property listings from search results"""
    #     try:
    #         json_data = {
    #             'searchQueryState': {
    #                 'pagination': {},
    #                 'isMapVisible': True,
    #                 'mapBounds': {
    #                     'west': -74.30523740039064,
    #                     'east': -73.35766659960939,
    #                     'south': 40.59682669577144,
    #                     'north': 40.745773606464226,
    #                 },
    #                 'regionSelection': [
    #                     {
    #                         'regionId': 270915,
    #                         'regionType': 17,
    #                     },
    #                 ],
    #                 'filterState': {
    #                     'sortSelection': {
    #                         'value': 'globalrelevanceex',
    #                     },
    #                 },
    #                 'isListVisible': True,
    #             },
    #             'wants': {
    #                 'cat1': [
    #                     'mapResults',
    #                 ],
    #             },
    #             'requestId': 2,
    #         }

    #         response = self.session.put(
    #             'https://www.zillow.com/async-create-search-page-state',
    #             json=json_data,
    #             timeout=30
    #         )
    #         response.raise_for_status()
            
    #         data = response.json()
    #         return data['cat1']['searchResults']['mapResults']
            
    #     except Exception as e:
    #         logging.error(f"Error getting search results: {e}")
    #         return []

    def get_search_results(self) -> List[Dict]:
        """Get property listings from search results"""
        all_results = []
        current_page = 1

        while True:
            try:
                json_data = {
                    'searchQueryState': {
                        'pagination': {
                            'currentPage': current_page,
                        },
                        'isMapVisible': True,
                        'mapBounds': {
                            'west': -74.30523740039064,
                            'east': -73.35766659960939,
                            'south': 40.59682669577144,
                            'north': 40.745773606464226,
                        },
                        'regionSelection': [
                            {
                                'regionId': 270915,
                                'regionType': 17,
                            },
                        ],
                        'filterState': {
                            'sortSelection': {
                                'value': 'globalrelevanceex',
                            },
                        },
                        'isListVisible': True,
                    },
                    'wants': {
                        'cat1': [
                            'mapResults',
                        ],
                    },
                    'requestId': 2,
                }

                response = self.session.put(
                    'https://www.zillow.com/async-create-search-page-state',
                    json=json_data,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                results = data['cat1']['searchResults']['mapResults']
                
                if not results:
                    break
                
                all_results.extend(results)
                logging.info(f"Got {len(results)} results from page {current_page}")
                current_page += 1

                # only 20 pages allowed
                if current_page > 2:
                    break

                time.sleep(3)
                
            except Exception as e:
                logging.error(f"Error getting search results: {e}")
                break

        return all_results

    def get_property_details(self, zpid: str) -> Optional[Dict]:
        """Get detailed property information"""
        try:
            params = {
                'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"a2b500eeeec76ac685562c34e99dae1ee6a1a841a5a78ebc90bedae726c71659"}}',
                'variables': json.dumps({
                    "zpid": zpid,
                    "platform": "DESKTOP_WEB",
                    "formType": "OPAQUE"
                })
            }

            response = self.session.get(
                'https://www.zillow.com/graphql/',
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()['data']['property']

            # # save to json file
            # with open(f"{zpid}.json", "w") as f:
            #     json.dump(data, f, indent=4)
            
            # Get facts dictionary
            facts_dict = {}
            if 'resoFacts' in data and 'atAGlanceFacts' in data['resoFacts']:
                facts_dict = {
                    fact['factLabel']: fact['factValue']
                    for fact in data['resoFacts']['atAGlanceFacts']
                }

            # Format price history
            price_history = []
            for item in data.get('priceHistory', []):
                date = item.get('date', '')
                price = item.get('price')
                event = item.get('event', '')
                
                if price is None:
                    price_str = "N/A"
                else:
                    price_str = f"${price:,}"
                    
                price_history.append(f"{date}: {price_str} ({event})")

            price_history_str = ', '.join(price_history)

            # Format schools
            schools = []
            for school in data.get('schools', []):
                rating = school.get('rating', 'N/A')
                name = school.get('name', 'Unknown')
                schools.append(f"Rating: {rating} Name: {name}")
            schools_str = ', '.join(schools)

            # Extract address components safely
            address = data.get('address', {})

            # handle 'openHouse': data['property']['openHouseSchedule'],
            # [{'startTime': '2024-10-26 11:30:00', 'endTime': '2024-10-26 13:30:00'}, {'startTime': '2024-10-27 12:00:00', 'endTime': '2024-10-27 14:00:00'}]
            openHouse = data.get('openHouse', {})
            openHouseSchedule = []
            for item in openHouse:
                startTime = item.get('startTime', '')
                endTime = item.get('endTime', '')
                openHouseSchedule.append(f"{startTime} - {endTime}")
            
            openHouseSchedule_str = ', '.join(openHouseSchedule)


            # images list responsivePhotos
            imagesList = data.get('responsivePhotos', [])
          

            # this is a list of dictionaries, 
            # each dic has mixedSources dic
            # it has different formats of images like jpeg, webp, etc
            # we take the jpeg format, so select that one and
            # get the last element(dic) of the list since it is the highest resolution
            # then get the url from that dic
            images = []
            for item in imagesList:
                mixedSources = item.get('mixedSources', [])
                jpegSources = mixedSources.get('jpeg', [])
                if jpegSources:
                    images.append(jpegSources[-1].get('url'))

            
            property_data = {
                'ZPID': zpid,
                'url': f"https://www.zillow.com/homedetails/{zpid}_zpid/",
                'fetchDate': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'streetAddress': address.get('streetAddress', ''),
                'city': address.get('city', ''),
                'state': address.get('state', ''),
                'zipcode': address.get('zipcode', ''),
                'price': data.get('price'),
                'bedrooms': data.get('bedrooms'),
                'bathrooms': data.get('bathrooms'),
                'livingArea': data.get('livingArea'),
                'yearBuilt': facts_dict.get('Year Built', ''),
                'propertyType': data.get('resoFacts', {}).get('homeType', ''),
                'status': data.get('homeStatus', ''),
                'daysOnZillow': str(facts_dict.get('Days on Zillow', '')).replace('Days', '').strip(),
                'views': data.get('pageViewCount', ''),
                'saved': data.get('favoriteCount', ''),
                'priceHistory': price_history_str,
                'description': data.get('description', '').replace('\n', ' '),
                'schools': schools_str,
                'propertyTaxRate': data.get('propertyTaxRate'),
                'annualHomeownersInsurance': data.get('annualHomeownersInsurance', ''),
                'hoaFee': (data.get('resoFacts', {}).get('hoaFee') or '').replace('monthly', '').strip(),
                'appliances': ', '.join(data.get('resoFacts', {}).get('appliances', []) if isinstance(data.get('resoFacts', {}).get('appliances'), list) else []),
                'heating': ', '.join(data.get('resoFacts', {}).get('heating', []) if isinstance(data.get('resoFacts', {}).get('heating'), list) else []),
                'cooling': ', '.join(data.get('resoFacts', {}).get('cooling', []) if isinstance(data.get('resoFacts', {}).get('cooling'), list) else []),
                'parkingFeatures': ', '.join(data.get('resoFacts', {}).get('parkingFeatures', []) if isinstance(data.get('resoFacts', {}).get('parkingFeatures'), list) else []),
                'openHouseSchedule': openHouseSchedule_str,
                'mlsId': data.get('attributionInfo', {}).get('mlsId', ''),
                'mlsName' : data.get('attributionInfo', {}).get('mlsName', ''),
                'agentName' : data.get('attributionInfo', {}).get('agentName', ''),
                'agentPhoneNumber' : data.get('attributionInfo', {}).get('agentPhoneNumber', ''),
                'coAgentName' : data.get('attributionInfo', {}).get('coAgentName', ''),
                'coAgentNumber' : data.get('attributionInfo', {}).get('coAgentNumber', ''),
                'brokerName' : data.get('attributionInfo', {}).get('brokerName', ''),
                'brokerPhoneNumber' : data.get('attributionInfo', {}).get('brokerPhoneNumber', ''),
                'propertyJSON' : json.dumps(data, indent=4)

            }

            # I need to add all images in different columns Images1, Images2, etc
            for i in range(len(images)):
                property_data[f'Images{i+1}'] = images[i]

            return property_data

        except Exception as e:
            logging.error(f"Error getting property details for ZPID {zpid}: {e}")
            return None

    def save_data(self, properties: List[Dict]):
        """Save property data to Excel"""
        try:
            file_path = os.path.join(self.data_dir, 'zillow_data.xlsx')
            
            # Combine with existing data if file exists
            if os.path.exists(file_path):
                existing_df = pd.read_excel(file_path)
                new_df = pd.DataFrame(properties)
                df = pd.concat([existing_df, new_df], ignore_index=True)
                # Remove duplicates based on ZPID and keep the latest
                df = df.sort_values('fetchDate').drop_duplicates(subset=['ZPID'], keep='last')
            else:
                df = pd.DataFrame(properties)

            # Save to Excel
            df.to_excel(file_path, index=False)
            logging.info(f"Saved {len(properties)} properties to {file_path}")

            # Create backup
            backup_path = os.path.join(
                self.data_dir, 
                f'zillow_data_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            )
            df.to_excel(backup_path, index=False)
            logging.info(f"Created backup at {backup_path}")

        except Exception as e:
            logging.error(f"Error saving data: {e}")
            # Try to save to a temporary file
            try:
                temp_path = os.path.join(
                    self.data_dir,
                    f'zillow_data_temp_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
                )
                pd.DataFrame(properties).to_excel(temp_path, index=False)
                logging.info(f"Saved data to temporary file: {temp_path}")
            except Exception as inner_e:
                logging.error(f"Failed to save to temporary file: {inner_e}")

    def run(self, delay: int = 2, max_retries: int = 3):
        """Main function to run the scraper"""
        logging.info("Starting Zillow data collection")
        
        properties = []
        try:
            # Get search results with retries
            results = None
            for attempt in range(max_retries):
                results = self.get_search_results()
                if results:
                    break
                logging.warning(f"Retry {attempt + 1} of {max_retries} for search results")
                time.sleep(delay * 2)

            if not results:
                logging.error("Failed to get search results after all retries")
                return

            logging.info(f"Found {len(results)} properties")

            # Process each property
            for item in results[:15]:
                try:
                    zpid = str(item['zpid'])
                    
                    if zpid in self.existing_zpids:
                        logging.info(f"Skipping existing property {zpid}")
                        continue

                    logging.info(f"Processing property {zpid}")
                    
                    # Get property details with retries
                    property_data = None
                    for attempt in range(max_retries):
                        property_data = self.get_property_details(zpid)
                        if property_data:
                            break
                        logging.warning(f"Retry {attempt + 1} of {max_retries} for ZPID {zpid}")
                        time.sleep(delay * 2)

                    if property_data:
                        properties.append(property_data)
                        self.save_data(properties)  # Save data after each property
                        properties.clear()  # Clear the list to avoid duplicates
                        time.sleep(delay)
                    
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logging.error(f"Error processing property {zpid}: {e}")
                    continue

        except KeyboardInterrupt:
            logging.info("Operation interrupted by user")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        finally:
            # Save whatever data we have
            if properties:
                self.save_data(properties)
                logging.info(f"Scraped {len(properties)} new properties")
            else:
                logging.info("No new properties to save")

if __name__ == "__main__":
    scraper = ZillowScraper()
    scraper.run()