from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import logging
import json
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

class AMISScraper:
    """Enhanced Agricultural Market Information System (AMIS) scraper with improved reliability."""
    
    # Configuration constants
    DELAYS = {
        'between_requests': 2,
        'after_filter': 3,
        'after_page_load': 1,
        'between_retries': 5
    }
    
    MAX_RETRIES = 3
    BATCH_SIZE = 1000  # Number of rows to process at once for memory efficiency
    
    def __init__(self, 
                 url: str = "https://amis.co.ke/site/market_search/market_search",
                 headless: bool = True,
                 timeout: int = 40):
        """
        Initialize the enhanced AMIS scraper.
        
        Args:
            url (str): Base URL for scraping
            headless (bool): Whether to run Chrome in headless mode
            timeout (int): Maximum wait time for page elements
        """
        self.url = url
        self.timeout = timeout
        self.entry_options = [10, 50, 100, 1000, 1500, 3000]
        
        # Set up directories
        self.base_dir = "amis_data"
        self.dirs = {
            'exports': os.path.join(self.base_dir, 'exports'),
            'logs': os.path.join(self.base_dir, 'logs'),
            'progress': os.path.join(self.base_dir, 'progress')
        }
        self._setup_directories()
        
        # Initialize logger
        self.logger = self._setup_logger()
        
        # Initialize webdriver with enhanced options
        self.driver = self._initialize_webdriver(headless)
        self.wait = WebDriverWait(self.driver, timeout)
        
        # Load or initialize progress tracking
        self.progress_file = os.path.join(self.dirs['progress'], 'scraping_progress.json')
        self.progress = self._load_progress()

    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        for directory in self.dirs.values():
            os.makedirs(directory, exist_ok=True)

    def _setup_logger(self) -> logging.Logger:
        """Configure enhanced logging with file and console output."""
        logger = logging.getLogger('AMISScraper')
        logger.setLevel(logging.INFO)
        
        # File handler
        fh = logging.FileHandler(
            os.path.join(self.dirs['logs'], f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        )
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger

    def _initialize_webdriver(self, headless: bool) -> webdriver.Chrome:
        """Initialize Chrome WebDriver with enhanced options."""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        
        # Performance and stability options
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--ignore-certificate-errors")
        
        # Memory management options
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--disk-cache-size=1")
        
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )

    def _load_progress(self) -> Dict:
        """Load or initialize progress tracking."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                self.logger.warning("Could not load progress file, starting fresh")
        return {
            'last_county': None,
            'last_market': None,
            'last_product': None,
            'completed': [],
            'timestamp': None
        }

    def _save_progress(self, county: str, market: str, product: str):
        """Save current scraping progress."""
        self.progress.update({
            'last_county': county,
            'last_market': market,
            'last_product': product,
            'timestamp': datetime.now().isoformat()
        })
        self.progress['completed'].append({
            'county': county,
            'market': market,
            'product': product,
            'timestamp': datetime.now().isoformat()
        })
        
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)

    def _get_total_entries(self) -> Optional[int]:
        """Get total number of entries available from the page."""
        try:
            info_text = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".dataTables_info"))
            ).text
            match = re.search(r'of (\d+) entries', info_text)
            if match:
                return int(match.group(1))
            return None
        except Exception as e:
            self.logger.error(f"Error getting total entries: {str(e)}")
            return None

    def _optimize_entries(self, total_entries: int) -> int:
        """Choose optimal entries per page based on total available."""
        if not total_entries:
            return self.entry_options[0]
            
        for entry_option in reversed(self.entry_options):
            if entry_option <= total_entries:
                return entry_option
        return self.entry_options[0]

    def _verify_data_completeness(self, df: pd.DataFrame, expected_count: int) -> bool:
        """Verify scraped data matches expected count."""
        if df is None or df.empty:
            self.logger.warning("No data found")
            return False
            
        actual_count = len(df)
        if actual_count < expected_count:
            self.logger.warning(
                f"Data completeness check failed. Expected {expected_count} rows, got {actual_count}"
            )
            return False
            
        return True

    def _refresh_session(self):
        """Refresh browser session if needed."""
        try:
            self.driver.delete_all_cookies()
            self.driver.refresh()
            time.sleep(self.DELAYS['after_page_load'])
            self._wait_for_page_load()
        except Exception as e:
            self.logger.error(f"Error refreshing session: {str(e)}")
            self._reinitialize_driver()

    def _reinitialize_driver(self):
        """Reinitialize the WebDriver if it becomes unresponsive."""
        try:
            self.driver.quit()
        except:
            pass
        finally:
            self.driver = self._initialize_webdriver(True)  # Reinitialize in headless mode
            self.wait = WebDriverWait(self.driver, self.timeout)

    def main():
        # Test configuration
        test_config = {
            'counties': ["Nairobi"],
            'products': ["Dry maize"],
            'markets': ["Nyamakima"],
            'start_date': "2023-01-01",
            'end_date': "2023-12-31",
            'resume': True
        }
        
        # Full configuration
        full_config = {
            'counties': ["Nairobi", "Mombasa", "Kisumu"],  # Add all counties
            'products': ["Dry maize", "Beans", "Rice"],     # Add all products
            'markets': ["Nyamakima", "City Market"],        # Add all markets
            'start_date': "2023-01-01",
            'end_date': "2023-12-31",
            'resume': True
        }
        
        try:
            # Initialize scraper
            scraper = AMISScraper(headless=True, timeout=120)
            
            # Run test configuration first
            scraper.logger.info("Starting test run...")
            test_df = scraper.run_all(
                counties=test_config['counties'],
                products=test_config['products'],
                markets=test_config['markets'],
                start_date=test_config['start_date'],
                end_date=test_config['end_date'],
                resume=test_config['resume']
            )
            
            if test_df is not None:
                scraper.logger.info("Test run completed successfully!")
                
                # If test run is successful, proceed with full configuration
                scraper.logger.info("Starting full run...")
                full_df = scraper.run_all(
                    counties=full_config['counties'],
                    products=full_config['products'],
                    markets=full_config['markets'],
                    start_date=full_config['start_date'],
                    end_date=full_config['end_date'],
                    resume=full_config['resume']
                )
                
                if full_df is not None:
                    scraper.logger.info("Full run completed successfully!")
                else:
                    scraper.logger.error("Full run failed to collect data")
            else:
                scraper.logger.error("Test run failed to collect data")
        
        except Exception as e:
            scraper.logger.critical(f"Critical error in main process: {str(e)}", exc_info=True)
        
        finally:
            try:
                scraper.driver.quit()
                scraper.logger.info("Browser closed successfully")
            except Exception as e:
                scraper.logger.error(f"Error closing browser: {str(e)}")


    def _save_intermediate_data(self, 
                              df: pd.DataFrame, 
                              county: str, 
                              market: str, 
                              product: str):
        """Save intermediate results to CSV."""
        filename = (
            f"amis_data_{county}_{market}_{product}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        filepath = os.path.join(self.dirs['exports'], filename)
        df.to_csv(filepath, index=False)
        self.logger.info(f"Saved intermediate data to {filepath}")

    def _save_final_results(self, all_data: List[pd.DataFrame]) -> pd.DataFrame:
        """Consolidate and save final results."""
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates
        final_df = final_df.drop_duplicates()
        
        # Save final consolidated CSV
        final_output_path = os.path.join(
            self.dirs['exports'],
            f"amis_data_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        final_df.to_csv(final_output_path, index=False)
        self.logger.info(f"Saved final consolidated data to {final_output_path}")
        
        return final_df

# Usage example
def run_all(self, 
            counties: List[str],
            products: List[str],
            markets: List[str],
            start_date: str = "2022-01-01",
            end_date: str = None,
            resume: bool = True) -> Optional[pd.DataFrame]:
    """
    Run the complete scraping process.
    
    Args:
        counties: List of counties to scrape
        products: List of products to scrape
        markets: List of markets to scrape
        start_date: Start date for data collection
        end_date: End date for data collection (defaults to current date)
        resume: Whether to resume from last saved progress
    """
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    all_data = []
    
    try:
        # Resume from last progress if requested
        if resume and self.progress['last_county']:
            start_idx = {
                'county': counties.index(self.progress['last_county']),
                'market': markets.index(self.progress['last_market']),
                'product': products.index(self.progress['last_product'])
            }
        else:
            start_idx = {'county': 0, 'market': 0, 'product': 0}
        
        for county_idx in range(start_idx['county'], len(counties)):
            county = counties[county_idx]
            
            for market_idx in range(start_idx['market'], len(markets)):
                market = markets[market_idx]
                
                for product_idx in range(start_idx['product'], len(products)):
                    product = products[product_idx]
                    
                    # Skip if already completed
                    if any(c['county'] == county and 
                          c['market'] == market and 
                          c['product'] == product 
                          for c in self.progress['completed']):
                        continue
                    
                    retry_count = 0
                    while retry_count < self.MAX_RETRIES:
                        try:
                            self.logger.info(
                                f"Scraping: County={county}, Market={market}, "
                                f"Product={product}, Attempt={retry_count + 1}"
                            )
                            
                            # Set filters and get total entries
                            if not self.set_filters(county, market, product, 
                                                  start_date, end_date, "100"):
                                raise Exception("Failed to set filters")
                            
                            total_entries = self._get_total_entries()
                            if not total_entries:
                                self.logger.warning("No entries found, skipping...")
                                break
                            
                            # Optimize entries per page
                            optimal_entries = self._optimize_entries(total_entries)
                            
                            # Reset filters with optimal entries
                            if not self.set_filters(county, market, product,
                                                  start_date, end_date, 
                                                  str(optimal_entries)):
                                raise Exception("Failed to set optimized filters")
                            
                            # Scrape data
                            df = self.scrape_table()
                            
                            # Verify data completeness
                            if self._verify_data_completeness(df, total_entries):
                                # Add metadata
                                df['county'] = county
                                df['market'] = market
                                df['product'] = product
                                df['scrape_date'] = datetime.now().strftime("%Y-%m-%d")
                                
                                # Save intermediate results
                                self._save_intermediate_data(df, county, market, product)
                                all_data.append(df)
                                
                                # Update progress
                                self._save_progress(county, market, product)
                                break
                            
                            retry_count += 1
                            time.sleep(self.DELAYS['between_retries'])
                            self._refresh_session()
                            
                        except Exception as e:
                            self.logger.error(
                                f"Error scraping {county}/{market}/{product}: {str(e)}"
                            )
                            retry_count += 1
                            if retry_count < self.MAX_RETRIES:
                                time.sleep(self.DELAYS['between_retries'])
                                self._refresh_session()
                    
                    time.sleep(self.DELAYS['between_requests'])
                
                # Reset product index when moving to next market
                start_idx['product'] = 0
            
            # Reset market index when moving to next county
            start_idx['market'] = 0
    
    except Exception as e:
        self.logger.critical(f"Critical error in scraping process: {str(e)}")
        return None
    
    finally:
        self.driver.quit()
    
    # Consolidate and save final results
    if all_data:
        return self._save_final_results(all_data)
    
    return None

from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class DatePickerHandler:
    def __init__(self, driver, logger, wait_timeout=10):
        self.driver = driver
        self.logger = logger
        self.wait = WebDriverWait(driver, wait_timeout)

    def _validate_dates(self, start_date: str, end_date: str) -> bool:
        """Validate date formats and ensure the end date is not earlier than the start date."""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            if end < start:
                self.logger.error("End date cannot be earlier than start date.")
                return False
            return True
        except ValueError:
            self.logger.error("Invalid date format. Use YYYY-MM-DD.")
            return False

    def _parse_caption(self, caption_text: str) -> datetime:
        """Parse the caption text in format 'Month, YYYY'."""
        if not caption_text or caption_text.isspace():
            raise ValueError("Empty or whitespace caption")
        
        try:
            # The caption format is consistently "Month, YYYY" as seen in the HTML
            return datetime.strptime(caption_text.strip(), "%B, %Y")
        except ValueError as e:
            self.logger.error(f"Failed to parse caption: {caption_text}")
            raise e

    def _navigate_to_month_year(self, datepicker, target_date: datetime):
        """Navigate to the correct month and year in the date picker."""
        max_attempts = 24
        attempts = 0

        while attempts < max_attempts:
            try:
                # Using more specific selector for the caption
                caption = datepicker.find_element(By.CSS_SELECTOR, "table.dp_header td.dp_caption")
                current_date = self._parse_caption(caption.text.strip())

                if current_date.year < target_date.year or \
                   (current_date.year == target_date.year and current_date.month < target_date.month):
                    next_button = datepicker.find_element(By.CSS_SELECTOR, "table.dp_header td.dp_next")
                    next_button.click()
                elif current_date.year > target_date.year or \
                     (current_date.year == target_date.year and current_date.month > target_date.month):
                    prev_button = datepicker.find_element(By.CSS_SELECTOR, "table.dp_header td.dp_previous")
                    prev_button.click()
                else:
                    break

                time.sleep(0.5)
                attempts += 1
            except Exception as e:
                self.logger.warning(f"Navigation attempt {attempts + 1} failed: {str(e)}")
                time.sleep(1)
                attempts += 1
        if attempts >= max_attempts:
            raise ValueError("Failed to navigate to target month/year within maximum attempts.")

    def _set_date_in_calendar(self, date_str: str, field_id: str) -> bool:
        """Set the given date using the Zebra DatePicker widget."""
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            
            # Open the date picker
            date_input = self.wait.until(EC.presence_of_element_located((By.ID, field_id)))
            self.driver.execute_script("arguments[0].scrollIntoView(true);", date_input)
            time.sleep(0.5)
            date_input.click()
            time.sleep(1)

            # Locate the date picker
            datepicker = self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.Zebra_DatePicker:not(.dp_hidden)")
            ))

            # Navigate to the correct month/year
            self._navigate_to_month_year(datepicker, target_date)

            # Select the day
            days = datepicker.find_elements(
                By.CSS_SELECTOR,
                "table.dp_daypicker td:not(.dp_not_in_month):not(.dp_disabled)"
            )
            for day in days:
                if day.text.strip() == str(target_date.day):
                    day.click()
                    time.sleep(0.5)

                    # Verify the date is set
                    actual_value = date_input.get_attribute("value")
                    if actual_value == date_str:
                        self.logger.info(f"Successfully set date to {date_str}")
                        return True
                    else:
                        self.logger.error(f"Date not set correctly. Expected {date_str}, got {actual_value}.")
                        return False
            self.logger.error(f"Target day {target_date.day} not found in date picker.")
            return False
        except Exception as e:
            self.logger.error(f"Error setting date in calendar: {str(e)}")
            return False

    def _set_dates(self, start_date: str, end_date: str, max_retries: int = 3) -> bool:
        """Set start and end dates using the date picker, with retries."""
        if not self._validate_dates(start_date, end_date):
            return False

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempt {attempt + 1} to set dates.")

                if not self._set_date_in_calendar(start_date, "dateStartSearch"):
                    continue
                if not self._set_date_in_calendar(end_date, "dateEndSearch"):
                    continue

                # Verify dates
                start_elem = self.driver.find_element(By.ID, "dateStartSearch")
                end_elem = self.driver.find_element(By.ID, "dateEndSearch")
                if (start_elem.get_attribute("value") == start_date and
                        end_elem.get_attribute("value") == end_date):
                    self.logger.info("Dates set successfully.")
                    return True
            except Exception as e:
                self.logger.error(f"Error in setting dates on attempt {attempt + 1}: {str(e)}")
            time.sleep(2)
        return False

# Utility function to calculate the last six months
def get_last_six_months():
    today = datetime.now()
    start_date = today - timedelta(days=6 * 30)  # Approximation for 6 months
    return start_date.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

# Example usage
if __name__ == "__main__":
    # Initialize the web driver and logger as per your setup
    driver = ...  # Your Selenium WebDriver instance
    logger = ...  # Your logging instance

    date_picker_handler = DatePickerHandler(driver, logger)

    # Get the last 6 months date range
    start_date, end_date = get_last_six_months()

    # Set the date range in the calendar
    if date_picker_handler._set_dates(start_date, end_date):
        logger.info(f"Successfully set dates from {start_date} to {end_date}")
    else:
        logger.error("Failed to set dates for the last six months.")

def _set_date_in_calendar(self, date_str: str, field_id: str) -> bool:
    """
    Set a specific date in the Zebra DatePicker widget.

    Args:
        date_str: Date in YYYY-MM-DD format.
        field_id: ID of the date input field.

    Returns:
        bool: True if the date was set successfully, False otherwise.
    """
    try:
        # Parse the target date
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        self.logger.info(f"Setting date to: {target_date.strftime('%Y-%m-%d')}")

        # Locate and click the input field to open the date picker
        date_input = self.wait.until(
            EC.presence_of_element_located((By.ID, field_id))
        )
        self.driver.execute_script("arguments[0].scrollIntoView(true);", date_input)
        time.sleep(0.5)
        date_input.click()
        time.sleep(1)  # Wait for date picker to open

        # Locate the active date picker
        datepicker = self.wait.until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR, 
                "div.Zebra_DatePicker:not(.dp_hidden)"
            ))
        )

        # Helper method: Parse caption text to a datetime object
        def parse_caption(caption_text: str) -> datetime:
            """Parse caption text in 'Month, YYYY' format."""
            if not caption_text.strip():
                raise ValueError("Caption text is empty or invalid")
            try:
                return datetime.strptime(caption_text.strip(), "%B, %Y")
            except ValueError as e:
                self.logger.error(f"Failed to parse caption: {caption_text}")
                raise e

        # Helper method: Navigate to the target month/year
        def navigate_to_target_date():
            """Navigate the date picker to the target month/year."""
            max_attempts = 24
            attempts = 0

            while attempts < max_attempts:
                try:
                    # Get the current caption
                    caption = datepicker.find_element(
                        By.CSS_SELECTOR, 
                        "table.dp_header td.dp_caption"
                    )
                    current_date = parse_caption(caption.text)

                    # Compare current and target dates
                    if current_date.year < target_date.year or \
                        (current_date.year == target_date.year and current_date.month < target_date.month):
                        next_button = datepicker.find_element(
                            By.CSS_SELECTOR, 
                            "table.dp_header td.dp_next"
                        )
                        next_button.click()
                    elif current_date.year > target_date.year or \
                        (current_date.year == target_date.year and current_date.month > target_date.month):
                        prev_button = datepicker.find_element(
                            By.CSS_SELECTOR, 
                            "table.dp_header td.dp_previous"
                        )
                        prev_button.click()
                    else:
                        return  # Reached the target month/year

                    time.sleep(0.5)
                    attempts += 1

                except Exception as e:
                    self.logger.warning(f"Attempt {attempts + 1}: Failed to navigate: {str(e)}")
                    attempts += 1
                    time.sleep(1)

            raise ValueError("Exceeded maximum attempts to navigate to target date")

        # Navigate to the correct month/year
        navigate_to_target_date()

        # Locate and select the target day
        days = datepicker.find_elements(
            By.CSS_SELECTOR,
            "table.dp_daypicker td:not(.dp_not_in_month):not(.dp_disabled)"
        )
        target_day_str = str(target_date.day)

        for day in days:
            if day.text.strip() == target_day_str:
                self.logger.info(f"Selecting day: {target_day_str}")
                self.driver.execute_script("arguments[0].scrollIntoView(true);", day)
                time.sleep(0.5)
                day.click()
                time.sleep(0.5)

                # Validate that the date was set correctly
                actual_value = date_input.get_attribute("value")
                expected_value = target_date.strftime("%Y-%m-%d")
                if actual_value == expected_value:
                    self.logger.info(f"Date set successfully: {expected_value}")
                    return True
                else:
                    self.logger.error(f"Validation failed. Expected: {expected_value}, Got: {actual_value}")
                    return False

        self.logger.error(f"Target day {target_day_str} not found")
        return False

    except Exception as e:
        self.logger.error(f"Error setting date: {str(e)}")
        return False
    
# def run(self, county: str, product: str, market: str,
    #         start_date: str, end_date: str, 
    #         entries: str = "100",
    #         output_file: str = "amis_data.csv") -> Optional[pd.DataFrame]:
    #     """Run the complete scraping process."""
    #     try:
    #         if not self.set_filters(county, market, product, start_date, end_date, entries):
    #             return None
                
    #         df = self.scrape_table()
    #         if df is not None and not df.empty:
    #             df.to_csv(output_file, index=False)
    #             self.logger.info(f"Data saved to {output_file}")
    #             return df
            
    #         return None

    #     except Exception as e:
    #         self.logger.error(f"Error during scraping process: {str(e)}")
    #         return None
            
    #     finally:
    #         self.driver.quit()
    #         self.logger.info("Browser closed")
    
    # scraper.run(
    #     county="Nairobi",
    #     product="Dry maize",  # Changed to match exact text from options
    #     market="Nyamakima",
    #     start_date="2024-01-01",
    #     end_date="2024-11-23",
    #     entries="100",
    #     output_file="amis_data.csv"
    # )

def scrape_table(self) -> Optional[pd.DataFrame]:
        """Scrape data from the results table."""
        try:
            self.logger.info("Starting table scraping")
            
            # Adding delay to allow the table to load completely
            time.sleep(3)  # Adjust the delay time as needed (e.g., 3 seconds)
        
            # Fetch headers
            headers = [th.text.strip() for th in 
                    self.driver.find_elements(By.CSS_SELECTOR, "table.table-bordered th")]
            self.logger.debug(f"Headers found: {headers}, count: {len(headers)}")
            
            # Fetch rows
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table.table-bordered tbody tr")
            data = []
            
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                cols_data = [col.text.strip() or None for col in cols]
                
                # Adjust for mismatched rows
                if len(cols_data) != len(headers):
                    self.logger.warning(
                        f"Row length mismatch: Expected {len(headers)}, got {len(cols_data)}. Adjusting..."
                    )
                    cols_data += [None] * (len(headers) - len(cols_data))  # Fill missing columns
                    cols_data = cols_data[:len(headers)]  # Truncate extra columns
                
                self.logger.debug(f"Row data: {cols_data}")
                data.append(cols_data)
            
            # Validate data
            if not data:
                self.logger.warning("No data found in table")
                return None
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=headers)
            df = df.dropna(axis=1, how='all')  # Drop entirely empty columns
            
            self.logger.info(f"Successfully scraped {len(df)} rows of data")
            return df

        except Exception as e:
            self.logger.error(f"Error scraping table: {str(e)}")
            return None

    def main():
        # Test configuration
        test_config = {
            'counties': ["Nairobi"],
            'products': ["Dry maize"],
            'markets': ["Nyamakima"],
            'start_date': "2023-01-01",
            'end_date': "2023-12-31",
            'resume': True
        }
        
        # Full configuration
        full_config = {
            'counties': ["Nairobi", "Mombasa", "Kisumu"],  # Add all counties
            'products': ["Dry maize", "Beans", "Rice"],     # Add all products
            'markets': ["Nyamakima", "City Market"],        # Add all markets
            'start_date': "2023-01-01",
            'end_date': "2023-12-31",
            'resume': True
        }
        
        try:
            # Initialize scraper
            scraper = AMISScraper(headless=True, timeout=120)
            
            # Run test configuration first
            scraper.logger.info("Starting test run...")
            test_df = scraper.run_all(
                counties=test_config['counties'],
                products=test_config['products'],
                markets=test_config['markets'],
                start_date=test_config['start_date'],
                end_date=test_config['end_date'],
                resume=test_config['resume']
            )
            
            if test_df is not None:
                scraper.logger.info("Test run completed successfully!")
                
                # If test run is successful, proceed with full configuration
                scraper.logger.info("Starting full run...")
                full_df = scraper.run_all(
                    counties=full_config['counties'],
                    products=full_config['products'],
                    markets=full_config['markets'],
                    start_date=full_config['start_date'],
                    end_date=full_config['end_date'],
                    resume=full_config['resume']
                )
                
                if full_df is not None:
                    scraper.logger.info("Full run completed successfully!")
                else:
                    scraper.logger.error("Full run failed to collect data")
            else:
                scraper.logger.error("Test run failed to collect data")
        
        except Exception as e:
            scraper.logger.critical(f"Critical error in main process: {str(e)}", exc_info=True)
        
        finally:
            try:
                scraper.driver.quit()
                scraper.logger.info("Browser closed successfully")
            except Exception as e:
                scraper.logger.error(f"Error closing browser: {str(e)}")


    def _save_intermediate_data(self, 
                              df: pd.DataFrame, 
                              county: str, 
                              market: str, 
                              product: str):
        """Save intermediate results to CSV."""
        filename = (
            f"amis_data_{county}_{market}_{product}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        filepath = os.path.join(self.dirs['exports'], filename)
        df.to_csv(filepath, index=False)
        self.logger.info(f"Saved intermediate data to {filepath}")

    def _save_final_results(self, all_data: List[pd.DataFrame]) -> pd.DataFrame:
        """Consolidate and save final results."""
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates
        final_df = final_df.drop_duplicates()
        
        # Save final consolidated CSV
        final_output_path = os.path.join(
            self.dirs['exports'],
            f"amis_data_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        final_df.to_csv(final_output_path, index=False)
        self.logger.info(f"Saved final consolidated data to {final_output_path}")
        
        return final_df



    from datetime import datetime, timedelta
import pandas as pd
import os
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

def compute_date_range(months_back=6):
    """Compute start and end dates for the last 'months_back' months."""
    today = datetime.now()
    start_date = today - timedelta(days=months_back * 30)  # Approximate 6 months
    return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

def scrape_with_dynamic_entries(driver, county, market, product, date_range):
    """
    Scrape data dynamically, adjusting entries per page until all data for a month is captured.
    """
    entry_options = [10, 50, 100, 500, 1500, 3000]  # Incrementally larger options
    for entry_limit in entry_options:
        try:
            set_entry_limit(driver, entry_limit)  # Adjust page entries
            apply_filters(driver, county, market, product)  # Apply filters for county, market, product

            # Scrape the current table
            df = scrape_current_table(driver)

            # Filter data by the required date range
            filtered_df = filter_data_by_date(df, date_range)

            # If filtered data covers the required range, return it
            if verify_month_coverage(filtered_df, date_range):
                return filtered_df
        except Exception as e:
            print(f"Failed at entry limit {entry_limit}: {e}")
    return None  # Return None if no data is captured

def set_entry_limit(driver, entry_limit):
    """Set the number of entries displayed per page."""
    dropdown = driver.find_element(By.CSS_SELECTOR, "selector-for-entries-dropdown")
    dropdown.click()
    option = driver.find_element(By.XPATH, f"//option[text()='{entry_limit}']")
    option.click()

def apply_filters(driver, county, market, product):
    """Apply filters for county, market, and product."""
    select_county(driver, county)
    select_market(driver, market)
    select_product(driver, product)

def scrape_current_table(driver):
    """Scrape the current table on the page into a DataFrame."""
    table = driver.find_element(By.CSS_SELECTOR, "selector-for-table")
    rows = table.find_elements(By.TAG_NAME, "tr")
    data = []
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "td")
        data.append([cell.text for cell in cells])
    return pd.DataFrame(data, columns=["Market", "Product", "Low Price", "High Price", "County", "Date"])

def filter_data_by_date(df, date_range):
    """Filter DataFrame rows to include only dates within the range."""
    start_date, end_date = date_range
    df['Date'] = pd.to_datetime(df['Date'])
    return df[(df['Date'] >= pd.to_datetime(start_date)) & (df['Date'] <= pd.to_datetime(end_date))]

def verify_month_coverage(df, date_range):
    """Verify that the DataFrame covers at least one full month in the range."""
    if df.empty:
        return False
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    months_covered = df['Date'].dt.to_period('M').unique()
    return all(pd.date_range(start_date, end_date, freq='M').to_period('M').isin(months_covered))

def save_to_csv(df, filename):
    """Save the filtered data to a CSV file."""
    filepath = f"data/{filename}"
    if not os.path.exists("data"):
        os.makedirs("data")
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def initialize_driver():
    """Initialize Selenium WebDriver."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode (optional)
    driver = webdriver.Chrome(options=options)
    return driver

# Workflow
start_date, end_date = compute_date_range(months_back=6)
date_range = (start_date, end_date)

# Initialize the driver
driver = initialize_driver()

# Example of what can be looped over
counties = ["Nairobi"]
markets = ["Nyamakima"]
products = ["Dry Maize"]

for county in counties:
    for market in markets:
        for product in products:
            fresh_data = scrape_with_dynamic_entries(driver, county, market, product, date_range)
            if fresh_data is not None:
                save_to_csv(fresh_data, f"{county}_{market}_{product}.csv")

# Quit driver after the scraping is done
driver.quit()
