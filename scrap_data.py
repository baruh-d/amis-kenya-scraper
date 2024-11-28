from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
import functools
import socket
import ssl
from urllib3.exceptions import MaxRetryError, NewConnectionError
from requests.exceptions import ConnectionError, Timeout
from dateutil.relativedelta import relativedelta
import pandas as pd
import time
import logging
import json
import os
import re
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta

class AMISScraper:
    """Enhanced Agricultural Market Information System (AMIS) scraper with improved reliability."""
    # Configuration constants
    DELAYS = {
        'between_requests': 2,
        'after_filter': 3,
        'after_page_load': 1,
        'between_retries': 5
    }
    
    MAX_ENTRIES = 3
    BATCH_SIZE = 1000 # Number of rows to process at once for memory efficiency
    
    """
    A scraper for the Agricultural Market Information System (AMIS) website.
    
    Attributes:
        url (str): The base URL for the AMIS market search page
        driver (webdriver.Chrome): Selenium WebDriver instance
        wait (WebDriverWait): WebDriverWait instance for handling dynamic elements
        logger (logging.Logger): Logger instance for tracking operations
    """
    
    def __init__(self, url: str = "https://amis.co.ke/site/market_search/market_search", 
                 headless: bool = False, 
                 timeout: int = 40):
        """
        Initialize the AMIS scraper.
        
        Args:
            url (str): The base URL for scraping
            headless (bool): Whether to run Chrome in headless mode
            timeout (int): Maximum wait time in seconds for page elements
        """
        self.url = url
        self.entry_options = [10, 50, 100, 1000, 1500, 3000]
        self.timeout = timeout
        
        # set up directories
        self.base_dir = "amis_data"
        self.dirs = {
            'exports': os.path.join(self.base_dir, 'exports'),
            'logs': os.path.join(self.base_dir, 'logs'),
            'progress': os.path.join(self.base_dir, 'progress')
        }
        self._setup_directories()
        
        # Set up logging
        self.logger = self._setup_logger()
        self.logger.info("Initializing AMISScraper")
        
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
                EC.presence_of_element_located((By.CSS_SELECTOR, "selPerPage"))
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

    def _wait_for_page_load(self):
        """Wait for page to be fully loaded."""
        try:
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            self.logger.info("Page loaded successfully")
        except TimeoutException:
            self.logger.error("Timeout waiting for page to load")
            raise

    def _set_select_value(self, name: str, value: str, max_retries: int = 3, retry_delay: float = 5.0) -> bool:
        """
        Set value in a select element with improved handling and retry mechanism.
        
        :param name: Name attribute of the select element
        :param value: Value to be selected
        :param max_retries: Maximum number of retry attempts
        :param retry_delay: Delay between retry attempts
        :return: Boolean indicating successful selection
        """
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempt {attempt + 1} to set {name} to {value}")
                
                # Handle array-style names in CSS selector
                escaped_name = name.replace("[]", "\\[\\]")
                selector = f"select[name='{escaped_name}']"
                
                # Wait for select element with retry on StaleElementReferenceException
                select_element = None
                for _ in range(3):
                    try:
                        select_element = self.wait.until(
                            lambda d: d.find_element(By.CSS_SELECTOR, selector)
                        )
                        break
                    except StaleElementReferenceException:
                        self.logger.debug(f"Stale element encountered for {name}, retrying...")
                        time.sleep(1)
                        continue
                
                if not select_element:
                    raise NoSuchElementException(f"Could not locate stable select element for {name}")
                
                # Scroll element into view
                self.driver.execute_script("arguments[0].scrollIntoView(true);", select_element)
                time.sleep(1)
                
                # Wait for options to be present and verify they're loaded
                options = select_element.find_elements(By.CSS_SELECTOR, "option")
                if len(options) <= 1:
                    self.logger.warning(f"Insufficient options found for {name}, retrying...")
                    time.sleep(retry_delay)
                    continue

                # Try selection methods
                select = Select(select_element)
                if self._select_by_visible_text(select, value):
                    return True
                if self._select_by_js(self.driver, select_element, value):
                    return True
                if self._select_by_case_insensitive(select, value):
                    return True
                    
                self.logger.warning(f"Attempt {attempt + 1} failed. Retrying...")
                time.sleep(retry_delay)

            except (NoSuchElementException, WebDriverException) as e:
                self.logger.error(f"WebDriver error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return False
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                return False

        self.logger.error(f"Failed to set {name} to {value} after {max_retries} attempts")
        return False

    def _select_by_visible_text(self, select: Select, value: str) -> bool:
        """
        Attempt to select an option by exact visible text.
        
        :param select: Selenium Select object
        :param value: Value to select
        :return: Boolean indicating successful selection
        """
        try:
            select.select_by_visible_text(value)
            if select.first_selected_option.text.strip() == value:
                self.logger.info(f"Successfully selected {value} by visible text")
                return True
        except Exception as e:
            self.logger.debug(f"Selection by visible text failed: {str(e)}")
        return False

    def _select_by_js(self, driver, select_element, value: str) -> bool:
        """
        Attempt to select an option using JavaScript.
        
        :param driver: Selenium WebDriver
        :param select_element: WebElement of the select
        :param value: Value to select
        :return: Boolean indicating successful selection
        """
        try:
            js_script = """
            var select = arguments[0];
            var value = arguments[1];
            var options = select.options;
            for (var i = 0; i < options.length; i++) {
                if (options[i].text.trim() === value) {
                    select.selectedIndex = i;
                    select.dispatchEvent(new Event('change', { bubbles: true }));
                    return true;
                }
            }
            return false;
            """
            result = driver.execute_script(js_script, select_element, value)
            if result:
                self.logger.info(f"Successfully selected '{value}' using JavaScript")
                return True
            else:
                self.logger.warning(f"Option '{value}' not found in select via JS")
        except Exception as e:
            self.logger.debug(f"JavaScript selection failed for '{value}': {str(e)}")
        return False

    def _select_by_case_insensitive(self, select: Select, value: str) -> bool:
        """
        Attempt to select an option using case-insensitive matching.
        
        :param select: Selenium Select object
        :param value: Value to select
        :return: Boolean indicating successful selection
        """
        try:
            value_lower = value.strip().lower()  # Normalize the value to lower case
            for option in select.options:
                option_text = option.text.strip()
                if option_text.lower() == value_lower:
                    select.select_by_visible_text(option.text)
                    if select.first_selected_option.text.strip().lower() == value_lower:
                        self.logger.info(f"Successfully selected '{value}' (case-insensitive match)")
                        return True
            self.logger.warning(f"Option '{value}' not found in select (case-insensitive match)")
        except Exception as e:
            self.logger.debug(f"Case-insensitive selection failed for '{value}': {str(e)}")
        return False

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
                            self.logger.debug(f"Navigating to next month: {current_date.strftime('%B, %Y')}")
                        elif current_date.year > target_date.year or \
                            (current_date.year == target_date.year and current_date.month > target_date.month):
                            prev_button = datepicker.find_element(
                                By.CSS_SELECTOR, 
                                "table.dp_header td.dp_previous"
                            )
                            prev_button.click()
                            self.logger.debug(f"Navigating to previous month: {current_date.strftime('%B, %Y')}")
                        else:
                            self.logger.info(f"Reached target month/year: {target_date.strftime('%B, %Y')}")
                            return  # Reached the target month/year

                        time.sleep(0.75)  # Increased sleep time to account for page load
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

    def _wait_for_datepicker_update(self) -> bool:
        """Helper method to wait for date picker updates to complete."""
        try:
            # Wait for the date picker to be in an updated state (e.g., waiting for the disappearance of loading indicators)
            self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.loading-indicator")))
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.Zebra_DatePicker:not(.dp_hidden)")))
            return True
        except Exception as e:
            self.logger.error(f"Error waiting for date picker update: {str(e)}")
            return False

    def _retry_set_date_in_calendar(self, date_str: str, field_id: str, max_retries: int = 3) -> bool:
        """Attempt to set a date with retries."""
        for attempt in range(max_retries):
            if self._set_date_in_calendar(date_str, field_id):
                return True
            self.logger.warning(f"Attempt {attempt + 1} to set date failed. Retrying...")
            time.sleep(2)  # Wait before retrying
        return False

    def _set_dates(self, start_date: str, end_date: str, max_retries: int = 3) -> bool:
        """Set start and end dates using the calendar widget with retry mechanism."""
        if not self._validate_dates(start_date, end_date):
            return False

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempt {attempt + 1} to set dates")
                
                # Set start date
                if not self._retry_set_date_in_calendar(start_date, "dateStartSearch", max_retries):
                    return False
                
                time.sleep(1)  # Wait between setting dates
                
                # Set end date
                if not self._retry_set_date_in_calendar(end_date, "dateEndSearch", max_retries):
                    return False

                # Verify the dates were set correctly
                start_elem = self.driver.find_element(By.ID, "dateStartSearch")
                end_elem = self.driver.find_element(By.ID, "dateEndSearch")
                
                actual_start = start_elem.get_attribute("value")
                actual_end = end_elem.get_attribute("value")
                
                # Convert dates to consistent format for comparison
                expected_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                expected_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                
                if actual_start == expected_start and actual_end == expected_end:
                    self.logger.info("Dates set and verified successfully")
                    return True
                    
                self.logger.warning(
                    f"Date verification failed on attempt {attempt + 1}. "
                    f"Expected: {expected_start} to {expected_end}, "
                    f"Got: {actual_start} to {actual_end}"
                )
                
            except Exception as e:
                self.logger.error(f"Error in date setting attempt {attempt + 1}: {str(e)}")
            
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait before retrying
        
        return False

    def set_filters(self, county: str, market: str, product: str, 
                    start_date: str, end_date: str, entries: str = "100") -> bool:
        """Set search filters with improved error handling and sequence."""
        try:
            self.logger.info(f"Setting filters for {product} in {market}, {county}")
            self.driver.get(self.url)

            # Wait for initial page load
            self._wait_for_page_load()

            # Set filters in sequence with proper delays
            filter_sequence = [
                ("county[]", county, 5),
                ("market[]", market, 3),
                ("product[]", product, 2),
                ("per_page", entries, 2)
            ]
            
            # Set filter values one by one
            if not self._apply_filters_in_sequence(filter_sequence):
                return False

            # Set dates after all dropdowns
            if not self._set_dates(start_date, end_date):
                return False

            # Click filter button with retry logic
            return self._retry_click_filter_button()

        except Exception as e:
            self.logger.error(f"Error in set_filters: {str(e)}")
            return False

    def _wait_for_page_load(self):
        """Wait for the page to load completely by checking for specific element."""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "page-content"))  # Adjust selector as necessary
            )
        except Exception as e:
            self.logger.error(f"Error while waiting for page load: {str(e)}")
            raise

    def _apply_filters_in_sequence(self, filter_sequence):
        """Apply filter values in sequence."""
        for name, value, delay in filter_sequence:
            if not self._set_select_value(name, value):
                self.logger.error(f"Failed to set {name} to {value}")
                return False
            time.sleep(delay)
        return True

    def _retry_click_filter_button(self, max_retries=3):
        """Retry clicking the filter button with a maximum number of retries."""
        for attempt in range(max_retries):
            try:
                if self._click_filter_button():
                    self.logger.info("Filter button clicked successfully.")
                    return True
                time.sleep(2)
            except Exception as e:
                self.logger.error(f"Error clicking filter (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
        self.logger.error("Failed to click the filter button after multiple attempts.")
        return False

    def _validate_dates(self, start_date: str, end_date: str) -> bool:
        """Validate date formats and ranges."""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            if end < start:
                self.logger.error("End date cannot be earlier than start date")
                return False
            return True
        except ValueError:
            self.logger.error("Invalid date format. Use YYYY-MM-DD")
            return False
        
    def _click_filter_button(self) -> bool:
        """Click the filter button with proper error handling."""
        try:
            # Wait for the filter button to be clickable
            filter_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit']"))
            )
            filter_button.click()
            
            # Wait for results table to load or error message
            try:
                # Wait for the table or any loading indicator to appear
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
                self.logger.info("Results table loaded successfully")
                return True
            except TimeoutException:
                self.logger.error("No results table found after filtering")
                return False
            
        except Exception as e:
            self.logger.error(f"Error clicking filter button: {str(e)}. URL: {self.driver.current_url}")
            return False
    def scrape_table(self) -> Optional[pd.DataFrame]:
        """Scrape the data from the table and handle pagination dynamically."""
        try:
            all_data = []  # List to hold all the scraped data
            
            # Wait for the table to load
            self._wait_for_table_load()

            # Check if data is available
            if not self._is_data_available():
                self.logger.warning("No data available on the current page")
                return None
            
            # Start scraping data for the first page
            page_num = 1
            while True:
                # Wait for the table on the current page to load
                self._wait_for_table_load()

                # Extract data from the table on this page
                page_data = self._extract_table_data(page_num)
                if page_data:
                    all_data.append(page_data)
                
                # Check if there's a next page, and if so, go to the next one
                if self._has_next_page():
                    page_num += 1
                    self._go_to_next_page()
                    time.sleep(2)  # Adjust delay as necessary
                else:
                    break

            # If we have scraped data, convert it into a DataFrame
            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                return df
            else:
                self.logger.warning("No data was scraped from the table")
                return None

        except Exception as e:
            self.logger.error(f"Error in scrape_table: {str(e)}")
            return None

    def _wait_for_table_load(self):
        """Wait for the table to load by checking the presence of a specific element."""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))  # Adjust selector if necessary
            )
        except Exception as e:
            self.logger.error(f"Error waiting for table load: {str(e)}")
            raise

    def _is_data_available(self) -> bool:
        """Check if data exists in the table."""
        try:
            table = self.driver.find_element(By.CSS_SELECTOR, "table")  # Adjust CSS selector if needed
            rows = table.find_elements(By.TAG_NAME, "tr")
            return len(rows) > 1  # Data rows should be more than 1 (header row)
        except Exception as e:
            self.logger.error(f"Error checking for data availability: {str(e)}")
            return False

    def _extract_table_data(self, page_num: int) -> pd.DataFrame:
        """Extract data from the table on the current page."""
        try:
            table = self.driver.find_element(By.CSS_SELECTOR, "table")  # Adjust CSS selector if needed
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            # Extract headers
            headers = [header.text for header in rows[0].find_elements(By.TAG_NAME, "th")]
            
            # Extract rows of data
            data = []
            for row in rows[1:]:  # Skip header row
                columns = row.find_elements(By.TAG_NAME, "td")
                row_data = [column.text for column in columns]
                data.append(row_data)
            
            # Convert the extracted data into a DataFrame
            df = pd.DataFrame(data, columns=headers)
            self.logger.info(f"Extracted data for page {page_num}")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting table data: {str(e)}")
            return None

    def _has_next_page(self) -> bool:
        """Check if there is a next page of results."""
        try:
            # Find the next page button (adjust selector as necessary)
            next_button = self.driver.find_element(By.CSS_SELECTOR, ".pagination .next")
            return "disabled" not in next_button.get_attribute("class")
        except Exception as e:
            self.logger.error(f"Error checking for next page: {str(e)}")
            return False

    def _go_to_next_page(self):
        """Click the next page button to load the next set of results."""
        try:
            next_button = self.driver.find_element(By.CSS_SELECTOR, ".pagination .next")
            next_button.click()
            self.logger.info("Navigated to next page")
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {str(e)}")

    def quit_driver(self):
        """Properly close the driver instance."""
        try:
            self.driver.quit()
            self.logger.info("Driver closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing driver: {str(e)}")

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """Save the filtered data to a CSV file."""
        filepath = f"data/{filename}"
        if not os.path.exists("data"):
            os.makedirs("data")
        df.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")

    def scrape_by_month(self, start_date: str, end_date: str, county: str, market: str, product: str, output_file: str = "scraped_data.csv") -> pd.DataFrame:
        """Scrape data month by month within a given date range and save to CSV."""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        all_monthly_data = []

        while start <= end:
            # Adjust the filter for the specific month
            month_start_date = start.strftime("%Y-%m-%d")
            month_end_date = (start + relativedelta(day=31)).strftime("%Y-%m-%d")
            
            # Set the filters for the current month
            if not self.set_filters(county, market, product, month_start_date, month_end_date):
                print(f"Skipping month {month_start_date} due to filter issues")
                start += relativedelta(months=1)
                continue

            # Scrape the table for this month
            df = self.scrape_table()
            if df is not None:
                all_monthly_data.append(df)
            
            # Move to the next month
            start += relativedelta(months=1)

        # Combine all monthly data
        if all_monthly_data:
            final_df = pd.concat(all_monthly_data, ignore_index=True)
            # Save the DataFrame to a CSV file
            self.save_to_csv(final_df, output_file)
            return final_df
        else:
            print("No data collected for the given date range")
            return None
        
def network_retry(max_attempts=3, delay=5):
    """
    Decorator for handling network-related errors with exponential backoff.
    
    Args:
        max_attempts (int): Maximum number of retry attempts
        delay (int): Base delay between retries in seconds
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except (
                    ConnectionError, 
                    socket.error, 
                    ssl.SSLError, 
                    MaxRetryError, 
                    NewConnectionError, 
                    Timeout,
                    TimeoutError
                ) as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise  # Re-raise the last exception if all attempts fail
                    
                    logger = args[0].logger if args else None
                    if logger:
                        logger.warning(
                            f"Network error on attempt {attempts}: {str(e)}. "
                            f"Retrying in {delay * attempts} seconds..."
                        )
                    
                    time.sleep(delay * attempts)  # Exponential backoff
        return wrapper
    return decorator

def run_all(
    self, 
    counties: List[str], 
    products: List[str], 
    markets: List[str], 
    start_date: str, 
    end_date: str, 
    resume: bool = False
) -> Optional[pd.DataFrame]:
    """
    Systematically scrape data across multiple configurations with progress tracking.
    
    Args:
        counties (List[str]): List of counties to scrape
        products (List[str]): List of products to scrape
        markets (List[str]): List of markets to scrape
        start_date (str): Start date for data collection
        end_date (str): End date for data collection
        resume (bool): Whether to resume from last saved progress
    
    Returns:
        Optional[pd.DataFrame]: Consolidated DataFrame of scraped data
    """
    try:
        # If resuming, load the last saved progress
        if resume and self.progress['last_county']:
            start_index = {
                'counties': counties.index(self.progress['last_county']) if self.progress['last_county'] in counties else 0,
                'markets': markets.index(self.progress['last_market']) if self.progress['last_market'] in markets else 0,
                'products': products.index(self.progress['last_product']) if self.progress['last_product'] in products else 0
            }
        else:
            start_index = {'counties': 0, 'markets': 0, 'products': 0}
        
        all_data = []
        
        # Nested loops for comprehensive scraping
        for county_idx in range(start_index['counties'], len(counties)):
            county = counties[county_idx]
            
            for market_idx in range(start_index['markets'], len(markets)):
                market = markets[market_idx]
                
                for product_idx in range(start_index['products'], len(products)):
                    product = products[product_idx]
                    
                    self.logger.info(f"Scraping: {county}, {market}, {product}")
                    
                    # Apply error handling and retry logic
                    try:
                        # Set filters and scrape
                        if not self.set_filters(county, market, product, start_date, end_date):
                            self.logger.warning(f"Skipping: {county}, {market}, {product} - Filter setup failed")
                            continue
                        
                        df = self.scrape_table()
                        
                        if df is not None and not df.empty:
                            df['County'] = county
                            df['Market'] = market
                            df['Product'] = product
                            all_data.append(df)
                            
                            # Save progress after successful scrape
                            self._save_progress(county, market, product)
                        
                    except Exception as e:
                        self.logger.error(f"Error scraping {county}, {market}, {product}: {str(e)}")
                        continue
                    
                    # Reset product loop index after complete processing
                    start_index['products'] = 0
                
                # Reset market loop index after complete processing
                start_index['markets'] = 0
            
            # Reset county loop index after complete processing
            start_index['counties'] = 0
        
        # Consolidate all scraped data
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            self.save_to_csv(final_df, f"amis_scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            return final_df
        
        self.logger.warning("No data collected across all configurations")
        return None
    
    except Exception as e:
        self.logger.critical(f"Critical error in run_all: {str(e)}", exc_info=True)
        return None

# Add the method to the class
AMISScraper.run_all = run_all

# Main function to run the scraper
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

# Ensure main() is called only when the script is run directly
if __name__ == "__main__":
    main()
