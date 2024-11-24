from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import logging
from typing import Optional, Dict
from datetime import datetime

class AMISScraper:
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
        # Set up logging
        self.logger = self._setup_logger()
        self.logger.info("Initializing AMISScraper")
        
        # Configure Chrome options
        chrome_options = self._configure_chrome_options(headless)
        
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            self.url = url
            self.wait = WebDriverWait(self.driver, timeout)
            self.logger.info("Chrome WebDriver initialized successfully")
            
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize WebDriver: {str(e)}")
            raise

    def _setup_logger(self) -> logging.Logger:
        """Configure and return a logger instance."""
        logger = logging.getLogger('AMISScraper')
        logger.setLevel(logging.INFO)
        
        # Create console handler with formatting
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger

    def _configure_chrome_options(self, headless: bool) -> Options:
        """Configure Chrome options for optimal performance."""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        return chrome_options

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

    def _set_select_value(self, name: str, value: str, max_retries: int = 3, retry_delay: int = 5) -> bool:
        """
        Set value in a select element with improved handling and retry mechanism.
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
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        break
                    except StaleElementReferenceException:
                        time.sleep(1)
                        continue
                
                if not select_element:
                    raise Exception(f"Could not locate stable select element for {name}")
                
                # Scroll element into view
                self.driver.execute_script("arguments[0].scrollIntoView(true);", select_element)
                time.sleep(1)
                
                # Wait for options to be present and verify they're loaded
                options_present = self.wait.until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, f"{selector} option")) > 1
                )
                
                if not options_present:
                    self.logger.warning(f"No options found for {name}, retrying...")
                    time.sleep(retry_delay)
                    continue
                
                # Try different selection methods
                select = Select(select_element)
                options = [opt.text.strip() for opt in select.options]
                self.logger.info(f"Available options for {name}: {options}")
                
                # Method 1: Direct selection using Select class
                try:
                    select.select_by_visible_text(value)
                    time.sleep(1)
                    if select.first_selected_option.text.strip() == value:
                        self.logger.info(f"Successfully selected {value} using Select class")
                        return True
                except Exception as e:
                    self.logger.debug(f"Direct selection failed: {str(e)}")
                
                # Method 2: JavaScript selection
                try:
                    js_script = """
                    var select = arguments[0];
                    var value = arguments[1];
                    var options = select.options;
                    for(var i = 0; i < options.length; i++) {
                        if(options[i].text.trim() === value) {
                            select.selectedIndex = i;
                            select.dispatchEvent(new Event('change', { bubbles: true }));
                            return true;
                        }
                    }
                    return false;
                    """
                    result = self.driver.execute_script(js_script, select_element, value)
                    if result:
                        self.logger.info(f"Successfully selected {value} using JavaScript")
                        return True
                except Exception as e:
                    self.logger.debug(f"JavaScript selection failed: {str(e)}")
                
                # Method 3: Case-insensitive match
                for option in options:
                    if option.lower() == value.lower():
                        select.select_by_visible_text(option)
                        time.sleep(1)
                        if select.first_selected_option.text.strip().lower() == value.lower():
                            self.logger.info(f"Successfully selected {value} using case-insensitive match")
                            return True
                
                self.logger.warning(f"Attempt {attempt + 1} failed. Retrying after delay...")
                time.sleep(retry_delay)
                
            except Exception as e:
                self.logger.error(f"Error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return False
        
        self.logger.error(f"Failed to set {name} to {value} after {max_retries} attempts")
        return False
    
    def _set_date_in_calendar(self, date_str: str, field_id: str) -> bool:
        """
        Set date using the Zebra DatePicker interface with improved element selection.
        
        Args:
            date_str: Date in YYYY-MM-DD format
            field_id: ID of the date input field
        """
        try:
            # Parse the target date
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            
            # Click the input field to open the date picker
            date_input = self.wait.until(
                EC.presence_of_element_located((By.ID, field_id))
            )
            self.driver.execute_script("arguments[0].scrollIntoView(true);", date_input)
            time.sleep(0.5)
            date_input.click()
            time.sleep(1)  # Wait for picker to open
            
            # Find the active date picker - using a more robust selector
            datepicker = self.wait.until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR, 
                    "div.Zebra_DatePicker:not(.dp_hidden)"
                ))
            )
            
            def parse_caption(caption_text: str) -> datetime:
                """Parse the caption text in format 'Month, YYYY'."""
                if not caption_text or caption_text.isspace():
                    raise ValueError("Empty or whitespace caption")
                
                # The caption format is consistently "Month, YYYY" as seen in the HTML
                try:
                    return datetime.strptime(caption_text.strip(), "%B, %Y")
                except ValueError as e:
                    self.logger.error(f"Failed to parse caption: {caption_text}")
                    raise e
            
            # Navigate to correct month/year
            max_attempts = 24  # Prevent infinite loop
            attempts = 0
            
            while attempts < max_attempts:
                try:
                    # Using more specific selector for the caption
                    caption = datepicker.find_element(
                        By.CSS_SELECTOR, 
                        "table.dp_header td.dp_caption"
                    )
                    current_text = caption.text.strip()
                    current = parse_caption(current_text)
                    
                    if current.year < target_date.year or (current.year == target_date.year and current.month < target_date.month):
                        next_button = datepicker.find_element(
                            By.CSS_SELECTOR, 
                            "table.dp_header td.dp_next"
                        )
                        next_button.click()
                    elif current.year > target_date.year or (current.year == target_date.year and current.month > target_date.month):
                        prev_button = datepicker.find_element(
                            By.CSS_SELECTOR, 
                            "table.dp_header td.dp_previous"
                        )
                        prev_button.click()
                    else:
                        break
                        
                    time.sleep(0.5)
                    attempts += 1
                    
                except Exception as e:
                    self.logger.warning(f"Navigation attempt {attempts} failed: {str(e)}")
                    time.sleep(1)
                    attempts += 1
                    if attempts >= max_attempts:
                        raise ValueError("Failed to navigate to target month after maximum attempts")
            
            # Find and click the target day using more specific selectors
            days = datepicker.find_elements(
                By.CSS_SELECTOR,
                "table.dp_daypicker td:not(.dp_not_in_month):not(.dp_disabled)"
            )
            target_day_str = str(target_date.day)
            
            for day in days:
                if day.text.strip() == target_day_str:
                    # Check if day is already selected
                    if 'dp_selected' in day.get_attribute('class').split():
                        self.logger.info(f"Day {target_day_str} is already selected")
                        return True
                    
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", day)
                    time.sleep(0.5)
                    day.click()
                    time.sleep(0.5)
                    
                    # Verify the date was set correctly
                    actual_value = date_input.get_attribute("value")
                    expected_value = target_date.strftime("%Y-%m-%d")
                    if actual_value == expected_value:
                        self.logger.info(f"Successfully set date to {expected_value}")
                        return True
                    break
            
            self.logger.error(f"Failed to set date to {date_str}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error setting date in calendar: {str(e)}")
            return False

    def _wait_for_datepicker_update(self) -> bool:
        """Helper method to wait for date picker updates to complete."""
        try:
            # Wait for any animations or updates to complete
            time.sleep(0.5)
            return True
        except Exception as e:
            self.logger.error(f"Error waiting for date picker update: {str(e)}")
            return False

    def _set_dates(self, start_date: str, end_date: str, max_retries: int = 3) -> bool:
        """Set start and end dates using the calendar widget with retry mechanism."""
        if not self._validate_dates(start_date, end_date):
            return False

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempt {attempt + 1} to set dates")
                
                # Set start date
                if not self._set_date_in_calendar(start_date, "dateStartSearch"):
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    return False
                
                time.sleep(1)  # Wait between setting dates
                
                # Set end date
                if not self._set_date_in_calendar(end_date, "dateEndSearch"):
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
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
                
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                    
            except Exception as e:
                self.logger.error(f"Error in date setting attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return False
                
        return False

    def set_filters(self, county: str, market: str, product: str, 
                   start_date: str, end_date: str, entries: str = "100") -> bool:
        """Set search filters with improved error handling and sequence."""
        try:
            self.logger.info(f"Setting filters for {product} in {market}, {county}")
            self.driver.get(self.url)
            
            # Wait for initial page load
            self._wait_for_page_load()
            time.sleep(2)  # Additional wait after page load
            
            # Set filters in sequence with proper delays
            filter_sequence = [
                ("county[]", county, 5),
                ("market[]", market, 3),
                ("product[]", product, 2),
                ("per_page", entries, 2)
            ]
            
            for name, value, delay in filter_sequence:
                if not self._set_select_value(name, value):
                    self.logger.error(f"Failed to set {name} to {value}")
                    return False
                time.sleep(delay)
            
            # Set dates after all dropdowns
            if not self._set_dates(start_date, end_date):
                return False
            
            # Click filter with retry
            for attempt in range(3):
                try:
                    if self._click_filter_button():
                        return True
                    time.sleep(2)
                except Exception as e:
                    self.logger.error(f"Error clicking filter (attempt {attempt + 1}): {str(e)}")
                    if attempt < 2:
                        time.sleep(2)
                        continue
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error in set_filters: {str(e)}")
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
            filter_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit']"))
            )
            filter_button.click()
            
            # Wait for table to load or error message
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
                self.logger.info("Results table loaded successfully")
                return True
            except TimeoutException:
                self.logger.error("No results table found after filtering")
                return False
                
        except Exception as e:
            self.logger.error(f"Error clicking filter button: {str(e)}")
            return False

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

    def run(self, county: str, product: str, market: str,
            start_date: str, end_date: str, 
            entries: str = "100",
            output_file: str = "amis_data.csv") -> Optional[pd.DataFrame]:
        """Run the complete scraping process."""
        try:
            if not self.set_filters(county, market, product, start_date, end_date, entries):
                return None
                
            df = self.scrape_table()
            if df is not None and not df.empty:
                df.to_csv(output_file, index=False)
                self.logger.info(f"Data saved to {output_file}")
                return df
            
            return None

        except Exception as e:
            self.logger.error(f"Error during scraping process: {str(e)}")
            return None
            
        finally:
            self.driver.quit()
            self.logger.info("Browser closed")

if __name__ == "__main__":
    scraper = AMISScraper(headless=False, timeout=120)
    scraper.run(
        county="Nairobi",
        product="Dry maize",  # Changed to match exact text from options
        market="Nyamakima",
        start_date="2024-01-01",
        end_date="2024-11-23",
        entries="100",
        output_file="amis_data.csv"
    )