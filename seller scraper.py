import time
import re
import random
import logging
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("seller_scraper.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

PAID_SIGNALS = [
    "$", "usd", "€", "£", "buy now", "purchase", "add to cart",
    "enroll", "get access", "one-time", "lifetime access",
    "per month", "per year", "checkout", "price"
]

FREE_SIGNALS = [
    "free download", "full course free", "pirated",
    "telegram", "torrent", "cracked", "nulled", "free"
]

BAD_DOMAINS = [
    "amazon", "wikipedia", "facebook", "instagram",
    "linkedin", "twitter", "youtube", "goodreads",
    "reddit", "pinterest", "tiktok", "snapchat",
    "google", "bing", "yahoo", "apple", "audible",
    "udemy", "skillshare", "coursera", "edx"
]

EMAIL_PRIORITY_PREFIXES = [
    "info@", "contact@", "hello@", "support@",
    "media@", "press@", "team@", "hi@"
]

JUNK_NAME_KEYWORDS = [
    "visit the", "list of", "how to", "the best",
    "free download", "full course", "shortcut",
    "flashbooks", "zip reads", "getflashnotes"
]

CONTACT_PAGE_KEYWORDS = ["contact", "about", "reach", "connect", "work-with"]


def human_delay(min_s: float = 2, max_s: float = 4):
    time.sleep(random.uniform(min_s, max_s))


def is_junk_name(name: str) -> bool:
    n = (name or "").lower().strip()
    if len(n) < 2:
        return True
    return any(k in n for k in JUNK_NAME_KEYWORDS)


def safe_get(driver, url: str, retries: int = 3, timeout: int = 20) -> bool:
    for i in range(retries):
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            return True
        except TimeoutException:
            log.warning(f"Timeout attempt {i+1}: {url}")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            return True
        except WebDriverException as e:
            msg = str(e).lower()
            if "unsupported protocol" in msg or "invalid argument" in msg:
                log.warning(f"Bad URL skipped: {url}")
                return False
            log.warning(f"Retry {i+1} for {url}: {e}")
            time.sleep(2 + i * 2)
        except Exception as e:
            log.warning(f"Retry {i+1} unknown error: {e}")
            time.sleep(2 + i * 2)
    return False


def wait_for_page(driver, timeout: int = 10):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        pass


def is_good_url(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lower()
        return bool(domain) and not any(b in domain for b in BAD_DOMAINS)
    except Exception:
        return False


def clean_google_url(href: str) -> str:
    if not href or not href.startswith("http"):
        return ""
    if "google.com/url" in href:
        try:
            parsed = parse_qs(urlparse(href).query)
            real = parsed.get("q", [""])[0]
            if real.startswith("http"):
                return real
        except Exception:
            pass
        return ""
    if "google.com" in href or "google.co" in href:
        return ""
    return href


def get_links_safe(driver) -> list:
    hrefs = []
    try:
        elements = driver.find_elements(By.TAG_NAME, "a")
        for el in elements:
            try:
                href = el.get_attribute("href")
                text = el.text.lower()
                if href:
                    hrefs.append((href, text))
            except Exception:
                continue
    except Exception as e:
        log.debug(f"Link extraction error: {e}")
    return hrefs


def extract_google_result_urls(driver, max_results: int = 5) -> list:
    urls, seen = [], set()
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, "div#search a[href]")
        for el in elements:
            href = clean_google_url(el.get_attribute("href") or "")
            if href and href not in seen and is_good_url(href):
                urls.append(href)
                seen.add(href)
    except Exception:
        pass
    return urls[:max_results]


def google_search(driver, query: str, max_results: int = 5) -> list:
    if not safe_get(driver, "https://www.google.com"):
        return []
    human_delay(1, 2)
    try:
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.RETURN)
        human_delay(2, 4)
        wait_for_page(driver)
    except TimeoutException:
        return []
    return extract_google_result_urls(driver, max_results)


def clean_email(raw: str) -> str:
    raw = raw.lower().strip()
    match = re.match(r"\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b", raw)
    return match.group(1) if match else ""


def is_valid_email(email: str) -> bool:
    email = email.lower().strip()
    if not re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$", email):
        return False
    junk_patterns = ["example", "test", "fake", ".png", ".jpg", ".gif", "noreply", "domain.com", "youremail"]
    return (not any(x in email for x in junk_patterns)) and len(email) <= 80


def extract_emails(text: str, html: str) -> list:
    emails = set()
    for e in re.findall(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", text):
        cleaned = clean_email(e)
        if cleaned:
            emails.add(cleaned)
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "mailto:" in href.lower():
                cleaned = clean_email(href.lower().replace("mailto:", "").split("?")[0].strip())
                if cleaned:
                    emails.add(cleaned)
    except Exception:
        pass
    return [e for e in emails if is_valid_email(e)]


def score_email(email: str, domain: str) -> int:
    score = 0
    email_lower = email.lower()
    clean_domain = domain.replace("www.", "").split(".")[0]
    if clean_domain and clean_domain in email_lower:
        score += 5
    if any(email_lower.startswith(prefix) for prefix in EMAIL_PRIORITY_PREFIXES):
        score += 3
    if any(x in email_lower for x in ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]):
        score -= 2
    if len(email_lower.split("@")[0]) < 3:
        score -= 3
    return score


def scrape_gumroad_discover(driver, max_products: int = 50) -> list:
    product_urls = set()
    if not safe_get(driver, "https://gumroad.com/discover"):
        return []
    wait_for_page(driver)
    human_delay(2, 3)

    scrolls = random.randint(6, 10)
    for _ in range(scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        human_delay(1, 2)

        anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/l/"]')
        anchors += driver.find_elements(By.CSS_SELECTOR, 'a[href*="gumroad.com/l/"]')
        for a in anchors:
            href = a.get_attribute("href") or ""
            if not href:
                continue
            if "/l/" in href and "gumroad.com" in href:
                product_urls.add(href.split("?")[0].rstrip("/"))
        if len(product_urls) >= max_products:
            break

    return list(product_urls)[:max_products]


def validate_seller(driver, product_url: str) -> bool:
    if not safe_get(driver, product_url):
        return False
    human_delay(1, 2)
    wait_for_page(driver)

    try:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        text = soup.get_text(" ", strip=True).lower()
    except Exception:
        return False

    if any(sig in text for sig in FREE_SIGNALS):
        return False

    price_text = " ".join([el.get_text(" ", strip=True).lower() for el in soup.select("[class*='price'], [data-testid*='price']")])
    has_price_symbol = any(sym in text for sym in ["$", "usd", "€", "£"])
    if not has_price_symbol and not any(sig in price_text for sig in ["$", "usd", "€", "£"]):
        return False

    if re.search(r"\b(0|0\.00)\s*(usd|\$)?\b", text) or "free" in price_text:
        return False

    return any(sig in text for sig in PAID_SIGNALS)


def extract_creator_info(driver, product_url: str) -> dict:
    info = {"name": "", "store_url": "", "external_links": []}
    if not safe_get(driver, product_url):
        return info
    wait_for_page(driver)
    human_delay(1, 2)

    try:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        text = soup.get_text(" ", strip=True)

        store_url = ""
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = urljoin(product_url, href)
            p = urlparse(href)
            if "gumroad.com" in p.netloc and "/l/" not in p.path and p.netloc != "gumroad.com":
                store_url = f"{p.scheme}://{p.netloc}"
                break

        parsed_product = urlparse(product_url)
        if not store_url and parsed_product.netloc.endswith(".gumroad.com"):
            store_url = f"{parsed_product.scheme}://{parsed_product.netloc}"

        creator_name = ""
        for sel in ["[class*='creator']", "[class*='seller']", "[class*='author']", "[data-testid*='creator']"]:
            el = soup.select_one(sel)
            if el:
                candidate = el.get_text(strip=True)
                if candidate and not is_junk_name(candidate):
                    creator_name = candidate
                    break

        if not creator_name:
            match = re.search(r"\bby\s+([A-Za-z0-9][A-Za-z0-9\s._-]{1,60})", text, re.IGNORECASE)
            if match:
                candidate = match.group(1).strip(" .,-|")
                if not is_junk_name(candidate):
                    creator_name = candidate

        if not creator_name and store_url:
            creator_name = urlparse(store_url).netloc.split(".")[0]

        external_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href.startswith("http"):
                href = urljoin(product_url, href)
            domain = urlparse(href).netloc.lower()
            if domain and "gumroad.com" not in domain and "google" not in domain:
                external_links.append(href)

        info["name"] = creator_name.strip()
        info["store_url"] = store_url
        info["external_links"] = list(dict.fromkeys(external_links))[:10]
    except Exception as e:
        log.warning(f"Creator extraction failed for {product_url}: {e}")

    return info


def find_external_profiles(driver, creator_name: str) -> dict:
    profiles = {"website": ""}
    if not creator_name or is_junk_name(creator_name):
        return profiles

    website_urls = google_search(driver, query=f'"{creator_name}" official website', max_results=3)
    for url in website_urls:
        domain = urlparse(url).netloc.lower()
        if "gumroad.com" in domain:
            continue
        if not any(bad in domain for bad in BAD_DOMAINS):
            profiles["website"] = url
            break
    return profiles


def scrape_contact(driver, url: str) -> tuple:
    if not url or not safe_get(driver, url):
        return [], ""
    wait_for_page(driver)
    human_delay(1, 2)

    all_emails, source = [], ""
    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        page_emails = extract_emails(soup.get_text(), html)
        if page_emails:
            all_emails.extend(page_emails)
            source = url
    except Exception:
        pass

    links = get_links_safe(driver)
    visited = 0
    for href, txt in links:
        if visited >= 2:
            break
        if not href or "http" not in href:
            continue
        if any(k in (href.lower() + txt) for k in CONTACT_PAGE_KEYWORDS):
            if safe_get(driver, href):
                wait_for_page(driver)
                try:
                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    sub = extract_emails(soup.get_text(), html)
                    if sub:
                        all_emails.extend(sub)
                        if not source:
                            source = href
                    visited += 1
                except Exception:
                    pass
    return list(set(all_emails)), source


def pick_best_email(email_source_pairs: list) -> tuple:
    best_email, best_source, best_score = "", "", -999
    for email, domain, src in email_source_pairs:
        s = score_email(email, domain)
        if s > best_score:
            best_score, best_email, best_source = s, email, src
    return best_email, best_source, best_score


def build_driver():
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--lang=en-US")
    return uc.Chrome(options=options, use_subprocess=True, version_main=147)


def main():
    driver = build_driver()
    results = []
    processed_urls = set()
    processed_creators = set()

    log.info("Starting direct Gumroad scraping pipeline...")
    product_urls = scrape_gumroad_discover(driver, max_products=50)
    log.info(f"Discovered {len(product_urls)} product URLs from Gumroad Discover")

    for product_url in product_urls:
        if product_url in processed_urls:
            continue
        processed_urls.add(product_url)

        if not validate_seller(driver, product_url):
            continue

        creator = extract_creator_info(driver, product_url)
        creator_name = (creator.get("name") or "").strip()
        store_url = (creator.get("store_url") or "").strip()
        external_links = creator.get("external_links", [])

        if not creator_name:
            log.info(f"Skipping creator-empty product: {product_url}")
            continue
        creator_key = creator_name.lower()
        if creator_key in processed_creators:
            continue
        processed_creators.add(creator_key)

        website = ""
        all_email_candidates = []

        if store_url:
            emails, src = scrape_contact(driver, store_url)
            domain = urlparse(store_url).netloc
            for e in emails:
                all_email_candidates.append((e, domain, f"Store:{src}"))

            if not website:
                for ext in external_links:
                    if any(b in urlparse(ext).netloc.lower() for b in BAD_DOMAINS):
                        continue
                    website = ext
                    break

        if website:
            emails, src = scrape_contact(driver, website)
            domain = urlparse(website).netloc
            for e in emails:
                all_email_candidates.append((e, domain, f"Website:{src}"))

        # Optional fallback: one Google search only if no website found
        if not website:
            profiles = find_external_profiles(driver, creator_name)
            website = profiles.get("website", "")
            if website:
                emails, src = scrape_contact(driver, website)
                domain = urlparse(website).netloc
                for e in emails:
                    all_email_candidates.append((e, domain, f"Website:{src}"))

        if not all_email_candidates:
            emails, src = scrape_contact(driver, product_url)
            domain = urlparse(product_url).netloc
            for e in emails:
                all_email_candidates.append((e, domain, f"ProductPage:{src}"))

        best_email, best_source, confidence = pick_best_email(all_email_candidates)
        source_label = "Store Page" if "Store:" in best_source else "Website" if "Website:" in best_source else "Product Page"

        results.append({
            "Creator Name": creator_name,
            "Product URL": product_url,
            "Store URL": store_url,
            "Website": website,
            "Email": best_email,
            "Source": source_label if best_email else "Not Found",
            "Confidence Score": confidence if best_email else 0,
        })

    driver.quit()

    out_df = pd.DataFrame(results)
    columns = ["Creator Name", "Product URL", "Store URL", "Website", "Email", "Source", "Confidence Score"]
    for c in columns:
        if c not in out_df.columns:
            out_df[c] = ""
    out_df = out_df[columns]
    out_df.to_csv("sellers_output.csv", index=False, encoding="utf-8")

    log.info(f"DONE -> sellers_output.csv | Records: {len(results)} | Emails found: {out_df['Email'].astype(bool).sum() if len(out_df) else 0}")


if __name__ == "__main__":
    main()
