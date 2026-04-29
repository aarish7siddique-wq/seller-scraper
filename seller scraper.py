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

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("seller_scraper.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

# Platforms we discover sellers from
TARGET_PLATFORMS = {
    "gumroad":   "site:gumroad.com",
    "payhip":    "site:payhip.com",
    "podia":     "site:podia.com",
    "teachable": "site:teachable.com",
    "kajabi":    "site:kajabi.com",
}

# Keywords that indicate a paid product (validate seller)
PAID_SIGNALS = [
    "$", "€", "£", "buy now", "purchase", "add to cart",
    "enroll", "get access", "one-time", "lifetime access",
    "per month", "per year", "checkout"
]

# Keywords that disqualify a product (piracy / free)
FREE_SIGNALS = [
    "free download", "full course free", "pirated",
    "telegram", "torrent", "cracked", "nulled"
]

# Domains we will NOT scrape for emails (noise / social walls)
BAD_DOMAINS = [
    "amazon", "wikipedia", "facebook", "instagram",
    "linkedin", "twitter", "youtube", "goodreads",
    "reddit", "pinterest", "tiktok", "snapchat",
    "google", "bing", "yahoo", "apple", "audible",
    "udemy", "skillshare", "coursera", "edx"
]

# Email prefix priority scoring
EMAIL_PRIORITY_PREFIXES = [
    "info@", "contact@", "hello@", "support@",
    "media@", "press@", "team@", "hi@"
]

# Known junk / role names to skip
JUNK_NAME_KEYWORDS = [
    "visit the", "list of", "how to", "the best",
    "free download", "full course", "shortcut",
    "flashbooks", "zip reads", "getflashnotes"
]

# Contact/about sub-page keywords
CONTACT_PAGE_KEYWORDS = ["contact", "about", "reach", "connect", "work-with"]


# ─────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────

def human_delay(min_s: float = 3, max_s: float = 6):
    time.sleep(random.uniform(min_s, max_s))


def is_junk_name(name: str) -> bool:
    n = name.lower().strip()
    if len(n) < 4:
        return True
    return any(k in n for k in JUNK_NAME_KEYWORDS)


def safe_get(driver, url: str, retries: int = 3, timeout: int = 15) -> bool:
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
            return True  # partial load still usable
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
    urls = []
    seen = set()

    try:
        elements = driver.find_elements(By.CSS_SELECTOR, "div#search a[href]")
        for el in elements:
            try:
                href = clean_google_url(el.get_attribute("href") or "")
                if href and href not in seen and is_good_url(href):
                    urls.append(href)
                    seen.add(href)
            except Exception:
                continue
        log.info(f"  Method 1 found {len(urls)} URLs")
    except Exception as e:
        log.debug(f"Method 1 failed: {e}")

    if not urls:
        try:
            elements = driver.find_elements(By.TAG_NAME, "a")
            for el in elements:
                try:
                    href = clean_google_url(el.get_attribute("href") or "")
                    if href and href not in seen and is_good_url(href):
                        urls.append(href)
                        seen.add(href)
                except Exception:
                    continue
            log.info(f"  Method 2 (fallback) found {len(urls)} URLs")
        except Exception as e:
            log.debug(f"Method 2 failed: {e}")

    return urls[:max_results]


def google_search(driver, query: str, max_results: int = 5) -> list:
    """Run a Google search and return result URLs."""
    if not safe_get(driver, "https://www.google.com"):
        log.warning("Google load failed")
        return []
    human_delay(2, 4)
    try:
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.RETURN)
        human_delay(3, 5)
        wait_for_page(driver)
    except TimeoutException:
        log.warning("Search box not found")
        return []

    current = driver.current_url.lower()
    if "sorry" in current or "captcha" in current:
        log.warning("CAPTCHA detected → solve manually then press ENTER")
        input("Press ENTER after solving CAPTCHA...")

    return extract_google_result_urls(driver, max_results)


# ─────────────────────────────────────────
# EMAIL UTILITIES
# ─────────────────────────────────────────

def clean_email(raw: str) -> str:
    """
    Strict email extraction + cleaning.
    Removes trailing junk words and normalises.
    """
    raw = raw.lower().strip()
    # Strip any trailing words after the email (e.g. "contact")
    match = re.match(r"\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b", raw)
    if match:
        return match.group(1)
    return ""


def is_valid_email(email: str) -> bool:
    email = email.lower().strip()
    if not re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$", email):
        return False
    junk_patterns = [
        "example", "test", "fake", ".png", ".jpg", ".gif",
        "noreply", "no-reply", "donotreply", "sentry", "wpcf7",
        "domain.com", "youremail", "email@email", "admin@admin",
        "user@", "name@", "yourname"
    ]
    if any(x in email for x in junk_patterns):
        return False
    if len(email) > 80:
        return False
    return True


def extract_emails(text: str, html: str) -> list:
    emails = set()
    # Strict regex extraction from visible text
    found = re.findall(
        r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", text
    )
    for e in found:
        cleaned = clean_email(e)
        if cleaned:
            emails.add(cleaned)

    # Mailto links in HTML
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "mailto:" in href.lower():
                raw = href.lower().replace("mailto:", "").split("?")[0].strip()
                cleaned = clean_email(raw)
                if cleaned:
                    emails.add(cleaned)
    except Exception as e:
        log.debug(f"HTML email parse error: {e}")

    return [e for e in emails if is_valid_email(e)]


def score_email(email: str, domain: str) -> int:
    score = 0
    email_lower = email.lower()
    clean_domain = domain.replace("www.", "").split(".")[0]

    if clean_domain and clean_domain in email_lower:
        score += 5
    for prefix in EMAIL_PRIORITY_PREFIXES:
        if email_lower.startswith(prefix):
            score += 3
            break
    if any(x in email_lower for x in ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]):
        score -= 2
    local = email_lower.split("@")[0]
    if len(local) < 3:
        score -= 3
    return score


# ─────────────────────────────────────────
# STEP 1 — SELLER DISCOVERY
# ─────────────────────────────────────────

def discover_sellers(driver, platform_key: str, niche: str = "course", max_results: int = 5) -> list:
    """
    Search Google for sellers on a given platform.
    Returns list of dicts: {product_url, platform}
    """
    site_op = TARGET_PLATFORMS.get(platform_key, f"site:{platform_key}.com")
    query = f'{site_op} "{niche}" buy enroll price'
    log.info(f"[DISCOVERY] Platform={platform_key} | Query: {query}")

    urls = google_search(driver, query, max_results=max_results)
    sellers = []
    for url in urls:
        sellers.append({
            "product_url": url,
            "platform": platform_key,
        })
    return sellers


# ─────────────────────────────────────────
# STEP 2 — SELLER VALIDATION
# ─────────────────────────────────────────

def validate_seller(driver, product_url: str) -> bool:
    """
    Visit the product page and verify it is a paid offering by a real creator.
    Returns True if valid seller, False to skip.
    """
    if not safe_get(driver, product_url):
        return False
    human_delay(2, 4)
    wait_for_page(driver)

    try:
        html = driver.page_source
        text = BeautifulSoup(html, "html.parser").get_text().lower()
    except Exception:
        return False

    # Reject piracy / free content
    if any(sig in text for sig in FREE_SIGNALS):
        log.info(f"  [REJECT] Free/piracy signals found: {product_url}")
        return False

    # Must have at least one paid signal
    if not any(sig in text for sig in PAID_SIGNALS):
        log.info(f"  [REJECT] No paid signals found: {product_url}")
        return False

    log.info(f"  [VALID] Seller confirmed: {product_url}")
    return True


# ─────────────────────────────────────────
# STEP 3 — EXTRACT CREATOR IDENTITY
# ─────────────────────────────────────────

def extract_creator_info(driver, product_url: str) -> dict:
    """
    Extract creator name, store link, and any external links from the product page.
    Returns dict: {name, store_url, external_links}
    """
    info = {"name": "", "store_url": "", "external_links": []}

    if not safe_get(driver, product_url):
        return info

    human_delay(1, 3)
    wait_for_page(driver)

    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()

        # ── Creator name heuristics ──────────────────────────────────────
        # 1. Gumroad: seller name usually in <a class="js-creator-url"> or
        #    a heading near "by" keyword
        name = ""

        # Try common platform patterns
        for sel in [
            "[class*='creator']", "[class*='author']", "[class*='seller']",
            "[class*='profile']", "[data-testid='creator-name']"
        ]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                candidate = el.get_text(strip=True)
                if not is_junk_name(candidate):
                    name = candidate
                    break

        # Fallback: look for "by <Name>" pattern in text
        if not name:
            by_match = re.search(r"\bby\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})", text)
            if by_match:
                candidate = by_match.group(1).strip()
                if not is_junk_name(candidate):
                    name = candidate

        info["name"] = name

        # ── Store URL ────────────────────────────────────────────────────
        parsed = urlparse(product_url)
        # e.g. gumroad.com/username  → strip the product slug
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            store_url = f"{parsed.scheme}://{parsed.netloc}/{path_parts[0]}"
            info["store_url"] = store_url

        # ── External links ───────────────────────────────────────────────
        external = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href.startswith("http"):
                href = urljoin(product_url, href)
            link_domain = urlparse(href).netloc.lower()
            # Keep only links pointing away from the current platform
            if link_domain and link_domain != parsed.netloc and "google" not in link_domain:
                external.append(href)
        info["external_links"] = list(set(external))[:10]

    except Exception as e:
        log.warning(f"Creator info extraction error {product_url}: {e}")

    return info


# ─────────────────────────────────────────
# STEP 4 — EXTERNAL PROFILE DISCOVERY
# ─────────────────────────────────────────

def find_external_profiles(driver, creator_name: str) -> dict:
    """
    Search Google for the creator's external profiles.
    Returns dict: {website, instagram, linkedin, youtube}
    """
    profiles = {"website": "", "instagram": "", "linkedin": "", "youtube": ""}

    if not creator_name or is_junk_name(creator_name):
        return profiles

    # Search 1: official website
    website_urls = google_search(
        driver,
        query=f'"{creator_name}" official website',
        max_results=5
    )
    for url in website_urls:
        domain = urlparse(url).netloc.lower()
        # Skip known platforms and social
        if not any(bad in domain for bad in BAD_DOMAINS + list(TARGET_PLATFORMS.keys())):
            profiles["website"] = url
            break

    # Search 2: social profiles
    social_urls = google_search(
        driver,
        query=f'"{creator_name}" instagram OR linkedin OR youtube',
        max_results=5
    )
    for url in social_urls:
        if "instagram.com" in url and not profiles["instagram"]:
            profiles["instagram"] = url
        elif "linkedin.com" in url and not profiles["linkedin"]:
            profiles["linkedin"] = url
        elif "youtube.com" in url and not profiles["youtube"]:
            profiles["youtube"] = url

    log.info(f"  Profiles found for '{creator_name}': {profiles}")
    return profiles


# ─────────────────────────────────────────
# STEP 5 — CONTACT SCRAPING
# ─────────────────────────────────────────

def scrape_contact(driver, url: str) -> tuple:
    """
    Scrape a website URL for emails. Also checks /contact and /about sub-pages.
    Returns (list_of_emails, source_url_where_found)
    """
    if not url or not safe_get(driver, url):
        return [], ""

    human_delay(2, 4)
    wait_for_page(driver)

    all_emails = []
    source = ""

    # ── Homepage ─────────────────────────────────────────────────────────
    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        page_emails = extract_emails(soup.get_text(), html)
        if page_emails:
            all_emails.extend(page_emails)
            source = url

        footer = soup.find("footer")
        if footer:
            all_emails.extend(extract_emails(footer.get_text(), str(footer)))
    except Exception as e:
        log.warning(f"Homepage parse error {url}: {e}")

    # ── Contact / About sub-pages ─────────────────────────────────────────
    links = get_links_safe(driver)
    visited = 0

    for href, txt in links:
        if visited >= 2:
            break
        if not href or "http" not in href:
            continue
        combined = href.lower() + txt
        if any(k in combined for k in CONTACT_PAGE_KEYWORDS):
            if safe_get(driver, href):
                human_delay(2, 4)
                wait_for_page(driver)
                try:
                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    sub_emails = extract_emails(soup.get_text(), html)
                    if sub_emails:
                        all_emails.extend(sub_emails)
                        if not source:
                            source = href
                    visited += 1
                    log.info(f"    Sub-page scraped: {href}")
                except Exception as e:
                    log.warning(f"Sub-page error {href}: {e}")

    return list(set(all_emails)), source


# ─────────────────────────────────────────
# STEP 6+7 — PICK BEST EMAIL
# ─────────────────────────────────────────

def pick_best_email(email_source_pairs: list) -> tuple:
    """
    Given list of (email, domain, source_url), return (best_email, best_source, score).
    """
    best_email, best_source, best_score = "", "", -999
    for email, domain, src in email_source_pairs:
        s = score_email(email, domain)
        if s > best_score:
            best_score = s
            best_email = email
            best_source = src
    return best_email, best_source, best_score


# ─────────────────────────────────────────
# DRIVER SETUP
# ─────────────────────────────────────────

def build_driver():
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--lang=en-US")
    # ⚠ Update version_main to match your installed Chrome (check chrome://version)
    driver = uc.Chrome(options=options, use_subprocess=True, version_main=147)
    return driver


# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def main():
    driver = build_driver()
    results = []
    processed_names = set()   # cache to avoid duplicate work
    processed_urls = set()    # cache product URLs already seen

    # ── Configuration ────────────────────────────────────────────────────
    NICHES = ["course", "digital product", "ebook", "template"]
    PLATFORMS = list(TARGET_PLATFORMS.keys())   # all 5 platforms
    MAX_SELLERS_PER_QUERY = 5                   # Google results per query
    MAX_SEARCHES_PER_SELLER = 3                 # profile search budget

    log.info("=" * 60)
    log.info("SELLER DISCOVERY + CONTACT EXTRACTION ENGINE")
    log.info("=" * 60)

    for platform in PLATFORMS:
        for niche in NICHES:
            log.info(f"\n{'─'*50}")
            log.info(f"PLATFORM: {platform.upper()} | NICHE: {niche}")
            log.info(f"{'─'*50}")

            # ── STEP 1: Discover sellers ─────────────────────────────────
            sellers = discover_sellers(driver, platform, niche, MAX_SELLERS_PER_QUERY)
            human_delay(3, 6)

            for seller in sellers:
                product_url = seller["product_url"]

                if product_url in processed_urls:
                    log.info(f"  Skipping already processed URL: {product_url}")
                    continue
                processed_urls.add(product_url)

                log.info(f"\n  Product URL: {product_url}")

                # ── STEP 2: Validate seller ──────────────────────────────
                if not validate_seller(driver, product_url):
                    continue
                human_delay(2, 4)

                # ── STEP 3: Extract creator info ─────────────────────────
                creator = extract_creator_info(driver, product_url)
                creator_name = creator.get("name", "")
                store_url = creator.get("store_url", "")
                external_links = creator.get("external_links", [])

                log.info(f"  Creator: '{creator_name}' | Store: {store_url}")

                if not creator_name or creator_name in processed_names:
                    log.info(f"  Skipping — no name or already processed.")
                    if not creator_name:
                        results.append({
                            "Creator Name": "(unknown)",
                            "Product URL": product_url,
                            "Platform": platform,
                            "Website": "",
                            "Email": "",
                            "Source": "Creator name not found",
                            "Confidence Score": 0,
                        })
                    continue
                processed_names.add(creator_name)
                human_delay(2, 4)

                # ── STEP 4: Find external profiles ───────────────────────
                search_count = 0
                profiles = {}
                if search_count < MAX_SEARCHES_PER_SELLER:
                    profiles = find_external_profiles(driver, creator_name)
                    search_count += 2   # website + social = 2 searches

                website = profiles.get("website", "")
                instagram = profiles.get("instagram", "")
                youtube = profiles.get("youtube", "")
                human_delay(2, 5)

                # ── STEP 5: Scrape contact from all sources ───────────────
                all_email_candidates = []

                # Priority 1: Official website
                if website and search_count < MAX_SEARCHES_PER_SELLER + 1:
                    emails, src = scrape_contact(driver, website)
                    domain = urlparse(website).netloc
                    for e in emails:
                        all_email_candidates.append((e, domain, f"Website:{src}"))
                    search_count += 1
                    human_delay(2, 4)

                # Priority 2: Store/profile page on platform
                if store_url and not all_email_candidates:
                    emails, src = scrape_contact(driver, store_url)
                    domain = urlparse(store_url).netloc
                    for e in emails:
                        all_email_candidates.append((e, domain, f"Store:{src}"))
                    human_delay(2, 4)

                # Priority 3: Product page itself
                if not all_email_candidates:
                    emails, src = scrape_contact(driver, product_url)
                    domain = urlparse(product_url).netloc
                    for e in emails:
                        all_email_candidates.append((e, domain, f"ProductPage:{src}"))
                    human_delay(2, 4)

                # Priority 4: External links found on product page
                if not all_email_candidates:
                    for ext_url in external_links[:3]:
                        if any(bad in urlparse(ext_url).netloc for bad in BAD_DOMAINS):
                            continue
                        emails, src = scrape_contact(driver, ext_url)
                        domain = urlparse(ext_url).netloc
                        for e in emails:
                            all_email_candidates.append((e, domain, f"ExternalLink:{src}"))
                        human_delay(2, 3)
                        if all_email_candidates:
                            break

                # ── STEP 6+7: Score and select best email ────────────────
                best_email, best_source, confidence = pick_best_email(all_email_candidates)

                # Determine source label
                if "Website:" in best_source:
                    source_label = "Website"
                elif "Store:" in best_source:
                    source_label = "Store Page"
                elif "ProductPage:" in best_source:
                    source_label = "Product Page"
                elif "ExternalLink:" in best_source:
                    source_label = "External Link"
                else:
                    source_label = "Not Found"

                log.info(f"  → Best Email: {best_email or 'NOT FOUND'} "
                         f"(score={confidence}, src={source_label})")

                # ── STEP 8: Append result ─────────────────────────────────
                results.append({
                    "Creator Name":      creator_name,
                    "Product URL":       product_url,
                    "Platform":          platform,
                    "Website":           website or store_url,
                    "Instagram":         instagram,
                    "YouTube":           youtube,
                    "Email":             best_email,
                    "Source":            source_label,
                    "Confidence Score":  confidence if best_email else 0,
                })

    # ── Save output ──────────────────────────────────────────────────────
    driver.quit()

    out_df = pd.DataFrame(results)

    # ── STEP 8 — Clean output columns ────────────────────────────────────
    column_order = [
        "Creator Name", "Product URL", "Platform",
        "Website", "Instagram", "YouTube",
        "Email", "Source", "Confidence Score"
    ]
    for col in column_order:
        if col not in out_df.columns:
            out_df[col] = ""
    out_df = out_df[column_order]

    out_df.to_csv("sellers_output.csv", index=False, encoding="utf-8")

    found = out_df["Email"].astype(bool).sum()
    log.info(f"\n{'='*60}")
    log.info(f"DONE → sellers_output.csv")
    log.info(f"Total records : {len(results)}")
    log.info(f"Emails found  : {found}")
    log.info(f"{'='*60}")


# ─────────────────────────────────────────
if __name__ == "__main__":
    main()