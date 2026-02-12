import os
import re
import json
import time
import hashlib
import smtplib
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as dateparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# =========================
# CONFIG
# =========================
# ======================
# LOCATION FILTERING
# ======================

US_KEYWORDS = [
    "united states", "united states of america", "u.s.", "u.s.a", "usa",
    "remote - us", "remote (us)", "remote, us", "us remote"
]


CALIFORNIA_KEYWORDS = [
    "california", "ca", "san francisco", "sf", "bay area",
    "los angeles", "la", "santa clara", "palo alto",
    "mountain view", "sunnyvale", "san jose", "redwood city"
]

LOOKBACK_HOURS = 6
MAX_EMAIL_ITEMS = 80
REQUEST_TIMEOUT = 20
USER_AGENT = "job-radar/1.0 (personal use)"

# Gmail SMTP (use App Password)
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

STATE_PATH = "state/seen.json"   # persisted via GitHub Actions cache

# ---- Keyword engine (broad, high-volume) ----
KW_HIGH = [
    # Core finance / FP&A
    "strategic finance","finance strategy","product finance","business finance","finance business partner",
    "corporate finance","fp&a","planning and analysis","planning & analysis","forecasting","budgeting",
    "variance","kpi","operational finance","commercial finance","go-to-market finance","gtm finance",
    "revenue finance","pricing finance","growth finance","monetization finance","accounting",

    # Deals / consulting overlap
    "corporate development","corp dev","m&a","mergers","acquisitions","transaction","due diligence",
    "valuation","investment analysis","deal analysis","lbo","dcf","investment bank","deal"

    # BizOps / strategy overlap (often finance-adjacent in tech)
    "bizops","business operations","strategy & operations","strategy and operations","business insights",
    "performance analytics","planning analyst","business analytics","analytics strategy"
]

KW_MED = [
    "financial analyst","finance analyst","finance manager","finance associate","strategy analyst",
    "business analyst","operations analyst","revenue analyst","pricing analyst","growth analyst",
    "monetization","pricing strategy","go-to-market","gtm","analytics","sql","etl","dashboard","reporting",
    "planning","strategic planning","commercial"
]

NEGATIVE = [
    "warehouse","area manager","night shift","hourly","driver","fulfillment","store associate","retail",
    "manufacturing","plant","security guard","nurse","cna","pharmacist","call center"
]

# ---- Target companies you care about (used for scoring boost + LinkedIn search link text) ----
TARGET_COMPANY_NAMES = [
    "Adobe","Affirm","Airbnb","Amazon","Anthropic","Apple","Atlassian","Brex","Chime","Cisco","Cloudflare",
    "Coinbase","Databricks","Datadog","Discord","DocuSign","DoorDash","Duolingo","Google","Elastic","Figma",
    "Jane Street","LinkedIn","Lyft","MathWorks","Meta","Microsoft","Netflix","Next Insurance","Lemonade",
    "Nextdoor","Notion","Nvidia","Okta","Oracle","OpenAI","Palantir","Pinterest","Plaid","Ramp","Robinhood",
    "Roblox","Scale AI","Scopely","Slack","Splunk","Snowflake","SoFi","Spotify","Square","Stripe","Tesla",
    "Twilio","Uber","Unity","Wayve","Wealthfront","Betterment","Workday","Zendesk"
]

# =========================
# ATS SOURCES
# =========================
# IMPORTANT: These "slugs" are ATS identifiers, not company names.
# If a company doesn't return jobs, it likely uses a different ATS or a different slug.
# You can add/remove entries freely.

GREENHOUSE_SLUGS = {
    # Common GH board slugs (starter set)
    "Airbnb": "airbnb",
    "Coinbase": "coinbase",
    "Databricks": "databricks",
    "Datadog": "datadog",
    "Discord": "discord",
    "Figma": "figma",
    "Notion": "notion",
    "Plaid": "plaid",
    "Robinhood": "robinhood",
    "Scale AI": "scaleai",
    "Snowflake": "snowflake",
    "Stripe": "stripe",
    "Twilio": "twilio",
    "Zendesk": "zendesk",
    # Add more as you confirm slugs
}

LEVER_SLUGS = {
    # Common Lever slugs (starter set)
    "Ramp": "ramp",
    "Brex": "brex",
    "Chime": "chime",
    "Cloudflare": "cloudflare",
    "DoorDash": "doordash",
    "Duolingo": "duolingo",
    "Nextdoor": "nextdoor",
    "Okta": "okta",
    "SoFi": "sofi",
    "Uber": "uber",
    "Unity": "unity",
    "Wealthfront": "wealthfront",
    # Add more as you confirm slugs
}

SMARTRECRUITERS_COMPANY_KEYS = {
    # SmartRecruiters uses a "company key" in the URL, often matches brand name but not always
    "Atlassian": "Atlassian",
    "Splunk": "Splunk",
    # Add more as needed
}

# If you want to aggressively expand beyond your list, add additional slugs here.


# =========================
# DATA MODEL
# =========================
@dataclass
class Job:
    source: str              # greenhouse / lever / smartrecruiters
    company: str
    title: str
    location: str
    posted_at: Optional[datetime]
    apply_url: str
    description_snippet: str

    def uid(self) -> str:
        base = f"{self.source}|{self.company}|{self.title}|{self.apply_url}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]


# =========================
# UTIL
# =========================
US_STATE_ABBR = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la","me","md",
    "ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc",
    "sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc"
}

def location_priority(location: str) -> int:
    loc = (location or "").lower().strip()

    # if location missing → don't hard filter
    if not loc:
        return 8

    is_ca = any(k in loc for k in CALIFORNIA_KEYWORDS)

    # detect US via keyword OR state abbreviation OR CA
    is_us_keyword = any(k in loc for k in US_KEYWORDS)
    state_hit = any(re.search(rf"(\\b|,|\\()({st})(\\b|\\)|\\s)", loc) for st in US_STATE_ABBR)

    is_us = is_us_keyword or state_hit or is_ca

    # allow generic remote jobs
    if not is_us and "remote" not in loc:
        return -999

    if is_ca:
        return 20

    return 5


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def score_job(job: Job) -> int:
    t = norm(job.title)
    d = norm(job.description_snippet)
    c = norm(job.company)

    for bad in NEGATIVE:
        if bad in t or bad in d:
            return -999

    score = 0
    loc_score = location_priority(job.location)
    if loc_score < 0:
        return -999

    score += loc_score


    # Boost target companies
    for name in TARGET_COMPANY_NAMES:
        if norm(name) in c:
            score += 8
            break

    for kw in KW_HIGH:
        if kw in t or kw in d:
            score += 10
    for kw in KW_MED:
        if kw in t or kw in d:
            score += 5

    # Small nudges
    if "manager" in t: score += 2
    if "senior" in t or t.startswith("sr"): score += 1
    if "intern" in t: score -= 2

    return score

def safe_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return dateparser.parse(dt_str)
    except Exception:
        return None

def linkedin_referral_search_link(company: str, title: str) -> str:
    # Safe: just a search URL you click manually
    q = f'{company} finance strategy FP&A "{title}"'
    return "https://www.linkedin.com/search/results/people/?keywords=" + urllib.parse.quote(q)

def load_seen() -> Dict[str, float]:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_seen(seen: Dict[str, float]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)

def is_recent(job: Job, cutoff: datetime) -> bool:
    # If we have posted_at, enforce cutoff. If not, allow (some ATS don’t provide accurate timestamps).
    if job.posted_at is None:
        return True
    # normalize to aware UTC if possible
    if job.posted_at.tzinfo is None:
        dt = job.posted_at.replace(tzinfo=timezone.utc)
    else:
        dt = job.posted_at.astimezone(timezone.utc)
    return dt >= cutoff

def http_get_json(url: str) -> Any:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def http_get_text(url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


# =========================
# GREENHOUSE
# =========================
def fetch_greenhouse_jobs() -> List[Job]:
    jobs: List[Job] = []
    for company, slug in GREENHOUSE_SLUGS.items():
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        try:
            data = http_get_json(url)
            for j in data.get("jobs", []):
                title = j.get("title", "").strip()
                location = (j.get("location") or {}).get("name", "") or ""
                apply_url = j.get("absolute_url", "") or ""
                updated_at = j.get("updated_at")
                created_at = j.get("created_at")
                posted_at = safe_dt(updated_at) or safe_dt(created_at)

                # description content can be long HTML; we keep a snippet
                content = (j.get("content") or "")
                snippet = re.sub("<[^<]+?>", " ", content)  # strip HTML tags
                snippet = re.sub(r"\s+", " ", snippet).strip()[:800]

                if not (title and apply_url):
                    continue

                jobs.append(Job(
                    source="greenhouse",
                    company=company,
                    title=title,
                    location=location,
                    posted_at=posted_at,
                    apply_url=apply_url,
                    description_snippet=snippet
                ))
        except Exception:
            # Skip quietly; you can inspect failures in Actions logs
            continue
    return jobs


# =========================
# LEVER
# =========================
def fetch_lever_jobs() -> List[Job]:
    jobs: List[Job] = []
    for company, slug in LEVER_SLUGS.items():
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            data = http_get_json(url)
            for j in data:
                title = (j.get("text") or "").strip()
                apply_url = j.get("hostedUrl") or j.get("applyUrl") or ""
                categories = j.get("categories") or {}
                location = (j.get("categories") or {}).get("location") or (j.get("location") or "")
                if isinstance(location, dict):
                    location = location.get("name", "") or ""

                created_at_ms = j.get("createdAt")
                posted_at = None
                if isinstance(created_at_ms, (int, float)):
                    posted_at = datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc)

                # Lever provides description in HTML
                desc_html = j.get("description") or ""
                snippet = re.sub("<[^<]+?>", " ", desc_html)
                snippet = re.sub(r"\s+", " ", snippet).strip()[:800]

                if not (title and apply_url):
                    continue

                jobs.append(Job(
                    source="lever",
                    company=company,
                    title=title,
                    location=str(location or ""),
                    posted_at=posted_at,
                    apply_url=apply_url,
                    description_snippet=snippet
                ))
        except Exception:
            continue
    return jobs


# =========================
# SMARTRECRUITERS
# =========================
def fetch_smartrecruiters_jobs() -> List[Job]:
    jobs: List[Job] = []
    for company, key in SMARTRECRUITERS_COMPANY_KEYS.items():
        # Public API endpoint
        url = f"https://api.smartrecruiters.com/v1/companies/{key}/postings"
        try:
            data = http_get_json(url)
            for j in data.get("content", []):
                title = (j.get("name") or "").strip()
                apply_url = j.get("ref") or j.get("applyUrl") or ""
                location = (j.get("location") or {}).get("city", "") or ""
                country = (j.get("location") or {}).get("country", "") or ""
                loc = ", ".join([x for x in [location, country] if x])

                posted_at = safe_dt(j.get("releasedDate"))

                snippet = (j.get("jobAd") or {}).get("sections", {}).get("companyDescription", "") or ""
                snippet = re.sub(r"\s+", " ", snippet).strip()[:800]

                if not (title and apply_url):
                    continue

                jobs.append(Job(
                    source="smartrecruiters",
                    company=company,
                    title=title,
                    location=loc,
                    posted_at=posted_at,
                    apply_url=apply_url,
                    description_snippet=snippet
                ))
        except Exception:
            continue
    return jobs


# =========================
# EMAIL
# =========================
def send_email(subject: str, html_body: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["From"] = SENDER
    msg["To"] = RECEIVER
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SENDER, PASSWORD)
        server.sendmail(SENDER, [RECEIVER], msg.as_string())


# =========================
# MAIN
# =========================
def main():
    sender = os.environ["JOBRADAR_EMAIL_FROM"]
    receiver = os.environ["JOBRADAR_EMAIL_TO"]
    password = os.environ["JOBRADAR_EMAIL_APP_PASSWORD"]
    globals()["SENDER"] = sender
    globals()["RECEIVER"] = receiver
    globals()["PASSWORD"] = password

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)

    seen = load_seen()

    all_jobs: List[Job] = []
    all_jobs += fetch_greenhouse_jobs()
    all_jobs += fetch_lever_jobs()
    all_jobs += fetch_smartrecruiters_jobs()

    # Filter + dedupe + score
    items: List[Tuple[int, Job]] = []
    for job in all_jobs:
        uid = job.uid()

        # skip already emailed
        if uid in seen:
            continue

        # recency filter (best effort)
        if not is_recent(job, cutoff):
            continue

        sc = score_job(job)
        if sc < 0:
            continue

        items.append((sc, job))

    # Sort by score desc, then company/title
    items.sort(key=lambda x: (-x[0], x[1].company.lower(), x[1].title.lower()))

    # Limit email size
    items = items[:MAX_EMAIL_ITEMS]

    # Update seen state
    ts = time.time()
    for _, job in items:
        seen[job.uid()] = ts

    # prune seen to avoid unbounded growth
    # keep last ~20k entries
    if len(seen) > 20000:
        # drop oldest (by timestamp)
        seen_items = sorted(seen.items(), key=lambda kv: kv[1], reverse=True)[:20000]
        seen = dict(seen_items)

    save_seen(seen)

    # Build email
    subject = f"Job Radar — Top matches (last {LOOKBACK_HOURS}h): {len(items)}"
    if not items:
        html = f"""
        <p>Hi Sheila,</p>
        <p>No new matches found in the last {LOOKBACK_HOURS} hours from the current ATS sources.</p>
        <p>This usually means either (1) those company slugs need adjustment, or (2) postings didn’t change in that window.</p>
        <p>If you want, add more Greenhouse/Lever/SmartRecruiters slugs and this will immediately get louder.</p>
        """
        send_email(subject, html)
        return

    li = []
    for sc, job in items:
        posted = "unknown"
        if job.posted_at:
            dt = job.posted_at.astimezone(timezone.utc) if job.posted_at.tzinfo else job.posted_at.replace(tzinfo=timezone.utc)
            posted = dt.strftime("%Y-%m-%d %H:%M UTC")

        ref_link = linkedin_referral_search_link(job.company, job.title)

        li.append(f"""
        <li>
          <b>{job.company}</b> — {job.title} <br/>
          <span>Score: {sc} | Source: {job.source} | Location: {job.location or "N/A"} | Posted: {posted}</span><br/>
          <a href="{job.apply_url}">Apply link</a>
          &nbsp;|&nbsp;
          <a href="{ref_link}">Find referrals on LinkedIn</a>
        </li>
        """)

    html = f"""
    <p>Hi Sheila,</p>
    <p>Here are the newest matched roles from ATS sources (Greenhouse / Lever / SmartRecruiters) in the last {LOOKBACK_HOURS} hours.</p>
    <ol>
      {''.join(li)}
    </ol>
    <p>Tip: If a company is missing, it likely uses a different ATS or a different slug. Add its slug in the config and it will show up next run.</p>
    """

    send_email(subject, html)


if __name__ == "__main__":
    main()
