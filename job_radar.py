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
import feedparser
from dateutil import parser as dateparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# =========================
# CONFIG
# =========================

LOOKBACK_HOURS = 6           
MAX_EMAIL_ITEMS = 120
# Per-company cap (prevents Amazon/Google flooding)
MAX_PER_COMPANY_DEFAULT = 6

COMPANY_CAPS = {
    "Amazon": 4,
    "Google": 5,
    "Microsoft": 3,
    "Meta": 4,
    "Apple": 3,
    "Nvidia": 2,
}

REQUEST_TIMEOUT = 15
USER_AGENT = "job-radar/2.0 (personal use)"

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

STATE_PATH = "state/seen.json"   # persisted via GitHub Actions cache


# ======================
# LOCATION FILTERING
# ======================

US_KEYWORDS = [
    "united states", "united states of america", "u.s.", "u.s.a", "usa",
    "remote - us", "remote (us)", "remote, us", "us remote", "us-only", "us only"
]

CALIFORNIA_KEYWORDS = [
    "california", "ca", "san francisco", "sf", "bay area",
    "los angeles", "la", "santa clara", "palo alto",
    "mountain view", "sunnyvale", "san jose", "redwood city",
    "san mateo", "menlo park", "cupertino", "irvine", "san diego"
]

US_STATE_ABBR = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la","me","md",
    "ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc",
    "sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc"
}

# ======================
# FULL COMPANY LIST (your list)
# ======================

TARGET_COMPANY_NAMES = [
    "Adobe","Affirm","Airbnb","Amazon","Anthropic","Apple","Atlassian","Brex","Chime","Cisco","Cloudflare",
    "Coinbase","Databricks","Datadog","Discord","DocuSign","DoorDash","Duolingo","Google","Elastic","Figma",
    "Janestreet","Jane Street","LinkedIn","Lyft","Mathworks","MathWorks","Meta","Microsoft","Netflix",
    "Next Insurance","Lemonade","Nextdoor","Notion","Nvidia","Okta","Oracle","OpenAI","Palantir","Pinterest",
    "Plaid","Ramp","Robinhood","Roblox","Scale AI","Scopely","Slack","Splunk","Snowflake","SoFi","Spotify",
    "Square","Stripe","Tesla","Twilio","Uber","Unity","Wayve","Wealthfront","Betterment","Workday","Zendesk"
]

# ======================
# KEYWORD ENGINE (big version)
# ======================

KW_HIGH = [
    # Finance core
    "strategic finance","finance strategy","product finance","business finance","finance business partner",
    "corporate finance","fp&a","planning and analysis","planning & analysis","forecasting","budgeting","variance",
    "kpi","operational finance","commercial finance","gtm finance","go-to-market finance","revenue finance",
    "pricing finance","growth finance","monetization finance","gross margin","unit economics","cac","ltv",
    "cohort","retention","contribution margin","investment bank",

    # Accounting / controllership-ish (keep broad)
    "accounting","technical accounting","controllership","controller","close","month-end","sox","audit",

    # Deals / M&A
    "corporate development","corp dev","m&a","mergers","acquisitions","transaction","transactions",
    "due diligence","valuation","lbo","dcf","investment analysis","deal analysis","investment bank","investment banking",
    "private equity","venture capital","strategic investments","integration","post-merger","pmi","deal",

    # BizOps / Strategy overlap
    "bizops","business operations","strategy & operations","strategy and operations","operating model",
    "business insights","performance analytics","planning analyst","business analytics","analytics strategy",
    "strategic planning","s&o","s&op","sales ops","revenue ops","revops","gtm strategy"
]

KW_MED = [
    "financial analyst","finance analyst","finance manager","finance associate","strategy analyst","strategy manager",
    "business analyst","operations analyst","revenue analyst","pricing analyst","growth analyst",
    "monetization","pricing","go-to-market","gtm","analytics","sql","etl","dashboard","reporting","modeling",
    "budget","forecast","variance analysis","data analysis","data modeling","metrics","kpi reporting",
    "commercial","market sizing","biz strategy","product strategy","product ops","program finance",
    "workload finance","cloud finance","infra finance","data center finance"
]

NEGATIVE = [
    "warehouse","area manager","night shift","hourly","driver","fulfillment","store associate","retail",
    "manufacturing","plant","security guard","nurse","cna","pharmacist","call center","armed"
]


# =========================
# ATS SOURCES (starter set)
# =========================

GREENHOUSE_SLUGS = {
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
}

LEVER_SLUGS = {
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
}

SMARTRECRUITERS_COMPANY_KEYS = {
    "Atlassian": "Atlassian",
    "Splunk": "Splunk",
}


# =========================
# DATA MODEL
# =========================

@dataclass
class Job:
    source: str
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

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def safe_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return dateparser.parse(dt_str)
    except Exception:
        return None

def http_get_json(url: str) -> Any:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def linkedin_referral_search_link(company: str, title: str) -> str:
    q = f'{company} "{title}" finance strategy fp&a'
    return "https://www.linkedin.com/search/results/people/?keywords=" + urllib.parse.quote(q)

def detect_company(text: str) -> str:
    t = norm(text)
    for c in TARGET_COMPANY_NAMES:
        if norm(c) in t:
            return c
    return "Unknown"

def extract_location(text: str) -> str:
    t = norm(text)

    # quick hits
    if "remote" in t:
        # keep any state mentions
        for st in US_STATE_ABBR:
            if re.search(rf"(\b|,|\()({st})(\b|\)|\s)", t):
                return f"Remote, {st.upper()}"
        if "california" in t or " ca" in t:
            return "Remote, CA"
        if "united states" in t or "usa" in t or "us " in t:
            return "Remote, US"
        return "Remote"

    # CA cities
    for k in CALIFORNIA_KEYWORDS:
        if k in t:
            return "CA"

    # any US state abbr
    for st in US_STATE_ABBR:
        if re.search(rf"(\b|,|\()({st})(\b|\)|\s)", t):
            return st.upper()

    # explicit US
    if any(k in t for k in US_KEYWORDS):
        return "US"

    return ""

def location_priority(location: str) -> int:
    loc = norm(location)

    if not loc:
        return 8  # treat missing as likely US-remote-ish

    is_ca = any(k in loc for k in CALIFORNIA_KEYWORDS)

    is_us_keyword = any(k in loc for k in US_KEYWORDS)
    state_hit = any(re.search(rf"(\b|,|\()({st})(\b|\)|\s)", loc) for st in US_STATE_ABBR)

    is_us = is_us_keyword or state_hit or is_ca

    # allow generic remote jobs even if US isn't explicitly mentioned
    if not is_us and "remote" not in loc:
        return -999

    if is_ca:
        return 20

    if "remote" in loc:
        return 12

    return 5

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

    # company boost
    for name in TARGET_COMPANY_NAMES:
        if norm(name) in c:
            score += 8
            break

    # keyword matching
    for kw in KW_HIGH:
        if kw in t or kw in d:
            score += 10
    for kw in KW_MED:
        if kw in t or kw in d:
            score += 5

    # nudges
    if "manager" in t: score += 2
    if "senior" in t or t.startswith("sr"): score += 1
    if "intern" in t: score -= 2

    return score

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
    if job.posted_at is None:
        return True
    if job.posted_at.tzinfo is None:
        dt = job.posted_at.replace(tzinfo=timezone.utc)
    else:
        dt = job.posted_at.astimezone(timezone.utc)
    return dt >= cutoff


# =========================
# GREENHOUSE
# =========================

def fetch_greenhouse_jobs() -> List[Job]:
    jobs: List[Job] = []
    for company, slug in GREENHOUSE_SLUGS.items():
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        try:
            data = http_get_json(url)
            for j in (data.get("jobs", [])[:80]):
                title = (j.get("title") or "").strip()
                location = ((j.get("location") or {}).get("name") or "").strip()
                apply_url = (j.get("absolute_url") or "").strip()
                posted_at = safe_dt(j.get("updated_at")) or safe_dt(j.get("created_at"))

                content = (j.get("content") or "")
                snippet = re.sub("<[^<]+?>", " ", content)
                snippet = re.sub(r"\s+", " ", snippet).strip()[:1000]

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
            time.sleep(0.3)
        except Exception:
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
            for j in (data[:120]):
                title = (j.get("text") or "").strip()
                apply_url = (j.get("hostedUrl") or j.get("applyUrl") or "").strip()

                location = (j.get("categories") or {}).get("location") or (j.get("location") or "")
                if isinstance(location, dict):
                    location = location.get("name", "") or ""
                location = str(location or "")

                created_at_ms = j.get("createdAt")
                posted_at = None
                if isinstance(created_at_ms, (int, float)):
                    posted_at = datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc)

                desc_html = j.get("description") or ""
                snippet = re.sub("<[^<]+?>", " ", desc_html)
                snippet = re.sub(r"\s+", " ", snippet).strip()[:1000]

                if not (title and apply_url):
                    continue

                jobs.append(Job(
                    source="lever",
                    company=company,
                    title=title,
                    location=location,
                    posted_at=posted_at,
                    apply_url=apply_url,
                    description_snippet=snippet
                ))
            time.sleep(0.3)
        except Exception:
            continue
    return jobs


# =========================
# SMARTRECRUITERS
# =========================

def fetch_smartrecruiters_jobs() -> List[Job]:
    jobs: List[Job] = []
    for company, key in SMARTRECRUITERS_COMPANY_KEYS.items():
        url = f"https://api.smartrecruiters.com/v1/companies/{key}/postings"
        try:
            data = http_get_json(url)
            for j in (data.get("content", [])[:120]):
                title = (j.get("name") or "").strip()
                apply_url = (j.get("ref") or j.get("applyUrl") or "").strip()

                city = (j.get("location") or {}).get("city", "") or ""
                country = (j.get("location") or {}).get("country", "") or ""
                loc = ", ".join([x for x in [city, country] if x]).strip()

                posted_at = safe_dt(j.get("releasedDate"))

                snippet = (j.get("jobAd") or {}).get("sections", {}).get("companyDescription", "") or ""
                snippet = re.sub(r"\s+", " ", snippet).strip()[:1000]

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
            time.sleep(0.3)
        except Exception:
            continue
    return jobs


# =========================
# GOOGLE NEWS RSS (Big Tech / Workday / custom ATS coverage)
# =========================

def news_rss_url(q: str) -> str:
    return "https://news.google.com/rss/search?q=" + urllib.parse.quote(q)

def build_news_queries() -> List[str]:
    # Keep it limited to avoid slow runs.
    # Strategy: site filter + role keywords + US/CA constraints
    base_sites = '(site:jobs.lever.co OR site:boards.greenhouse.io OR site:myworkdayjobs.com OR site:careers OR site:jobs)'
    geo = '("United States" OR USA OR "California" OR CA OR "Remote")'

    role_blocks = [
        '("strategic finance" OR "finance analyst" OR "product finance" OR FP&A OR "finance manager")',
        '("corporate development" OR "M&A" OR "due diligence" OR valuation OR "investment banking")',
        '("strategy" OR "strategy & operations" OR bizops OR "business operations" OR "finance & strategy")',
        '("pricing" OR monetization OR "revenue finance" OR "growth finance")'
    ]

    # target companies in batches
    batches = []
    batch_size = 10
    companies = [c for c in TARGET_COMPANY_NAMES if c.lower() not in ("janestreet",)]  # small clean-up
    for i in range(0, len(companies), batch_size):
        b = companies[i:i+batch_size]
        batches.append("(" + " OR ".join([f'"{x}"' for x in b]) + ")")

    queries = []
    for rb in role_blocks:
        for cb in batches[:6]:  # cap batches to keep runtime reasonable
            queries.append(f"{base_sites} {cb} {rb} {geo} job")
    return queries[:18]  # hard cap

def fetch_google_news_rss_jobs() -> List[Job]:
    jobs: List[Job] = []

    for q in build_news_queries():
        url = news_rss_url(q)
        feed = feedparser.parse(url)

        for e in feed.entries[:60]:
            title = (getattr(e, "title", "") or "").strip()
            link = (getattr(e, "link", "") or "").strip()
            summary = (getattr(e, "summary", "") or "").strip()

            if not title or not link:
                continue

            company = detect_company(title + " " + summary)
            loc = extract_location(title + " " + summary)

            # RSS usually doesn't provide a reliable posted datetime
            posted_at = None

            snippet = re.sub(r"\s+", " ", summary)[:1000]

            jobs.append(Job(
                source="google_news_rss",
                company=company,
                title=title,
                location=loc,
                posted_at=posted_at,
                apply_url=link,
                description_snippet=snippet
            ))

        time.sleep(0.2)

    return jobs


# =========================
# EMAIL
# =========================

def send_email(subject: str, html_body: str) -> None:
    sender = os.environ["JOBRADAR_EMAIL_FROM"]
    receiver = os.environ["JOBRADAR_EMAIL_TO"]
    password = os.environ["JOBRADAR_EMAIL_APP_PASSWORD"]

    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, [receiver], msg.as_string())


# =========================
# MAIN
# =========================

def main():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)

    seen = load_seen()

    all_jobs: List[Job] = []
    # ATS first (high quality)
    all_jobs += fetch_greenhouse_jobs()
    all_jobs += fetch_lever_jobs()
    all_jobs += fetch_smartrecruiters_jobs()
    # Big Tech / Workday / custom ATS补全
    all_jobs += fetch_google_news_rss_jobs()

    items: List[Tuple[int, Job]] = []
    for job in all_jobs:
        uid = job.uid()
        if uid in seen:
            continue

        if not is_recent(job, cutoff):
            continue

        sc = score_job(job)
        if sc < 0:
            continue

        items.append((sc, job))

    items.sort(key=lambda x: (-x[0], x[1].company.lower(), x[1].title.lower()))
    
    # company quota filter
    picked: List[Tuple[int, Job]] = []
    company_counts: Dict[str, int] = {}
    
    for sc, job in items:
        cap = COMPANY_CAPS.get(job.company, MAX_PER_COMPANY_DEFAULT)
        if company_counts.get(job.company, 0) >= cap:
            continue
        picked.append((sc, job))
        company_counts[job.company] = company_counts.get(job.company, 0) + 1
        if len(picked) >= MAX_EMAIL_ITEMS:
            break
    
    items = picked


    ts = time.time()
    for _, job in items:
        seen[job.uid()] = ts

    if len(seen) > 25000:
        seen_items = sorted(seen.items(), key=lambda kv: kv[1], reverse=True)[:25000]
        seen = dict(seen_items)

    save_seen(seen)

    subject = f"Job Radar — US-only (CA priority) — {len(items)} matches"
    if not items:
        html = f"""
        <p>Hi Sheila,</p>
        <p>No new matches found in the last {LOOKBACK_HOURS} hours.</p>
        <p>This can happen if postings didn’t change, or if many targets are on non-supported ATS. (We already added a Google RSS layer to cover Big Tech/Workday.)</p>
        <p>Try again later or expand ATS slugs for specific companies.</p>
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
          <b>{job.company}</b> — {job.title}<br/>
          <span>Score: {sc} | Source: {job.source} | Location: {job.location or "N/A"} | Posted: {posted}</span><br/>
          <a href="{job.apply_url}">Apply link</a>
          &nbsp;|&nbsp;
          <a href="{ref_link}">Find referrals on LinkedIn</a>
        </li>
        """)

    html = f"""
    <p>Hi Sheila,</p>
    <p>Here are the newest US-only matches (California prioritized). Click Apply to apply directly.</p>
    <ol>
      {''.join(li)}
    </ol>
    <p>Tip: For ATS sources, adding correct slugs (Greenhouse/Lever) boosts quality a lot. For Big Tech, the RSS layer helps cover Workday/custom ATS.</p>
    """

    send_email(subject, html)


if __name__ == "__main__":
    main()

