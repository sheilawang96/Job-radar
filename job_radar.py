import os
import re
import smtplib
import hashlib
import urllib.parse
from datetime import datetime, timezone
from email.mime.text import MIMEText

import feedparser

# =======================
# Email config (Gmail SMTP)
# =======================
SENDER = os.environ["JOBRADAR_EMAIL_FROM"]
RECEIVER = os.environ["JOBRADAR_EMAIL_TO"]
PASSWORD = os.environ["JOBRADAR_EMAIL_APP_PASSWORD"]

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

MAX_ITEMS_EMAIL = 60  # 
PER_FEED_LIMIT = 40   # 

# =======================
# Your priority company list
# =======================
COMPANIES = [
    "Adobe","Affirm","Airbnb","Amazon","Anthropic","Apple","Atlassian","Brex","Chime","Cisco","Cloudflare",
    "Coinbase","Databricks","Datadog","Discord","DocuSign","DoorDash","Duolingo","Google","Elastic","Figma",
    "Jane Street","LinkedIn","Lyft","MathWorks","Meta","Microsoft","Netflix","Next Insurance","Lemonade",
    "Nextdoor","Notion","Nvidia","Okta","Oracle","OpenAI","Palantir","Pinterest","Plaid","Ramp","Robinhood",
    "Roblox","Scale AI","Scopely","Slack","Splunk","Snowflake","SoFi","Spotify","Square","Stripe","Tesla",
    "Twilio","Uber","Unity","Wayve","Wealthfront","Betterment","Workday","Zendesk"
]

# =======================
# Keyword engine: “apply like a monster”
# =======================
KW_HIGH = [
    # Finance core
    "strategic finance","finance strategy","product finance","business finance","finance business partner",
    "corporate finance","fp&a","planning and analysis","planning & analysis","forecasting","budgeting","variance",
    "kpi","operational finance","commercial finance","gtm finance","revenue finance","pricing finance",
    "growth finance","monetization finance","investment bank","accounting",

    # Consulting / deal overlap
    "corporate development","corp dev","m&a","mergers","acquisitions","transaction","transactions",
    "due diligence","valuation","lbo","dcf","investment analysis","deal analysis",

    # BizOps / strategy overlap (tech finance)
    "bizops","business operations","strategy & operations","strategy and operations","business insights",
    "performance analytics","planning analyst","business analytics"
]

KW_MED = [
    "financial analyst","finance analyst","finance manager","finance associate","strategy analyst",
    "business analyst","operations analyst","revenue analyst","pricing analyst","growth analyst",
    "monetization","pricing","go-to-market","gtm","analytics","sql","etl","dashboard","reporting"
]

NEGATIVE = [
    "warehouse","area manager","night shift","hourly","driver","fulfillment","security","nurse","cna",
    "retail","store associate","manufacturing","plant","pharmacist"
]

# =======================
# Google RSS helpers
# =======================
def google_rss(query: str) -> str:
    q = urllib.parse.quote_plus(query)
    return f"https://www.google.com/search?q={q}&num=50&output=rss"

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def score(title: str, snippet: str) -> int:
    t = norm(title)
    s = norm(snippet)

    for bad in NEGATIVE:
        if bad in t or bad in s:
            return -999

    sc = 0
    for kw in KW_HIGH:
        if kw in t or kw in s:
            sc += 10
    for kw in KW_MED:
        if kw in t or kw in s:
            sc += 5

    # Title nudges
    if "manager" in t: sc += 2
    if "senior" in t or "sr " in t or t.startswith("sr"): sc += 1
    if "intern" in t: sc -= 2  

    return sc

def detect_company(text: str) -> str | None:
    txt = norm(text)
    for c in COMPANIES:
        if norm(c) in txt:
            return c
    return None

def jid(title: str, link: str) -> str:
    return hashlib.sha256((title + "||" + link).encode("utf-8")).hexdigest()[:20]

# =======================
# Build multi-search queries
# =======================
def build_queries():
    # 1) 全网广撒网（高产）
    broad = (
        '('
        '"strategic finance" OR "product finance" OR "finance business partner" OR FP&A OR '
        '"corporate finance" OR "finance analyst" OR "business operations" OR bizops OR '
        '"strategy & operations" OR "revenue analyst" OR monetization OR pricing OR '
        '"corporate development" OR "M&A" OR valuation OR "due diligence"'
        ') jobs'
    )

    # 2) target company（强制出现你关心的公司）
    # 分批是为了让 Google RSS 不至于太长太乱
    company_batches = []
    batch_size = 12
    for i in range(0, len(COMPANIES), batch_size):
        batch = COMPANIES[i:i+batch_size]
        company_part = "(" + " OR ".join([f'"{c}"' for c in batch]) + ")"
        q = f'{company_part} ({broad})'
        company_batches.append(q)

    # 3) ATS 来源加权（Greenhouse/Lever/Workday/Ashby）
    ats = (
        '('
        'site:boards.greenhouse.io OR site:jobs.lever.co OR site:myworkdayjobs.com OR '
        'site:ashbyhq.com OR site:smartrecruiters.com'
        ') '
        + broad
    )

    # 4) 你的大厂 + fintech 特别常见的 finance/strategy 关键词组合
    tech_finance = (
        '('
        '"growth & monetization" OR "growth and monetization" OR "finance & strategy" OR '
        '"finance and strategy" OR "pricing strategy" OR "gtm strategy" OR "revenue strategy" OR '
        '"business finance" OR "strategic planning"'
        ') jobs'
    )

    queries = [broad, ats, tech_finance] + company_batches
    return queries

# =======================
# Fetch & rank
# =======================
def fetch_jobs():
    queries = build_queries()
    seen = set()
    results = []

    for q in queries:
        url = google_rss(q)
        feed = feedparser.parse(url)

        for e in feed.entries[:PER_FEED_LIMIT]:
            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            summary = getattr(e, "summary", "")

            if not title or not link:
                continue

            key = jid(title, link)
            if key in seen:
                continue
            seen.add(key)

            sc = score(title, summary)
            if sc < 0:
                continue

            comp = detect_company(title) or detect_company(summary) or "Unknown"
            results.append((sc, comp, title, link))

    # sort high score first, then company name
    results.sort(key=lambda x: (-x[0], x[1], x[2]))
    return results[:MAX_ITEMS_EMAIL]

def send_email(items):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subject = f"🔥 Job Radar — Top Matches ({len(items)}) — {now}"

    li = []
    for sc, comp, title, link in items:
        li.append(f"<li><b>{comp}</b> — {title} "
                  f"(score {sc}) — <a href='{link}'>Apply</a></li>")

    html = f"""
    <p>Hi Sheila 😈，这是最新抓到的岗位（点击直接投递）：</p>
    <ol>
      {''.join(li)}
    </ol>
    <p>Tip：你想某家公司更密集出现，我可以把它放进“超级优先名单”。</p>
    """

    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = RECEIVER

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER, PASSWORD)
        server.sendmail(SENDER, [RECEIVER], msg.as_string())

def main():
    jobs = fetch_jobs()
    if not jobs:
        # 也发个空邮件，告诉你系统活着（避免你以为坏了）
        send_email([(0, "System", "No matches found this run — consider broadening keywords", "https://www.google.com")])
        return
    send_email(jobs)

if __name__ == "__main__":
    main()
