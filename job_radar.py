import os
import requests
import re
import smtplib
import urllib.parse
import feedparser
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

############################################
# EMAIL CONFIG
############################################

SMTP_HOST="smtp.gmail.com"
SMTP_PORT=587

EMAIL_FROM=os.environ["JOBRADAR_EMAIL_FROM"]
EMAIL_TO=os.environ["JOBRADAR_EMAIL_TO"]
EMAIL_PASS=os.environ["JOBRADAR_EMAIL_APP_PASSWORD"]

############################################
# COMPANY FLOOD CONTROL
############################################

MAX_PER_COMPANY_DEFAULT=4

COMPANY_CAPS={
"Amazon":3,
"Google":3,
"Meta":3,
"Microsoft":3,
"Apple":3,
"Nvidia":2
}

############################################
# TARGET COMPANIES
############################################

TARGET_COMPANIES=[
"Adobe","Airbnb","Amazon","Apple","Coinbase","Databricks","Datadog",
"Discord","DoorDash","Duolingo","Google","Meta","Microsoft",
"Nvidia","Notion","OpenAI","Palantir","Plaid","Ramp","Robinhood",
"Snowflake","Stripe","Tesla","Uber","Workday","Zendesk"
]

############################################
# KEYWORDS
############################################

INCLUDE=[
"finance","financial analyst","strategic finance","fp&a",
"corporate development","m&a","valuation",
"investment banking","investment bank",
"strategy","business operations","bizops",
"product strategy","product operations","program manager"
]

EXCLUDE=[
"intern","internship","software engineer","data engineer",
"machine learning","scientist","phd"
]

############################################
# DATA MODEL
############################################

@dataclass
class Job:
    company:str
    title:str
    location:str
    url:str
    source:str

############################################
# FILTER
############################################

def is_us_location(loc):

    if not loc:
        return True

    loc=loc.lower()

    if any(x in loc for x in ["united states","us","usa","remote","california","ca","new york"]):
        return True

    return False


def good(title,location):

    t=title.lower()

    if not is_us_location(location):
        return False

    if any(b in t for b in EXCLUDE):
        return False

    if any(k in t for k in INCLUDE):
        return True

    return False

############################################
# FETCH GREENHOUSE
############################################

GREENHOUSE={
"Airbnb":"airbnb",
"Coinbase":"coinbase",
"Databricks":"databricks",
"Datadog":"datadog",
"Notion":"notion",
"Plaid":"plaid",
"Robinhood":"robinhood",
"Snowflake":"snowflake",
"Stripe":"stripe",
"Zendesk":"zendesk"
}

def greenhouse():

    jobs=[]

    for company,slug in GREENHOUSE.items():

        try:
            url=f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
            data=requests.get(url,timeout=10).json()

            for j in data.get("jobs",[]):

                title=j.get("title","")
                loc=j.get("location",{}).get("name","")
                link=j.get("absolute_url","")

                if good(title,loc):
                    jobs.append(Job(company,title,loc,link,"greenhouse"))

        except:
            pass

    return jobs

############################################
# FETCH LEVER
############################################

LEVER={
"Ramp":"ramp",
"Brex":"brex",
"Chime":"chime",
"Cloudflare":"cloudflare",
"DoorDash":"doordash",
"Duolingo":"duolingo",
"Okta":"okta",
"SoFi":"sofi",
"Uber":"uber",
"Unity":"unity"
}

def lever():

    jobs=[]

    for company,slug in LEVER.items():

        try:
            url=f"https://api.lever.co/v0/postings/{slug}?mode=json"
            data=requests.get(url,timeout=10).json()

            for j in data:

                title=j.get("text","")
                loc=j.get("categories",{}).get("location","")
                link=j.get("hostedUrl","")

                if good(title,loc):
                    jobs.append(Job(company,title,loc,link,"lever"))

        except:
            pass

    return jobs

############################################
# GOOGLE RSS (Big Tech coverage)
############################################

def google_rss():

    jobs=[]

    query='(site:myworkdayjobs.com OR site:careers.google.com OR site:jobs.apple.com) ("finance" OR "strategy" OR "corporate development")'

    url="https://news.google.com/rss/search?q="+urllib.parse.quote(query)

    feed=feedparser.parse(url)

    for e in feed.entries[:80]:

        title=e.title
        link=e.link

        company="Unknown"
        for c in TARGET_COMPANIES:
            if c.lower() in title.lower():
                company=c
                break

        if good(title,"US"):
            jobs.append(Job(company,title,"US",link,"rss"))

    return jobs

############################################
# FLOOD CONTROL
############################################

def balance(jobs):

    counts={}
    final=[]

    for j in jobs:

        cap=COMPANY_CAPS.get(j.company,MAX_PER_COMPANY_DEFAULT)

        if counts.get(j.company,0)>=cap:
            continue

        final.append(j)
        counts[j.company]=counts.get(j.company,0)+1

    return final

############################################
# EMAIL
############################################

def send(jobs):

    html="<h3>Job Radar Results</h3><ul>"

    for j in jobs:

        html+=f"<li><b>{j.company}</b> — {j.title} ({j.location})<br><a href='{j.url}'>Apply</a></li>"

    html+="</ul>"

    msg=MIMEMultipart("alternative")

    msg["From"]=EMAIL_FROM
    msg["To"]=EMAIL_TO
    msg["Subject"]=f"Job Radar — {len(jobs)} matches"

    msg.attach(MIMEText(html,"html"))

    with smtplib.SMTP(SMTP_HOST,SMTP_PORT) as s:

        s.starttls()
        s.login(EMAIL_FROM,EMAIL_PASS)
        s.sendmail(EMAIL_FROM,EMAIL_TO,msg.as_string())

############################################
# MAIN
############################################

def main():

    jobs=[]

    jobs+=greenhouse()
    jobs+=lever()
    jobs+=google_rss()

    unique={}
    for j in jobs:
        key=(j.company,j.title)
        if key not in unique:
            unique[key]=j

    final=list(unique.values())

    final=balance(final)

    send(final)

if __name__=="__main__":
    main()
