import os
import requests
import re
import time
import smtplib
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

MAX_PER_COMPANY_DEFAULT=5

COMPANY_CAPS={
"Amazon":4,
"Google":4,
"Meta":4,
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
# ATS SOURCES
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

def good(title):

    t=title.lower()

    if any(b in t for b in EXCLUDE):
        return False

    if any(k in t for k in INCLUDE):
        return True

    return False

############################################
# FETCH GREENHOUSE
############################################

def greenhouse():

    jobs=[]

    for company,slug in GREENHOUSE.items():

        url=f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"

        try:
            data=requests.get(url,timeout=10).json()

            for j in data.get("jobs",[]):

                title=j.get("title","")
                loc=j.get("location",{}).get("name","")
                link=j.get("absolute_url","")

                if good(title):

                    jobs.append(Job(company,title,loc,link,"greenhouse"))

        except:
            pass

    return jobs

############################################
# FETCH LEVER
############################################

def lever():

    jobs=[]

    for company,slug in LEVER.items():

        url=f"https://api.lever.co/v0/postings/{slug}?mode=json"

        try:
            data=requests.get(url,timeout=10).json()

            for j in data:

                title=j.get("text","")
                loc=j.get("categories",{}).get("location","")
                link=j.get("hostedUrl","")

                if good(title):

                    jobs.append(Job(company,title,loc,link,"lever"))

        except:
            pass

    return jobs

############################################
# GOOGLE RSS (补 Big Tech)
############################################

def google_rss():

    jobs=[]

    import urllib.parse
    
    query='("finance" OR "strategy" OR "investment banking") job site:myworkdayjobs.com'
    
    url="https://news.google.com/rss/search?q="+urllib.parse.quote(query)


    feed=feedparser.parse(url)

    for e in feed.entries[:50]:

        title=e.title
        link=e.link

        if good(title):

            company="Unknown"
            for c in TARGET_COMPANIES:
                if c.lower() in title.lower():
                    company=c
                    break

            jobs.append(Job(company,title,"",""+link,"rss"))

    return jobs

############################################
# FLOOD CONTROL
############################################

def limit_company(jobs):

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

    # 去重
    unique={}
    for j in jobs:
        key=(j.company,j.title)
        if key not in unique:
            unique[key]=j

    final=list(unique.values())

    # 按公司防洪
    final=limit_company(final)

    send(final)

if __name__=="__main__":
    main()
