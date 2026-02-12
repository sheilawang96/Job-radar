import os
import smtplib
from email.mime.text import MIMEText

# 这是一个测试版本（先确保 GitHub Actions 和邮件能跑通）

sender = os.environ["JOBRADAR_EMAIL_FROM"]
receiver = os.environ["JOBRADAR_EMAIL_TO"]
password = os.environ["JOBRADAR_EMAIL_APP_PASSWORD"]

subject = "🔥 Job Radar Test Successful"
body = """
Hi Sheila 😈,

你的自动 job radar 已经跑起来了！

下一步我们会升级：
✅ 每6小时自动扫描岗位
✅ 自动匹配 finance / strategy / product roles
✅ 自动发 apply link 给你

现在这封邮件说明系统已经正常工作。

"""

msg = MIMEText(body)
msg["Subject"] = subject
msg["From"] = sender
msg["To"] = receiver

server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()
server.login(sender, password)
server.sendmail(sender, receiver, msg.as_string())
server.quit()
