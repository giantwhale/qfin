from time import sleep
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from .. import settings


def sendmail(sender=None, receipient=None, subject=None, text=None, html=None, retries=3):

    AWS_Config = settings.AWS_Config    

    EMAIL_HOST          = AWS_Config.EMAIL_HOST
    EMAIL_HOST_USER     = AWS_Config.EMAIL_HOST_USER
    EMAIL_HOST_PASSWORD = AWS_Config.EMAIL_HOST_PASSWORD
    EMAIL_PORT          = AWS_Config.EMAIL_PORT

    sender     = sender     or AWS_Config.Sender
    receipient = receipient or AWS_Config.Receipient
    subject    = subject    or '[qfin] No Subject'

    msg            = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'   ] = sender
    msg['To'     ] = receipient

    if text:
        part1 = MIMEText(text, 'plain')
        msg.attach(part1)

    if html:
        part2 = MIMEText(html, 'html')
        msg.attach(part2)

    for idx in range(retries):
        try:
            s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
            s.starttls()
            s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
            s.sendmail(sender, receipient.split(','), msg.as_string())
            s.quit()
            break
        except Exception as err:
            if idx < retries - 1:
                sleep(2)
            else:
                raise 
