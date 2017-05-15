import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')


def send_email(email_to, etl_name, task_name, load_date, error_message):
    email_from = config["DEFAULT"]["MAIL_DEFAULT_SENDER"]
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "ETL Failure"
    msg['From'] = email_from
    msg['To'] = email_to

    # The body of the message (a plain-text and an HTML version).
    text = "Hi!\nAn ETL error has occurred with the following details.\n ETL Name: '{}'\n Task Name: '{}'\n " \
           "Load Date: '{}'\n Error Message: '{}'\n".format(etl_name, task_name, load_date, str(error_message))
    html = """\
    <html>
      <head></head>
      <body>
        <p>Hi!<br>
           An ETL error has occurred with the following details.<br>
           <b> ETL Name: </b> '{}' <br>
           <b> Task Name: </b> '{}' <br>
           <b> Load Date: </b> '{}' <br>
           <b> Error Message: </b> '{}' <br>
        </p>
      </body>
    </html>
    """.format(etl_name, task_name, load_date, str(error_message))

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.ehlo()
    s.starttls()
    s.login(user=email_from, password=config["DEFAULT"]["MAIL_DEFAULT_PASSWORD"])
    s.sendmail(email_from, email_to, msg.as_string())
    s.quit()
