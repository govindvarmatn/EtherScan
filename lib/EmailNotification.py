import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import datetime
from lib.ConfigReader import ConfigReader
from dateutil import tz
import random
from time import sleep

logger = logging.getLogger('lib.ExecutionLogger')


def convert_time_zones(date_obj):
    from_zone = tz.tzutc()
    to_zone = tz.gettz('Asia/Kolkata')
    date_in = date_obj.replace(tzinfo=from_zone)
    date_out = date_in.astimezone(to_zone)
    return date_out


def style_transactions(v):
    green = '#A1F78A'
    amber = '#ECD17D'
    template = '<div style="background-color: {color};text-align: center;"><b>{val}</b></div>'
    if 'IN' in v.strip().upper():
        return template.replace('{color}', green).replace('{val}', str(v))
    else:
        return template.replace('{color}', amber).replace('{val}', str(v))


class EmailNotification(object):
    def __init__(self, app_name):
        self.app_name = app_name
        ConfigReader.init_conf(app_name)

        self.to = ConfigReader.get_config('EmailSection', 'email.send.to')
        self.cc = ConfigReader.get_config('EmailSection', 'email.send.cc')

        self.address_root = ConfigReader.get_config('EmailSection', 'email.address.root')
        self.transaction_root = ConfigReader.get_config('EmailSection', 'email.transaction.root')
        self.erc20_ext = ConfigReader.get_config('EmailSection', 'email.erc20.ext')
        self.erc721_ext = ConfigReader.get_config('EmailSection', 'email.erc721.ext')

        self.email_theme = ConfigReader.get_config('EmailSection', 'email.table.theme')
        self.email_image = ConfigReader.get_config('EmailSection', 'email.image.link')

        self.sender_name = ConfigReader.get_config('EmailSection', 'email.sender.name')
        self.from_ = [i.strip() for i in ConfigReader.get_config('EmailSection', 'email.from.id').split(',')]
        self.from_password = [i.strip() for i in ConfigReader.get_config('EmailSection', 'email.from.password').split(',')]
 


    def convert_df_to_html(self, df):
        df2 = df.copy()
        df2['hash'] = df2['hash'].apply(lambda x: f'<a href="{self.transaction_root}{x}">{x}</a>')
        df2.drop(['hash_unique'], axis=1, inplace=True)
        df2 = df2.rename({'transaction': 'type'}, axis=1)
        df2['type'] = df2['type'].apply(lambda x: style_transactions(x))
        html_val = df2.style \
            .set_table_attributes('class ="style-table"') \
            .hide_index() \
            .render()
        return html_val

    def get_html_content(self, df, address, name, token_code, template):
        if token_code == 'erc20':
            ext = self.erc20_ext
        else:
            ext = self.erc721_ext
        address = f'<a href="{self.address_root}{address}#{ext}">{address}</a>'
        new_html_cont = template.replace('{{token}}', token_code) \
                                .replace('{{address}}', address) \
                                .replace('{{name}}', name) \
                                .replace('{{table}}', self.convert_df_to_html(df)) \
                                .replace('{{color}}', self.email_theme) \
                                .replace('{{image_link}}', self.email_image)

        with open('out.html', 'w') as test_f:
            test_f.write(new_html_cont)

        return new_html_cont

    @staticmethod
    def close_smtplib_con(conn):
        try:
            conn.quit()
        except:
            pass

    def send_email(self, df, address, name, token_code):
        logger.info('sending notification email')
        time_email = convert_time_zones(datetime.datetime.now()).strftime("%Y/%m/%d %I:%M:%S %p")
        subject = f'{self.app_name} Transactions by {name} for {token_code} on {time_email}'
        with open('./template/email_template.html', 'r') as fr:
            template_content = fr.read()
        html_content = self.get_html_content(df, address, name, token_code, template_content)
        outer = MIMEMultipart('alternate')
        outer['subject'] = subject
        to_list = [i.strip() for i in self.to.split(',')]
        outer['to'] = ','.join(to_list)
        if self.cc.strip() != '':
            cc_list = [i.strip() for i in self.cc.split(',')]
            outer['cc'] = ','.join(cc_list)
        else:
            cc_list = []
        email_list = to_list + cc_list
        logger.info(f'sending emails to {email_list}')
        error_counter = 0
        while True:
            random_email_index = random.randint(0, len(self.from_ ) - 1)
            from_add = self.from_[random_email_index]
            from_pass = self.from_password[random_email_index]
            try:
                outer['From'] = f'{self.sender_name} <{from_add}>'
                part_2 = MIMEText(html_content, 'html')
                outer.attach(part_2)
                s = smtplib.SMTP('smtp.gmail.com', 587)
                s.ehlo()
                s.starttls()
                s.login(from_add, from_pass)
                logger.info(f'sending emails from {from_add} email address')
                s.sendmail(from_add,
                        email_list,
                        outer.as_string())
                self.close_smtplib_con(s)
                sleep(5)
                break
            except Exception as e:
                logger.exception(f'Email send error! for email id {from_add}')
                logger.warning('Switching emails because of SMTPlib excetptions')
                error_counter += 1
                self.close_smtplib_con(s)
                if error_counter == 2*len(self.from_):
                    self.close_smtplib_con(s)
                    raise smtplib.SMTPException(f'Could not send email to recipients from any of the emails {self.from_}')
                    break
                else:
                    self.close_smtplib_con(s)
                    continue

        
        logger.info('sent email notification !')







