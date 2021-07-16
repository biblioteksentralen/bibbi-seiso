import logging
import os
from textwrap import dedent
from typing import List, Union, Optional
import mdmail
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class SmtpSettings:
    host: str
    port: int
    user: str
    password: str
    tls: bool = True


@dataclass
class MailTemplate:
    recipients: Union[List[str], str]
    sender_name: str
    sender_email: str
    subject: str
    body: str


def vocabulary_changes_notification():
    return MailTemplate(
        recipients=os.getenv('MAIL_RECIPIENT'),
        sender_name=os.getenv('MAIL_SENDER_NAME'),
        sender_email=os.getenv('MAIL_SENDER_EMAIL'),
        subject="Vokabular oppdatert: %(vocabulary_name)s",
        body=dedent("""
        Hei!
        
        Det eksterne vokabularet «%(vocabulary_name)s» har blitt oppdatert i Promus med følgende endringer:
        
        ## Nye fagkoder
        
        %(new_concepts)s
        
        ## Endrede fagkoder
        
        %(changed_concepts)s
        
        Hilsen vokabularovervåkningstjenesten
        """)
    )


def send_email(
    mail: MailTemplate,
    params: dict,
    settings: Optional[SmtpSettings] = None
):
    settings = settings or SmtpSettings(
        host=os.getenv('MAIL_SMTP_HOST'),
        port=int(os.getenv('MAIL_SMTP_PORT', 567)),
        user=os.getenv('MAIL_SMTP_USER'),
        password=os.getenv('MAIL_SMTP_PASSWORD')
    )
    mdmail.send(
        mail.body % params,
        subject=mail.subject % params,
        from_email="%s <%s>" % (mail.sender_name, mail.sender_email),
        to_email=mail.recipients,
        smtp=settings.__dict__
    )
