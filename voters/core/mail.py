from django.conf import settings
from django.core.mail import EmailMessage, EmailMultiAlternatives, send_mail
from django.template.loader import get_template
from university_attendance_management.core.utils import get_current_time

class BaseEmailMessage:
    template_name = None

    def __init__(self, context: dict, subject: str):
        self._subject = subject
        self._context = context

    def send_mail(self, to: list, body: str):
        # Send a test email with plain text content
        test_message = "This is a test email message."
        mail = EmailMessage(
            subject="Test Email - " + self._subject,
            body=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to,
        )
        return mail.send(fail_silently=True)

    def send(self, to: list,attachments: list = None, *args, **kwargs):
        mail = EmailMessage(
            subject=self._subject,
            body=self._get_message(),
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=to,
            reply_to=kwargs.pop('reply_to', []),
        )
        mail.content_subtype = "html"

        if attachments:
            for attachment in attachments:
                if isinstance(attachment, tuple):
                    mail.attach(attachment[0], attachment[1], attachment[2])
                else:
                    mail.attach_file(attachment)
        return mail.send(fail_silently=True)

    def _get_message(self):
        return get_template(self.template_name).render(self._context)


def cron_started(task,today_date="",today_day=""):
    current_time = get_current_time()
    if settings.SEND_EMAIL:
        send_mail(
            subject=f'Cron Job Started for {task}',
            message=f'Your scheduled cron job has started running.{today_date}-{today_day} at {current_time}',
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=['binay@ittogether.com.au'],
            fail_silently=False,
        )

def cron_ended(task):
    if settings.SEND_EMAIL:
        send_mail(
            subject=f'Cron Job Ended for {task}',
            message=f'Your scheduled cron job has ended running.',
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=['binay@ittogether.com.au'],
            fail_silently=False,
        )
    
    
### HTML email msg for bulk upload ##
def format_errors_as_html(errors, total, fail, sheet): 
    success = total - fail
    message = f"""
    <p>Hello! There,</p>
    <p>The following errors occurred while processing the {sheet} data:</p>
    <div style="margin:10p">
        <b>Total:{total}</b> ||
        <b>Success:{success}</b> ||
        <b>Failed:{fail}</b> 
    </div>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
        <thead>
            <tr>
                <th style="background-color: #f2f2f2;">S/N</th>
                <th style="background-color: #f2f2f2;">Error</th>
            </tr>
        </thead>
        <tbody>
    """
    for index, error in enumerate(errors):
        index +=1
        message += f"""
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd;">{index}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{error['error']}</td>
            </tr>
        """
    message += f"""
        </tbody>
    </table>
    <p>Best regards,<br>Churchill</p>
    """
    return message


def send_html_email_message(errors, total, fail, sheet, email=None):
    """Function to send a beautifully formatted HTML email."""
    subject = 'Excel File Upload'
    html_content = format_errors_as_html(errors, total, fail, sheet)
    msg = EmailMultiAlternatives(
        subject,
        '', 
        settings.DEFAULT_FROM_EMAIL,  
        ["binayaparajuli17@gmail.com", "p.dahal@churchill.nsw.edu.au", email]
    )
    msg.attach_alternative(html_content, "text/html")
    msg.send()

    
    
from collections import defaultdict

def format_schedule_as_html(schedule_dict: defaultdict) -> str:
    html = """
    <h2>Schedule Summary</h2>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
      <thead style="background-color: #f2f2f2;">
        <tr>
          <th>Cell</th>
          <th>Schedules</th>
        </tr>
      </thead>
      <tbody>
    """

    for cell, schedules in schedule_dict.items():
        html += f"<tr><td>{cell}</td><td>"
        html += "<br>".join(schedule.replace(",", ", ") for schedule in schedules)
        html += "</td></tr>\n"

    html += """
      </tbody>
    </table>
    """
    return html



def send_html_email_message_schedule(scheduels_added_in_cohort):
    subject = 'Schedules in excel which are not present in our system'
    html_content = format_schedule_as_html(scheduels_added_in_cohort)
    msg = EmailMultiAlternatives(
        subject,
        '', 
        settings.DEFAULT_FROM_EMAIL,  
        ["binayaparajuli17@gmail.com", "p.dahal@churchill.nsw.edu.au"] 
    )
    msg.attach_alternative(html_content, "text/html")
    msg.send()
    print("Email sent successfully!")

