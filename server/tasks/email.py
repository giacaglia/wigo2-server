from __future__ import absolute_import
import logging
from rq.decorators import job

from config import Configuration
from server.db import wigo_db, redis
from server.models.user import User
from sendgrid import SendGridClient, Mail
from jinja2 import Environment, PackageLoader, Template


logger = logging.getLogger('wigo.web')


def create_sendgrid():
    return SendGridClient(Configuration.MAIL_USERNAME, Configuration.MAIL_PASSWORD, raise_errors=True)


@job('email', connection=redis, timeout=5)
def send_email_verification(user_id, resend=False):
    if not Configuration.PUSH_ENABLED:
        return

    sendgrid = create_sendgrid()

    user = User.find(user_id)

    verify_code = wigo_db.get_new_code({
        'type': 'verify_email',
        'user_id': user_id,
        'email': user.email
    })

    verify_link = '{}://{}/c/{}'.format('https' if Configuration.ENVIRONMENT != 'dev' else 'http',
                                        Configuration.WIGO_WEB_HOST, verify_code)

    logger.info('generated verify code for user "%s", "%s"' % (user.email, verify_code))

    first_name = user.first_name
    if not first_name:
        first_name = user.email

    msg = Mail()

    if resend:
        msg.set_subject('Everyone deserves a second chance')
    else:
        msg.set_subject('Welcome to Wigo')

    msg.set_from('Wigo <welcome@wigo.us>')

    if user.first_name and user.last_name:
        msg.add_to('%s <%s>' % (user.full_name, user.email))
    else:
        msg.add_to(user.email)

    msg.set_text("Hi %s\n\nPlease click the following link to verify your email address:\n\n%s\n\n"
                 % (first_name, verify_link))

    msg.set_html(render_template('confirmation_email.html',
                                 name=first_name, confirmation_link=verify_link, resend=resend))

    msg.add_unique_arg('user_id', user.id)
    msg.add_category('verify')
    msg.add_filter('opentrack', 'enable', 0)
    msg.add_filter('subscriptiontrack', 'enable', 1)
    msg.add_filter('subscriptiontrack', 'replace', '-UNSUB-')

    sendgrid.send(msg)
    logger.info('sent verification email to "%s"' % user.email)


def send_custom_email(user, subject, category, html, text, template_params=None):
    sendgrid = create_sendgrid()

    msg = Mail()
    msg.set_from('Wigo <welcome@wigo.us>')

    if user.first_name and user.last_name:
        msg.add_to('%s <%s>' % (user.full_name, user.email))
    else:
        msg.add_to(user.email)

    # subject and body are required, for some reason, with the sendgrid templates
    msg.set_subject(Template(subject).render(user=user))

    # turn on email opens tracking
    msg.add_filter('opentrack', 'enable', 1)
    msg.add_filter('subscriptiontrack', 'enable', 1)
    msg.add_filter('subscriptiontrack', 'replace', '-UNSUB-')

    params = {'user': user}
    if template_params:
        params.update(template_params)

    html = render_template('custom_email_inlined.html', html=Template(html).render(**params))
    msg.set_html(html)

    if text:
        text = Template(text + '\n\n\nClick here to unsubscribe: -UNSUB-\n').render(**params)
        msg.set_text(text)

    msg.add_unique_arg('user_id', user.id)

    if category:
        msg.add_category(category)

    sendgrid.send(msg)


def get_template(template):
    env = Environment(loader=PackageLoader('server'))
    return env.get_template(template)


def render_template(template, *args, **kwargs):
    template = get_template(template)
    return template.render(*args, **kwargs)