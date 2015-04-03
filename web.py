from __future__ import absolute_import

from flask import Flask
from flask.ext.admin import Admin
from redis import Redis
from server.admin import UserModelView, GroupModelView, ConfigView, NotificationView, MessageView
from server.db import WigoRedisDB
from server.models import User, Group, Config, Notification, Message

redis = Redis()
db = WigoRedisDB(redis)

app = Flask(__name__)
app.secret_key = '034599jkrtbg30ijwerrgjvn'
admin = Admin(app, name='Wigo')
admin.add_view(UserModelView(User, db))
admin.add_view(GroupModelView(Group, db))
admin.add_view(MessageView(Message, db))
admin.add_view(NotificationView(Notification, db))
admin.add_view(ConfigView(Config, db))
app.run(debug=True)