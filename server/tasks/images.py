from __future__ import absolute_import
from copy import deepcopy
import hashlib
import os
import threading
from urlparse import urlparse
from boto.s3.connection import S3Connection

import requests
from StringIO import StringIO
from PIL import Image
from config import Configuration
from server.models.user import User


saving_images = threading.local()


def save_images(user_id):
    cache = {}

    user = User.find(user_id)
    images = deepcopy(user.images)
    original_images = deepcopy(user.images)

    for img in reversed(images):
        url = img.get('url')
        small = img.get('small')
        height = img.get('height')
        width = img.get('width')

        # bad image if it has no url
        if not url:
            images.remove(img)
            continue

        # don't mess with facebook graph api images
        if 'graph.facebook.com' in url:
            continue

        try:
            # if the image isn't on the cdn, upload it there
            if Configuration.IMAGE_CDN not in url:
                img_obj = get_image(url, cache)
                if img_obj:
                    url_hash = hashlib.sha1(url.encode('utf-8')).hexdigest()
                    path = os.path.join('users', str(user.id % 1000),
                                        str(user.id), '{hash}.jpg'.format(hash=url_hash))
                    upload_image(path, img_obj)
                    url = 'https://{cdn}/{key}'.format(cdn=Configuration.IMAGE_CDN, key=path)
                    img['url'] = url

            # if no width and height set, get image and get width/height
            if not width or not height:
                img_obj = get_image(url, cache)
                if img_obj:
                    width, height = img_obj.size
                    if width:
                        img['width'] = width
                    if height:
                        img['height'] = height

            # if there is no small image, or the small image isn't on our cdn
            if (small is None or Configuration.IMAGE_CDN not in small) or \
                    (img.get('crop') != img.get('small_crop')):
                img_obj = get_image(url, cache)
                if img_obj:
                    small_img_obj = img_obj.copy()
                    crop = img.get('crop')
                    if crop and (crop.get('width') != width or crop.get('height') != height):
                        left = crop.get('x')
                        top = crop.get('y')
                        right = left + crop.get('width')
                        bottom = top + crop.get('height')
                        if (left + top + right + bottom) > 0:
                            small_img_obj = small_img_obj.crop((left, top, right, bottom))
                            img['small_crop'] = crop
                        else:
                            del img['crop']
                    else:
                        if 'small_crop' in img:
                            del img['small_crop']
                        if 'crop' in img:
                            del img['crop']

                    small_img_obj.thumbnail((200, 200), Image.ANTIALIAS)
                    split_path = os.path.split(urlparse(url).path)
                    dest = split_path[0] + '/' + os.path.splitext(split_path[1])[0]
                    if crop:
                        dest += '-{x}x{y}x{width}x{height}'.format(**crop)
                    dest += '-thumb.jpg'
                    if dest.startswith('/'):
                        dest = dest[1:]
                    upload_image(dest, small_img_obj)
                    img['small'] = 'https://{cdn}/{key}'.format(cdn=Configuration.IMAGE_CDN, key=dest)
        except ImageGoneException:
            images.remove(img)

    if len(images) == 0:
        images.append({
            'id': 'profile',
            'url': 'https://graph.facebook.com/%s/picture?width=600&height=600' % user.get_facebook_id(),
            'small': 'https://graph.facebook.com/%s/picture?width=200&height=200' % user.get_facebook_id()
        })

    if images != user.images:
        user = User.find(user_id)
        user.set_custom_property('images', images)

        saving_images.in_save = True
        try:
            user.save()
        finally:
            saving_images.in_save = False

        return True

    return False


def needs_images_saved(user):
    if not Configuration.CAPTURE_IMAGES:
        return False
    if getattr(saving_images, 'in_save', False):
        return False
    return any(needs_image_saved(img) for img in user.images)


def needs_image_saved(img):
    url = img.get('url')
    small = img.get('small')
    height = img.get('height')
    width = img.get('width')

    if not url:
        return True
    if 'graph.facebook.com' in url:
        return False
    if not (small and height and width):
        return True
    if Configuration.IMAGE_CDN not in url or Configuration.IMAGE_CDN not in small:
        return True
    if img.get('crop') != img.get('small_crop'):
        return True

    return False


def get_image(url, cache):
    img_obj = cache.get(url)
    if img_obj is None:
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                img_data = StringIO(resp.content)
                img_obj = Image.open(img_data)
                cache[url] = img_obj
            elif resp.status_code == 403 or resp.status_code == 404:
                raise ImageGoneException()
        except:
            pass
    return img_obj


def upload_image(path, img):
    img_data = StringIO()
    img.save(img_data, format='JPEG')
    s3conn = S3Connection(Configuration.IMAGES_AWS_ACCESS_KEY_ID, Configuration.IMAGES_AWS_SECRET_ACCESS_KEY)
    bucket = s3conn.get_bucket(Configuration.IMAGES_BUCKET)
    key = bucket.new_key(path)
    key.set_metadata('Content-Type', 'image/jpeg')
    key.set_metadata('Cache-Control', 'max-age=86400')
    key.set_contents_from_string(img_data.getvalue())
    key.set_acl('public-read')


class ImageGoneException(Exception):
    pass