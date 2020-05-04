"""
Afterglow Core: oauth plugin package

An OpenAuth2 plugin must subclass :class:`OAuthPlugin` and implement its
methods.
"""

from __future__ import absolute_import, division, print_function

import sys
import requests
import base64
import json
from datetime import datetime, timedelta
from urllib.parse import urlencode

from marshmallow.fields import Dict, String

from flask import request, url_for

from .. import app, auth, errors, url_prefix, Resource


__all__ = ['OAuthPlugin', 'OAuthUserProfile', 'OAuthToken']

if sys.version_info < (3, 1):
    # noinspection PyDeprecation
    base64_decode = base64.decodestring
else:
    base64_decode = base64.decodebytes

if app.config.get('DEBUG'):
    # Skip SSL certificate validation in debug mode
    if sys.version_info[0] < 3:
        # noinspection PyCompatibility,PyUnresolvedReferences
        from urllib2 import HTTPSHandler, build_opener, install_opener
    else:
        # noinspection PyCompatibility,PyUnresolvedReferences
        from urllib.request import HTTPSHandler, build_opener, install_opener
    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    install_opener(build_opener(HTTPSHandler(context=ctx)))

class OAuthUserProfile:
    id = None
    username = None
    email = None
    first_name = None
    last_name = None
    birth_date = None

class OAuthToken:
    def __init__(self, access, refresh, expiration):
        self.access = access
        self.refresh = refresh
        self.expiration = expiration


class OAuthPlugin(Resource):
    """
    Class for OAuth plugins
    """
    # Fields visible on the client side
    id = String(default=None)
    name = String(default=None)
    type = String(default=None)
    description = String(default=None)
    icon = String(default=None)
    register_users = String(default=None)
    authorize_url = String(default=None)
    request_token_params = Dict(default=None)
    client_id = String(default=None)

    # Internal fields related to access token exchange
    client_secret = None
    access_token_url = None
    access_token_method = None
    access_token_headers = None
    access_token_params = None

    def __init__(self, id=None, description=None, icon=None,
                 register_users=None, authorize_url=None,
                 request_token_params=None, client_id=None,
                 client_secret=None, access_token_url=None,
                 access_token_method='POST', access_token_headers=None,
                 access_token_params=None):
        """
        Initialize OAuth plugin

        :param str id: plugin ID
        :param str name: plugin name
        :param str description: plugin description
        :param str icon: plugin icon ID used by the client UI
        :param bool register_users: automatically register authenticated users
            if missing from the local user database; overrides
            REGISTER_AUTHENTICATED_USERS
        :param str authorize_url: URL for authorization (needed by client)
        :param dict request_token_params: additional parameters for auth code
            exchange, like scope
        :param str client_id: client ID
        :param str client_secret: client secret
        :param str access_token_url: URL for token exchange
        :param str access_token_method: HTTP method for access token URL;
            default: "POST"
        :param dict access_token_headers: additional headers for token exchange
        :param dict access_token_params: additional parameters for token
            exchange
        """
        super(OAuthPlugin, self).__init__()

        if id is None:
            self.id = self.name
        else:
            self.id = id

        if description is None:
            if self.description is None:
                self.description = self.name
        else:
            self.description = description

        if icon is not None:
            self.icon = icon
        if self.icon is None:
            self.icon = self.name

        if self.register_users is None:
            self.register_users = register_users

        self.authorize_url = authorize_url
        if request_token_params:
            self.request_token_params = request_token_params
        else:
            self.request_token_params = {}

        if not client_id:
            raise ValueError('Missing OAuth client ID')
        self.client_id = client_id

        if not client_secret:
            raise ValueError('Missing OAuth client secret')
        self.client_secret = str(client_secret)

        if not access_token_url:
            raise ValueError('Missing OAuth access token URL')
        self.access_token_url = str(access_token_url)

        if not access_token_method:
            raise ValueError('Missing OAuth access token method')
        access_token_method = str(access_token_method).upper()
        if access_token_method not in ('GET', 'POST'):
            raise ValueError('Invalid OAuth access token method "{}"'.format(
                access_token_method))
        self.access_token_method = access_token_method

        if access_token_headers:
            try:
                access_token_headers = dict(access_token_headers)
            except (TypeError, ValueError):
                raise ValueError(
                    'Invalid OAuth access token headers "{}"'.format(
                        access_token_headers))
        self.access_token_headers = access_token_headers

        if access_token_params:
            try:
                access_token_params = dict(access_token_params)
            except (TypeError, ValueError):
                raise ValueError(
                    'Invalid OAuth access token parameters "{}"'.format(
                        access_token_params))
        self.access_token_params = access_token_params

    def construct_authorize_url(self, state={}):
        """
        Generic authorization url formatter; implemented by OAuth plugin base that
        creates the OAuth server's authorization URL from state parameters

        :param dict state: additional application state to be added to OAuth state query parameter

        :return: authorization URL
        :rtype: str
        """

        state_json = json.dumps(state)
        qs = urlencode(dict(state=state_json, redirect_uri=url_for('oauth2_authorized', _external=True, plugin_id=self.id), client_id=self.client_id, **self.request_token_params))
        return '{}?{}'.format(self.authorize_url, qs)


    def get_token(self, code):
        """
        Generic token getter; implemented by OAuth plugin base that
        retrieves the token using an authorization code

        :param str code: authorization code

        :return: OAuthToken containing access, refresh, and expiration
        :rtype: OAuthToken
        """

        args = {
            'grant_type': 'authorization_code',
            'code': code,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': url_for('oauth2_authorized', _external=True, plugin_id=self.id),
        }
        if self.access_token_params:
            args.update(self.access_token_params)

        if self.access_token_method == 'POST':
            data = args
            args = None
        else:
            data = None

        try:
            resp = requests.request(
                self.access_token_method, self.access_token_url,
                params=args, data=data, headers=self.access_token_headers,
                verify=False if app.config.get('DEBUG') else None)
            if resp.status_code not in (200, 201):
                raise Exception(
                    'OAuth server returned HTTP status {}, message: {}'.format(
                        resp.status_code, resp.text))
            data = resp.json()

            # Get token expiration time
            expires = data.get('expires_in')
            if expires is not None:
                expires = datetime.utcnow() + timedelta(seconds=expires)

            return OAuthToken(access=data.get('access_token'),
                refresh=data.get('refresh_token'),
                expiration=expires)

        except Exception as e:
            raise auth.NotAuthenticatedError(error_msg=str(e))

    def get_user_profile(self, token):
        """
        Provider-specific user profile getter; implemented by OAuth plugin that
        retrieves the user's profile using the provider API and token

        :param OAuthToken token: provider API access, refresh, expiration token info

        :return: user profile
        :rtype: OAuthUserProfile
        """
        raise errors.MethodNotImplementedError(
            class_name=self.__class__.__name__, method_name='get_user_profile')
