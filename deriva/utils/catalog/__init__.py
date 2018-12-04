# This is a namespace package.
# See http://peak.telecommunity.com/DevCenter/setuptools#namespace-packages
__import__('pkg_resources').declare_namespace(__name__)

import sys
IS_PY2 = (sys.version_info[0] == 2)
IS_PY3 = (sys.version_info[0] == 3)

if IS_PY3:
    from urllib.parse import quote as _urlquote, unquote as urlunquote
    from urllib.parse import urlparse, urlsplit, urlunsplit
    from http.cookiejar import MozillaCookieJar
else:
    from urllib import quote as _urlquote, unquote as urlunquote
    from urlparse import urlparse, urlsplit, urlunsplit
    from cookielib import MozillaCookieJar
