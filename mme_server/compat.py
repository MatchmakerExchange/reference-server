"""
Compatibility code
"""

try:
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve

try:
    from urllib2 import urlopen, Request
except ImportError:
    from urllib.request import urlopen, Request

try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit
