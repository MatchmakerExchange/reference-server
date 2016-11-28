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
