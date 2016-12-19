from setuptools import setup, find_packages

NAME = 'mme-server'
VERSION = '0.2.0'
DESCRIPTION = 'A simple server that implements the Matchmaker Exchange API'
AUTHOR = 'Matchmaker Exchange API Team'
AUTHOR_EMAIL = 'api@matchmakerexchange.org'
LICENSE = 'MIT'
URL = 'https://github.com/MatchmakerExchange/reference-server'
DOWNLOAD_URL = '{}/tarball/{}'.format(URL, VERSION)
INSTALL_REQUIRES = [
    'Flask>=0.10.1',
    'Flask-Negotiate',
    # Use package versions for elasticserach 1.X
    # 'elasticsearch>=2.0.0,<3.0.0',
    # 'elasticsearch-dsl>=2.0.0,<3.0.0',
    'elasticsearch>=1.0.0,<2.0.0',
    'elasticsearch-dsl>=0.0.0,<1.0.0',
    'rdflib',
    'jsonschema',
    'rfc3987',
]
KEYWORDS = ['Matchmaker Exchange', 'Matchmaker Exchange API', 'patient matchmaking', 'genomics', 'rare disease']
CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'License :: OSI Approved :: MIT License',

    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
]

setup(
    name=NAME,
    packages=find_packages(),
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license=LICENSE,
    url=URL,
    download_url=DOWNLOAD_URL,
    keywords=KEYWORDS,
    classifiers=CLASSIFIERS,
    install_requires=INSTALL_REQUIRES,
    include_package_data=True,
    zip_safe=False,
    test_suite='mme_server.tests',
    entry_points={
        'console_scripts': [
            'mme-server=mme_server:main',
        ]
    }
)
