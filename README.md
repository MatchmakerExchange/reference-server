# Matchmaker Exchange Reference Server
[![Build Status](https://travis-ci.org/MatchmakerExchange/reference-server.svg?branch=master)](https://travis-ci.org/MatchmakerExchange/reference-server)
[![License](https://img.shields.io/github/license/MatchmakerExchange/reference-server.svg)](LICENSE.txt)
[![Coverage Status](https://img.shields.io/coveralls/MatchmakerExchange/reference-server/master.svg)](https://coveralls.io/github/MatchmakerExchange/reference-server?branch=master)

A simple server that stores patient records and implements the [Matchmaker Exchange API](https://github.com/ga4gh/mme-apis).

This is an example implementation, written by the [Matchmaker Exchange](http://www.matchmakerexchange.org/) technical team. The server uses a single elasticsearch instance to index the patient records, the [Human Phenotype Ontology](http://human-phenotype-ontology.github.io/), and Ensembl-Entrez-HGNC gene symbol mappings. By default, you can load the MME API benchmark dataset of 50 rare disease patient records compiled from the literature ([see the publication for more details](http://onlinelibrary.wiley.com/doi/10.1002/humu.22850)).

This code is intended to be illustrative and is **not** guaranteed to perform well in a production setting.


## Dependencies

- Python 2.7 or 3.3+
- ElasticSearch 2.x


## Quickstart

1. Clone the repository:

    ```sh
    git clone https://github.com/MatchmakerExchange/reference-server.git
    cd reference-server
    ```

1. Install the Python package dependencies (it's recommended that you do this inside a [Python virtual environment](#install-venv)):

    ```sh
    pip install -e .
    ```

1. Start up your elasticsearch server in another shell (see the [ElasticSearch instructions](#install-es) for more information).

    ```sh
    ./path/to/elasticsearch
    ```

1. Download and index vocabularies and sample data:

    ```sh
    mme-server quickstart
    ```

1. Run tests (must run quickstart first):

    ```sh
    mme-server test
    ```

1. Authorize an incoming server:

    ```sh
    mme-server clients add myclient --label "My Client" --key "<CLIENT_AUTH_TOKEN>"
    ```

    Leave off the `--key` option to have a secure key randomly generated for you.

1. Start up MME reference server:

    ```sh
    mme-server start
    ```

    By default, the server listens globally (`--host 0.0.0.0`) on port 8000 (`--port 8000`).

1. Try it out:

    ```sh
    curl -XPOST \
      -H 'X-Auth-Token: <CLIENT_AUTH_TOKEN>' \
      -H 'Content-Type: application/vnd.ga4gh.matchmaker.v1.0+json' \
      -H 'Accept: application/vnd.ga4gh.matchmaker.v1.0+json' \
      -d '{"patient":{
        "id":"1",
        "contact": {"name":"Jane Doe", "href":"mailto:jdoe@example.edu"},
        "features":[{"id":"HP:0000522"}],
        "genomicFeatures":[{"gene":{"id":"NGLY1"}}],
        "test": true
      }}' localhost:8000/v1/match
    ```

## Installation

## <a name="install-venv"></a> Your Python environment

It's recommended that you run the server within a Python virtual environment so dependencies are isolated from your system-wide Python installation.

To set up your Python virtual environment:

```sh
# Set up virtual environment within a folder '.virtualenv' (add `-p python3` to force python 3)
virtualenv .virtualenv
```

You can then activate this environment within a particular shell with:

```sh
source .virtualenv/bin/activate
```

### <a name="install-es"></a> ElasticSearch

First, download elasticsearch:

```sh
wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.6.zip
unzip elasticsearch-1.7.6.zip
```

Then, start up a local elasticsearch cluster to serve as our database (`-Des.path.data=data` puts the elasticsearch indices in a subdirectory called `data`):

```sh
./elasticsearch-1.7.6/bin/elasticsearch -Des.path.data=data
```



## Loading custom patient data

Custom patient data can be indexed by the server in two ways (if a patient 'id' matches an existing patient, the existing patient is updated):

1. Batch index from the command line:
    ```sh
    mme-server index patients --filename patients.json
    ```

1. Batch index from the Python interface:

    ```py
    >>> from mme_server.backend import get_backend
    >>> db = get_backend()
    >>> patients = db.get_manager('patients')
    >>> patients.index('/path/to/patients.json')
    ```

1. Single patient index the Python interface:

    ```py
    >>> from mme_server.backend import get_backend
    >>> db = get_backend()
    >>> patients = db.get_manager('patients')
    >>> from mme_server.models import Patient
    >>> patient = Patient.from_api({...})
    >>> patients.index_patient(patient)
    ```


## Questions

If you have any questions, feel free to post an issue on GitHub.


## Contributing

This repository is managed by the Matchmaker Exchange technical team. You can reach us via GitHub or by [email](mailto:api@matchmakerexchange.org).

Contributions are most welcome! Post an issue, submit a bugfix, or just try it out. We hope you find it useful.
