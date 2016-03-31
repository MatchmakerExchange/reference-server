# Matchmaker Exchange Reference Server
A simple server that stores patient records and implements the [Matchmaker Exchange API](https://github.com/ga4gh/mme-apis).

This is an example implementation, written by the [Matchmaker Exchange](http://www.matchmakerexchange.org/) technical team. The server uses a single elasticsearch instance to index the patient records, the [Human Phenotype Ontology](http://human-phenotype-ontology.github.io/), and Ensembl-Entrez-HGNC gene symbol mappings. By default, you can load the MME API benchmark dataset of 50 rare disease patient records compiled from the literature ([see the publication for more details](http://onlinelibrary.wiley.com/doi/10.1002/humu.22850)).

This code is intended to be illustrative and is **not** guaranteed to perform well in a production setting.

## Dependencies
- Python 2.7 or 3.X
- ElasticSearch


## Quickstart

1. Clone the repository:

    ```bash
    git clone https://github.com/MatchmakerExchange/reference-server.git
    cd reference-server
    ```

1. In a new shell, start up a local elasticsearch cluster, for example:

    ```bash
    # Download elasticsearch
    wget https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.1.1/elasticsearch-2.1.1.tar.gz
    tar -xzf elasticsearch-2.1.1.tar.gz
    cd elasticsearch-2.1.1/

    # Start up elasticsearch endpoint
    ./bin/elasticsearch
    ```

1. Set up your Python virtual environment and install the Python package dependencies, for example:

    ```bash
    # Set up virtual environment (add `-p python3` to force python 3)
    virtualenv .virtualenv
    source .virtualenv/bin/activate

    # Install dependencies
    pip install -r requirements.txt
    ```

1. Download and index vocabularies and sample data:

    ```bash
    python datastore.py
    ```

1. Run tests:

    ```bash
    python test.py
    ```

1. Start up MME reference server:

    ```bash
    python server.py
    ```

    By default, the server listens globally (`--host 0.0.0.0`) on port 8000 (`--port 8000`).

1. Try it out:

    ```bash
    curl -XPOST -H 'Content-Type: application/vnd.ga4gh.matchmaker.v1.0+json' \
         -H 'Accept: application/vnd.ga4gh.matchmaker.v1.0+json' \
         -d '{"patient":{
        "id":"1",
        "contact": {"name":"Jane Doe", "href":"mailto:jdoe@example.edu"},
        "features":[{"id":"HP:0000522"}],
        "genomicFeatures":[{"gene":{"id":"NGLY1"}}]
      }}' localhost:8000/match
    ```

## Questions

If you have any questions, feel free to post an issue on GitHub.

## Contributing

This repository is managed by the Matchmaker Exchange technical team. You can reach us via GitHub or by [email](mailto:api@matchmakerexchange.org).

Contributions are most welcome! Post an issue, submit a bugfix, or just try it out. We hope you find it useful.

## Implementations

We don't know of any organizations using this code in a production setting just yet. If you are, please let us know! We'd love to list you here.
