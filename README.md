# Matchmaker Exchange Reference Server
A simple illustrative reference server for the Matchmaker Exchange API.

The server is backed by elasticsearch, and creates local indexes of the Human Phenotype Ontology, Ensembl-Entrez-HGNC gene symbol mappings, and the MME API benchmark set of 50 rare disease patients.

## Dependencies
- Python 3.X or 2.7

## Quickstart

1. Start up a local elasticsearch cluster, for example:

    ```bash
    wget https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.1.1/elasticsearch-2.1.1.tar.gz
    tar -xzf elasticsearch-2.1.1.tar.gz
    cd elasticsearch-2.1.1/
    # Start up elasticsearch endpoint
    ./bin/elasticsearch
    ```

1. Set up your Python virtual environment and install necessary Python packages, for example:

    ```bash
    # Clone repository
    git clone https://github.com/MatchmakerExchange/reference-server.git
    cd reference-server
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
    curl -XPOST -d '{"patient":{
        "id":"1",
        "contact": {"name":"Jane Doe", "href":"mailto:jdoe@example.edu"},
        "features":[{"id":"HP:0000522"}],
        "genomicFeatures":[{"gene":{"id":"NGLY1"}}]
      }}' localhost:8000/match
    ```


## TODO
- Avoid costly/redundant parsing `api.Patient` objects when generating MatchResponse objects from patients in database
- Inspect `Accepts` header for API versioning
- Add `Content-Type` header to responses
- Handle errors with proper HTTP statuses and JSON message bodies
- Add tests for gene index
- Add end-to-end API query tests
- Add parser tests
- Add example for simple UI scoring
