#!/bin/bash
python3 setup.py sdist
twine upload -r pypi dist/txhqueue-*.gz
