#!/bin/bash -xe

python -m flake8 ts/flint tests
python setup.py install
# Can't use the "coverage" script directly because its #! line is too long.
python -m coverage run setup.py test
python -m coverage html
python -m coverage xml
python setup.py build_sphinx
conda build --python "${PYTHON_VERSION}" recipe
"${MINICONDA}/bin/ts-conda-upload" ts "$("${MINICONDA}/bin/conda" build --python "${PYTHON_VERSION}" --output recipe)"
