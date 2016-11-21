#!/bin/bash -xe

python -m flake8 ts/flint tests || /bin/true
python setup.py install
# Can't use the "coverage" script directly because its #! line is too long.
#python -m coverage run setup.py test
#python -m coverage html
#python -m coverage xml
python -m pytest -v -m 'not human' --cov=ts.flint --cov-report=term-missing --cov-report=xml
export PYTHONPATH=$(python -c "import os.path; from ts.spark import pypusa; l = pypusa.Launcher(); l.bootstrap(); d = os.path.join(l.spark_dir, 'python'); print(':'.join([d, os.path.join(d, 'lib', 'py4j-0.9-src.zip')]))" | tail -n1)
python setup.py build_sphinx
conda build --python "${PYTHON_VERSION}" recipe
