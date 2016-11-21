ts.flint
========

We must assume that ts.flint is a package that does something great.

Installing
----------

You can install directly with `setup.py` or by using `pip`:

    python setup.py install
    # -or-
    pip install .
    
You can also install directly from gitlab:

    pip install 'git+https://gitlab.twosigma.com/analytics/flint@<version tag>#subdirectory=python'

Developing
----------

First, create an environment using `conda` and do a "development
install" into it:

    conda create --yes --name ts-flint-dev --file requirements.txt
    source activate ts-flint-dev
    python setup.py develop

Now you can edit code and run scripts or a Python shell, and imports
of your modules and packages will come from this directory, so your
changes will take effect.

    >>> import os
    >>> import ts.flint
    >>> ts.flint.__file__[len(os.getcwd()):]
    '/ts/flint/__init__.py'
    
Running Tests
-------------

Tests are written with the builtin `unittest` package, live in
`tests/`, and are run with `setup.py`:

    python setup.py test
    
You can get code coverage by running them under `coverage`:

    coverage run setup.py test
    coverage report
    
Instead of `coverage report`'s text output, you can get an HTML report
including branch coverage information:

    coverage run setup.py test
    coverage html
    (cd build/coverage/html; python -m http.server 8080)
    
Then visit http://localhost:8080.

Building Documentation
----------------------

Docs live in `docs/` and can be built with `setup.py`:

    python setup.py build_sphinx
    (cd build/sphinx/html; python -m http.server 8080)

Releases
--------

Version information is pulled from git tags by `versioneer`, so
cutting a new release is simply tagging and pushing the tag:

    git tag 0.1.2
    git push origin 0.1.2
    
Once this tag exists, future package and documentation builds will
automatically get that version, and users can `pip install` using that
git tag from gitlab.

    pip install git+https://gitlab.twosigma.com/analytics/flint@0.1.2#subdirectory=python

Other projects can depend on your project with `dependency_links`:

    setup(
        ...,
        install_requires=['ts-flint==0.1.2'],
        dependency_links=['https://gitlab.twosigma.com/analytics/flint@0.1.2#egg=ts-flint-0.1.2&subdirectory=python'],
    )

Bugs
----

Please report bugs to Leif Walsh <leif@twosigma.com>.
