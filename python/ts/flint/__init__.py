#
#       Copyright (c) 2016 Two Sigma Investments, LP
#       All Rights Reserved
#
#       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
#       Two Sigma Investments, LP.
#
#       The copyright notice above does not evidence any
#       actual or intended publication of such source code.
#

'''ts.flint contains :mod:`.module` which isn't terribly interesting.'''

__author__ = 'Leif Walsh'
__maintainer__ = 'Leif Walsh'
__email__ = 'leif@twosigma.com'

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
