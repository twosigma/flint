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

import unittest


class VersionTestCase(unittest.TestCase):
    def test_version(self):
        import ts.flint
        self.assertGreater(len(ts.flint.__version__), 0)
