import os
import sys

from distutils.core import Command, setup
from distutils.command.build_ext import build_ext
from traceback import print_exc

cython_available = False
try:
    from Cython.Distutils import build_ext
    from Cython.Distutils.extension import Extension
    cython_available = True
except ImportError, e:
    print 'WARNING: cython not available, proceeding with pure python implementation. (%s)' % e
    pass

try:
    import nose
except ImportError:
    nose = None

def get_ext_modules():
    if not cython_available:
        return []

    try:
        import gevent
    except ImportError, e:
        print 'WARNING: gevent must be installed to build cython version of gevent-redis (%s).', e
        return []

    return [Extension('geventredis.core', ['geventredis/core.pyx'], include_dirs=['.', 'geventredis']),]

class TestCommand(Command):
    """Custom distutils command to run the test suite."""

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # crude check for inplace build:
        try:
            import geventredis
        except ImportError:
            print_exc()
            print ("Could not import geventredis!")
            print ("You must build geventredis with 'python setup.py build_ext --inplace' for 'python setup.py test' to work.")
            print ("If you did build geventredis in-place, then this is a real error.")
            sys.exit(1)

        if nose is None:
            print ("nose unavailable, skipping tests.")
        else:
            return nose.core.TestProgram(argv=["", '-vvs', os.path.join(self._zmq_dir, 'tests')])

__version__ = (0, 2, 0)

setup(
    name = 'geventredis',
    version = '.'.join([str(x) for x in __version__]),
    packages = ['geventredis'],
    cmdclass = {'build_ext': build_ext, 'test': TestCommand},
    ext_modules = get_ext_modules(),
    author = 'Phus Lu',
    author_email = 'phus.lu@gmail.com',
    url = 'http://github.com/phus/gevent-redis',
    description = 'gevent redis client',
    long_description=open('README.md').read(),
    install_requires = ['gevent'],
    license = 'Apache',
)
