from setuptools import setup, find_packages
setup(
    name = "gevent-redis",
    version = "0.1",
    install_requires = ['gevent'],
    packages = find_packages(),
)
