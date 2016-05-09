from setuptools import setup


setup(
    name='oem-storage-codernitydb',
    version='1.0.0',

    author="Dean Gardiner",
    author_email="me@dgardiner.net",

    install_requires=[
        'oem-framework>=1.0.0',

        'CodernityDB>=0.5.0'
    ]
)
