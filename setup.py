from setuptools import setup

setup(
    name='django-brushfire',
    version='0.1.0',
    author="Mike O'Malley",
    author_email='spuriousdata@gmail.com',
    description='Solr-only replacement for Haystack.',
    long_description=open('README.rst').read(),
    license='LICENSE',
    url='http://github.com/spuriousdata/brushfire',
    packages=['brushfire'],
    include_package_data=True,
    install_requires=[
        'Django >= 1.4.0',
        'requests >= 2.3.0',
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Django",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development",
        "Environment :: Web Environment",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
    ],
)
