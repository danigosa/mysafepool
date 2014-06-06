import os
from setuptools import setup

README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='mysafepool',
    version='0.3',
    packages=['mysqlpool'],
    include_package_data=True,
    license='MIT License',  # example license
    description='Safest MySQL Pool',
    long_description=README,
    url='http://codeispoetry.me/',
    author='danigosa',
    author_email='danigosa@gmail.com',
    install_requires=[
        "Django",
        "sqlalchemy",
        "pymysql"
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT',  # example license
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        # Replace these appropriately if you are stuck on Python 2.
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
)
