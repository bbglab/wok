'''
Wok is a workflow management system implemented in Python that makes very easy
to structure the workflows, parallelize their execution
and monitor its progress easily.

It is designed in a modular way allowing to adapt it to different infraestructures.

For the time being it is strongly focused on clusters implementing
any DRMAA compatible resource manager such as Sun Grid Engine
among others with a shared folder among work nodes.

Other, more flexible infrastructures (such as the cloud)
are being studied for future implementations.
'''

import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

from wok import VERSION, AUTHORS, AUTHORS_EMAIL

with open("README.rst", "r") as f:
	DOC = f.read()

setup(
    name = 'bbglab.wok',
    version = VERSION,
    packages = find_packages(),
    scripts = [
		'scripts/wok-run'
	],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = [
		'docutils>=0.3',
		'SQLAlchemy==0.8.2',
		#'pygments',
		'Flask==0.10.1',
		'Flask-Login==0.2.8',
		'blinker==1.3'
	],

    include_package_data = True,

    # metadata for upload to PyPI
    author = AUTHORS,
    author_email = AUTHORS_EMAIL,
    description = 'Workflow management system',
    license = 'GPL 3.0',
    keywords = 'workflow dataflow analysis parallel',
    url = 'https://bitbucket.org/bbglab/wok',
	download_url = "https://bitbucket.org/bbglab/wok/get/{}.tar.gz".format(VERSION),
	long_description = DOC,

	classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
		'Environment :: Console',
		'Environment :: Web Environment',
		'Intended Audience :: Science/Research',
		'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering',
		'Topic :: Scientific/Engineering :: Bio-Informatics'
    ]
)
