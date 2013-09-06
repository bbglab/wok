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

setup(
    name = 'wok',
    version = VERSION,
    packages = find_packages(),
    scripts = [
		'scripts/wok-run'
	],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = [
		'docutils>=0.3',
		'distribute>=0.6.3',
		'SQLAlchemy==0.8.2',
		'pygments',
		'Flask==0.10.1',
		'Flask-Login==0.2.7',
		'Flask-SQLAlchemy==1.0'
	],

    package_data = {
        # If any package contains *.txt or *.pdf files, include them:
        '' : ['*.txt', '*.pdf', '*.html']
    },

    # metadata for upload to PyPI
    author = AUTHORS,
    author_email = AUTHORS_EMAIL,
    description = 'Workflow management system',
    license = 'GPL 3.0',
    keywords = 'workflow dataflow analysis parallel',
    url = 'https://github.com/chris-zen/wok',
	long_description = __doc__,

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
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering',
		'Topic :: Scientific/Engineering :: Bio-Informatics'
    ]
)
