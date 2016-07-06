#
import setuptools

import pypandoc

long_description = pypandoc.convert('README.md', 'rst')

setuptools.setup(name='iqfeed',
                 version='0.4',
                 description='IQFeed / DTN Data downloader',
                 long_description=long_description,
                 classifiers=[
                             'Development Status :: 4 - Beta',
                             'License :: OSI Approved :: Apache Software License',
                             'Programming Language :: Python :: 2.7',
                             'Topic :: Office/Business :: Financial :: Investment',

                 ],
                 url='http://github.com/tibkiss/iqfeed-dl',
                 author='Tibor Kiss',
                 author_email='tibor.kiss@gmail.com',
                 license='Apache License, Version 2.0',
                 packages=setuptools.find_packages(),
                 install_requires=['docopt', 'pytz', 'backports.functools_lru_cache'],
                 entry_points={'console_scripts': ['iqfeed = iqfeed.main:main']},
                 zip_safe=False)
