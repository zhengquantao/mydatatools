#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
mydatatools包打包脚本
"""

from setuptools import setup, find_packages


if __name__ == '__main__':
    setup(
        name='mydatatools',
        version='0.5',
        author='zhengquantao80',
        author_email='zhengquantao80@gmail.com',
        long_description=__doc__,
        packages=find_packages(),
        include_package_data=True,
        zip_safe=False,
        exclude_package_data={'': ['.gitignore']},
        install_requires=[
            'pandas==2.2.2',
            'SQLAlchemy==2.0.34',
            'PyMySQL==1.1.1',
            'paramiko==3.4.1',
            'loguru==0.7.2'
        ]
    )



