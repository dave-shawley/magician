#!/usr/bin/env python
#
import setuptools

import magician


def read_requirements(file_name):
    requirements = []
    try:
        with open(file_name) as f:
            for line in f:
                try:
                    line = line[:line.index('#')].strip()
                except ValueError:
                    pass
                line = line.strip()
                if line.startswith('-r'):
                    requirements.extend(read_requirements(line[2:].strip()))
                elif line.startswith('-'):
                    pass  # skip instruction lines
                elif line:
                    requirements.append(line)
    except IOError:
        pass

    return requirements


install_requirements = read_requirements('requirements.txt')
testing_requirements = read_requirements('test-requirements.txt')

setuptools.setup(
    name='magician',
    description='Pulls rabbits out of the Async hat.',
    long_description='\n' + open('README.rst').read(),
    version=magician.__version__,
    author='Dave Shawley',
    author_email='daveshawley@gmail.com',
    url='http://github.com/dave-shawley/magician',
    install_requires=install_requirements,
    tests_require=testing_requirements,
    packages=setuptools.find_packages(exclude=['tests.*', 'tests']),
    zip_safe=True,
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Development Status :: 1 - Planning',
    ],
)
