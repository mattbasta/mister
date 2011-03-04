from setuptools import setup, find_packages

setup(
    me='mister',
    version='0.6',
    description='A friendly, simple map/reduce library.',
    long_description=open('README').read(),
    author='Matt Basta',
    author_email='me@mattbasta.com',
    url='http://github.com/mattbasta/mister',
    license='BSD',
    packages=find_packages(exclude=['tests',
                                    'tests/*']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[p.strip() for p in open('./requirements.txt')
                                              if not p.startswith('#')],
    scripts=[],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',]
    )

