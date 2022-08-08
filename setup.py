
import setuptools


setuptools.setup(
    name='cpg-infra',
    version='0.1.0',
    description='CPG Infrastructure as code',
    long_description=open('cpg_infra/README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/populationgenomics/cpg_infrastructure',
    license='MIT',
    packages=['cpg_infra']
    + ['cpg_infra.' + p for p in sorted(setuptools.find_packages('./cpg_infra'))],
    # package_data={},
    include_package_data=True,
    zip_safe=False,
    scripts=[],
    install_requires=[
        l.strip() for l in open('cpg_infra/requirements.txt').readlines()
    ],
    keywords=['cpg', 'infrastructure'],
    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)