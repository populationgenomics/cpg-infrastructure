"""
Setup for cpg_infra module, providing a cloud abstraction
useful for Pulumi Infrastructure-as-code.
"""
import setuptools

with open('requirements.txt', encoding='utf-8') as f:
    requirements = [line.strip() for line in f.readlines()]

with open('cpg_infra/README.md', encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='cpg-infra',
    version='0.1.0',
    description='CPG Infrastructure',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/populationgenomics/cpg-infrastructure',
    license='MIT',
    packages=[
        'cpg_infra',
        *['cpg_infra.' + p for p in sorted(setuptools.find_packages('./cpg_infra'))],
    ],
    # package_data={},
    include_package_data=True,
    zip_safe=False,
    scripts=[],
    install_requires=requirements,
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
    entry_points={
        'cpginfra.plugins': [
            'billing_aggregator = cpg_infra.billing_aggregator.driver:BillingAggregator',
        ],
    }
)
