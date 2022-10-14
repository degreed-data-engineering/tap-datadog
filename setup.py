from setuptools import setup, find_packages


setup(name = "tap-datadog",
    version = 0.1,
    description = "Datadog for Degreed taps in Meltano",

    author = "Degreed",
    author_email = "",
    url = "https://github.com/degreed-data-engineering/tap-datadog",
    packages = find_packages(),
    package_data = {},
    include_package_data = True,
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Science/Research',
      'Operating System :: OS Independent',
      'Programming Language :: Python',
      'Topic :: Scientific/Engineering :: Astronomy'
      ],
    zip_safe=False
)