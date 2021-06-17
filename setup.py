import setuptools

setuptools.setup(
    name="emr_containers",
    packages= setuptools.PEP420PackageFinder.find(include=['emr_containers', 'emr_containers.*']),
    install_requires=[
        "boto3>=1.17",
    ],
)