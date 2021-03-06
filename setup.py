import setuptools

setuptools.setup(
    name="emr_containers",
    packages= setuptools.PEP420PackageFinder.find(include=['emr_containers', 'emr_containers.*'], exclude=['mwaa.*']),
    install_requires=[
        "boto3>=1.17",
    ],
)