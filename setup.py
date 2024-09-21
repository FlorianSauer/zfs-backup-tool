import setuptools

setuptools.setup(
    name="ZfsBackupTool",
    packages=setuptools.find_packages(),
    version="0.2.0",
    scripts=["zfs-backup-tool.py"],
    install_requires=[
        "binpacking",
        "humanfriendly",
    ],
)
