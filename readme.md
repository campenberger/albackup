albackup - SQLAlchemy Database Backup
=====================================

**Disclaimer:** At this point it only works with SQLServer databases and has a fair bit of hacks to make it work.

Motivation
----------

We have been a Windows shop and are slowley shifting from Windows to Linux and from MS SQLServer to PostgreSQL. We also
utilize AWS and in particular RDS for our sites. While the RDS snapshots are great, it is not possible to backup one 
particular database and the standard database backup tools are not available. There are some Windows tools available,
but they are not very scriptable. Therefore we created our own backup utility that uses SQLAlchemy's reflection capabilities
and Pythons pickle to seralize data and runs on Linux

Pre-requisits
-------------
- unixODBC v2.3
- freeTDS-dev.0.99.134 

unixODBC must be configured for UTF-8 as default characterset. For example

    ./configure \
        --enable-gui=no \
        --enable-drivers=no \
        --enable-iconv \
        --with-iconv-char-enc=UTF8 \
        --with-iconv-ucode-enc=UTF16LE \
        --libdir=/usr/lib/x86_64-linux-gnu \
        --prefix=/usr \
        --sysconfdir=/etc

Ubuntu doesn't configure it that way and you will get problems, if you have non-ascii characters in table or column names

Installation
------------

- Install freeTDS and unixODBC. Make sure they work, using there respective clients tsql and isql
- clone the repository
- optional: create and active a virtual environment
- install the python requirements `pip install -r freeze.txt`
- Create a copy of sample.json for your backup and edit to configure the database you want to backup. You need to remove all the quotes in the file, because it is json.

Running
-------

There is one python module for backup and restore. Run it with `python -m albackup`, which will just result in a general
usage statement. The option -h gives you are more detailed explanation:

    # python -m albackup -h 
    usage: python -m albackup [-h] [--cfg CFG_FILE] [--meta-cache META_CACHE]
                              [--backup-dir BACKUP_DIR] [--debug]
                              MODE

    positional arguments:
      MODE                  mode of operation (dump or restore,chg-password)

    optional arguments:
      -h, --help            show this help message and exit
      --cfg CFG_FILE, -c CFG_FILE
                            Configuration for dump or restore operation
      --meta-cache META_CACHE
                            Allow caching of database meta data
      --backup-dir BACKUP_DIR
                            Target directory for backups
      --debug, -d           Run in debug mode

The tool has three run modes and 3 operate on a json configuration file. The 3 modes are:

- *dump* - Backup a database
- *restore* - Restore a database
- *chg-password* - Set or change the database password

The afore mentioned configuration file looks like the following, sans the comments, which are not json:

    // IMPORTANT: JSON files can't contain comments, so you need to remove
    // them all, before using the file
    {   "db_user":          "<replace with database user name>",

        // leave the password null and use the tool in the chg-password mode
        // to set the encrypted database password
        "db_password":      null,

        "db_server":        "<replace with IP or FQDN of database server>",

        "db_port":          1433,

        "db_name":          "<replace with target database for backup or restore>",

        // set to yes to allow restoring to the database, only do this when you
        // are absolutely sure you have the right database
        "allow_restore":    false,

        // enable the referential integrity check at the end of the restore. Set
        // this to false, for reporting databases
        "enable_ri_check":  true
    }

All modes look for a default configuration with the name `albackup.json` in the current working directory. **Note:** It is 
recommended to use different configuration files for backup and restore and have allow_restore set to false in
the backup configuration to make sure a database does not get overwritten inadvertently.

### Encrypt passwords

Each backup configuration file contains the connection password in an encrypted form. The set the initial password or change it, 
run `python -m albackup --cfg dump.json chg-password`. You will be prompted for the old password, if there is when and then for 
new password. You will have to type it in a 2nd time to confirm it. 

### Backup

A backup is made with the dump mode:

    python -m albackup --cfg dump.json --backup-dir ./backups dump

This will create a new subdirectory under backups based on name and host in the configuration file, as well as the current time. The directory contains a `*.pickle` file for each table as well as one file `_metadata.pickle` with all the meta data of the backup run. 

The tool will log progress information to stdout, and optional additional debugging information with --debug command line flag.

### Restore

Restore is similar:

    python -m albackup --cfg restore.json --backup-dir ./backups/some_db@some_host--20160427-1533

The main difference is that a specific backup directory must be given that will be restored. Furthermore the configuration file must
explicitly allow restoring to the database with `"allow_restore": false`, because tables will be deleted and re-created.

#### Restoring replicated databases

Some of our databases are replicated with SymmetricDS. This needs to be taken into consideration when restoring a database.

For moving a database:
* Pause the node replication
* Restore the database in the new location
* Create the SymmetricDS state database
* Change the node configurion
* Turn the node replication back on

To restore an older version of the database:
* Pause the node replication
* Restore the database in the new location
* Delete the node configuration
* Wipe or re-created the target database
* Re-setup the replication and perform an inital data load

### test_all.py

`test_all.py` is a utility that takes a configuration file test.json and iterates over a number of databases that it backs up, restore to a temp database and performs a schema compare between the two. It uses SQLWorkbench to do the compare, so the location of the tool must
be given as a parameter.

    {   
        "restore": {
            "name":             "restore-test",
            "db_user":          "<db dest user>",
            "db_password":      "<db dest password>",
            "db_server":        "<db dest host>",
            "db_port":          1433,
            "db_name":          "<db dest name>",
            "allow_restore":    true
        },
        "databases": [
            {
                "name": "src1",
                "db_password": "<db src1 password>", 
                "db_name": "<db src1 name>", 
                "db_user": "<db src1 user>", 
                "db_server": "<db src1 host>", 
                "db_port": 1433,
                "enable_ri_check":  true,
                "skip": true,
                "comment": "PASSED 2016-04-21"
            },
            {
                "name": "src2",
                "db_password": "<db src2 password>", 
                "db_name": "<db src2 name>", 
                "db_user": "<db src2 user>", 
                "db_server": "<db src1 host>", 
                "db_port": 1433,
                "enable_ri_check":  false,
                "skip": true,
                "comment": "PASSED 2016-04-21"
            }
        ]
    }

Please note that defaults `skip=false` and `enable_ri_check=true` will be used by test_all.py and can be overwritten in the 
configuration file. Extra attributes like `comment` can be given, but will be ignored.
