# vbackup
A simple script for incremental, versioned backups

**Usage**
```
vbackup info <file>
vbackup build <directory> <file>
vbackup restore [--ver=<id>|--num=<num>] <directory> <file>
vbackup trim [--output=<file>] <num> <file>
```
  
**Commands**
```
info              Show information about backup
build             Build backup from directory
restore           Restore backup to directory
trim              Trim backup to <num> versions
```

**Options**
```
--ver=<id>        Version ID to restore
--num=<num>       Version number to restore
--output=<file>   Save trimmed backup to separate file
```
