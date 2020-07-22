'''A utility for creating incremental backup archives.

Usage:
  vbackup info <file>
  vbackup build <directory> <file>
  vbackup restore [--ver=<id>|--num=<num>] <directory> <file>
  vbackup trim [--output=<file>] <num> <file>
  vbackup -h | --help
  
Commands:
  info              Show information about backup
  build             Build backup from directory
  restore           Restore backup to directory
  trim              Trim backup to <num> versions

Options:
  -h --help         Display this screen 
  --ver=<id>        Version ID to restore
  --num=<num>       Version number to restore
  --output=<file>   Save trimmed backup to separate file
'''
import os
import sys
import tarfile
import fnmatch
import json
import logging
import time
import tempfile
import zipfile
from tqdm import tqdm
from collections import OrderedDict
from docopt import docopt

logger = logging.getLogger('backup')

def taraddstr(tarobj, arcname, string):
    """Saves a string as a file in specified tar archive"""
    with tempfile.TemporaryFile() as temp:
        temp.write(string.encode())
        temp.seek(0)
        tinfo = tarobj.gettarinfo(arcname=arcname, fileobj=temp)
        tarobj.addfile(tinfo, temp)

def _copyfileobj(src, dst, length=None, exception=OSError):
    """Copy length bytes from fileobj src to fileobj dst.
       If length is None, copy the entire content.
    """
    bufsize = 4*1024*1024

    if length == 0:
        return
    if length is None:
        shutil.copyfileobj(src, dst, bufsize)
        return

    blocks, remainder = divmod(length, bufsize)
    for b in range(blocks):
        buf = src.read(bufsize)
        if len(buf) < bufsize:
            raise exception("unexpected end of data")
        dst.write(buf)

    if remainder != 0:
        buf = src.read(remainder)
        if len(buf) < remainder:
            raise exception("unexpected end of data")
        dst.write(buf)
    return

tarfile.copyfileobj = _copyfileobj      # Increased copy buffer size

class BackupVersion():
    def __init__(self, id = None, time = 0, size = 0, sizedelta = 0):
        self.id = id
        self.num = 0
        self.time = time                # Build time (in Unix time)
        self.size = size
        self.sizedelta = sizedelta      # Difference between version and last version
        self.files = {}                 # Keys are file names, values are BackupFile objects
        self.info = ''                  # Archive name of version info JSON
        self.data = ''                  # Archive name of version data ZIP
        self.newfiles = 0               # Number of files changed since last version

    def build_info(self):
        verinfo = { 'id': self.id, 
                    'time': self.time,
                    'size': self.size,
                    'sizedelta': self.sizedelta,
                    'files': {} }

        for f in self.files.values(): 
            verinfo['files'][f.name] = { 'mod': f.mod, 'size': f.size,
                'location': f.location }

        return verinfo

    def set_id(self, id):
        self.id = id
        self.info = 'versions/{}/version.json'.format(self.id)
        self.data = 'versions/{}/data.zip'.format(self.id)
        
class BackupFile:
    def __init__(self, name = '', size = 0, mod = 0, location = None, path = None):
        self.name = name            # Name of file in archive
        self.size = size
        self.mod = mod              # Modification time
        self.location = location    # Version the file is located in
        self.path = path            # Path to file (backup build only)

class Backup:
    def __init__(self, file='', id = None):
        self.id = id                                # ID of backup (not used internally)
        self.src = None                             # Source directory
        self.include = None
        self.exclude = None
        self.versions = {}                          # Keys are version IDs, values BackupVersion objects 
        self.file = os.path.normpath(file)
        self.filename = os.path.basename(file)
        self.curver = BackupVersion()               # Current (working) version
        self.lastver = BackupVersion()              # Most recent version in archive

        if os.path.isfile(file): self.load()

    def load(self):
        with tarfile.open(self.file) as t:
            verpaths = fnmatch.filter(t.getnames(),"versions/*/version.json")

            for path in verpaths:
                folder = os.path.split(path)[0]                 # Get version folder
                verfile = t.extractfile(path)                   # Extract version info.json...
                verinfo = json.loads(verfile.read().decode())   
                version = BackupVersion(verinfo['id'], verinfo['time'], 
                    verinfo['size'], verinfo['sizedelta'])
                version.info = path                             # Save archive name of version info 
                version.data = '{}/{}'.format(folder, 'data.zip')   # Save archive name of version data

                for item, data in verinfo['files'].items():
                    file = BackupFile(item, data['size'], data['mod'], data['location'])
                    version.files[item] = file

                self.versions[version.id] = version

            info = json.loads( t.extractfile('info.json').read().decode() )     # Get backup info
            self.id = info['id']
            self.include = info['include']
            self.exclude = info['exclude']
            self.src = info['src']

        timesort = sorted(self.versions.values(), key=lambda v: v.time)
        for idx, version in enumerate(timesort): version.num = idx+1
        latest = timesort[-1]      # Get most recent version
        self.lastver = latest

    def build(self, src=None, include=[], exclude=[]):  
        self.curver = BackupVersion()  
        curver = self.curver        # Shorter
        curver.time = round(time.time())
        if curver.time <= self.lastver.time: curver.time = self.lastver.time + 1
        tstr = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(curver.time))
        curver.set_id(tstr)

        # Normalise include/exclude paths
        include = [os.path.normpath(i) for i in include] if include else None
        exclude = [os.path.normpath(e) for e in exclude] if exclude else None
        self.include = include
        self.exclude = exclude

        lfiles = self.lastver.files  # File list from preceding backup version

        if not src: src = self.src
        else: self.src = os.path.realpath(src)

        logging.debug('Scanning for files in source directory ''{}'''.format(src))
        
        for root, dirs, files in os.walk(src):
            rel = os.path.relpath(root, src)
            # Remove unnecessary directories if includes/excludes are specified
            for d in reversed(dirs):
                drel = os.path.normpath(os.path.join(rel, d))
                if include and not ([i for i in include if i.startswith(drel) or
                        fnmatch.fnmatch(drel, os.path.join(os.path.dirname(i), '*')) or 
                        not os.sep in i]):
                    dirs.remove(d)
                if exclude and [e for e in exclude if fnmatch.fnmatch(drel, e)]: 
                    dirs.remove(d)

            for file in files:   
                fpath = os.path.realpath(os.path.join(root, file))     # Path to file
                frel = os.path.normpath(os.path.join(rel, file))
                frel_arc = frel.replace('\\','/')  # Archive name - uses forward slashes
                stat = os.stat(fpath)
                mod = stat.st_mtime          # Modification time

                if include:
                    if not [m for m in include if fnmatch.fnmatch(frel, m)]: continue  
                if exclude:
                    if [m for m in exclude if fnmatch.fnmatch(frel, m)]: continue  
                
                if frel_arc in lfiles:
                    existing = lfiles[frel_arc]
                    if mod == existing.mod and stat.st_size == existing.size:  
                        curver.files[frel_arc] = existing
                        curver.size += stat.st_size
                        continue                        # Skip file if same as previous version

                curver.size += stat.st_size
                curver.sizedelta += stat.st_size
                curfile = BackupFile(frel_arc, stat.st_size, mod, curver.id, fpath)
                curver.newfiles += 1
                curver.files[frel_arc] = curfile     # Add file to version file dict    

        logging.debug('{} changed files found'.format(curver.newfiles))


    def save(self, file=None, verbose=True):
        if not file: file = self.file
        curver = self.curver

        verinfo = curver.build_info()       # Convert version info to JSON
        bakinfo = { 'id': self.id, 'src': self.src, 
            'include': self.include, 'exclude': self.exclude}

        # Add files not in previous versions
        savelist = [f for f in curver.files.values() if f.location == curver.id]

        if savelist:
            if verbose: logging.info("Backing up '{}' > '{}'".format(self.src, os.path.basename(file)))
            with tarfile.open(file, 'a') as t:           
                with tempfile.SpooledTemporaryFile(256000000) as temp:   # Write data zip to temp file
                    with zipfile.ZipFile(temp, 'w', compression=zipfile.ZIP_DEFLATED) as z:
                        for f in tqdm(savelist, ncols=100): 
                            compression = None
                            name, ext = os.path.splitext(f.name)
                            if ext in {'.png','.jpg','.zip'}: compression = zipfile.ZIP_STORED
                            z.write(f.path, f.name, compression)    

                    temp.seek(0)
                    tinfo = t.gettarinfo(arcname=curver.data, fileobj=temp)
                    t.addfile(tinfo, temp)     # Add zip created in temp to tarball
                
                if not 'info.json' in t.getnames(): taraddstr(t, 'info.json', json.dumps(bakinfo)) # Backup info
                taraddstr(t, curver.info, json.dumps(verinfo,sort_keys=True,indent=4)) # Version info
                      
        else: logging.info("Skipped backup '{}' (no files to backup)".format(self.src))


    def restore(self, dst, ver = None, to_zip = False):
        if not ver: version = self.lastver
        elif ver not in self.versions: 
            logging.warning('Version {} does not exist. Restoring lastest version instead'.format(ver))
            version = self.lastver      
        else: version = self.versions[ver]
        
        extractlist = {}    # Keys: version id, Values: file names to extract from version
        for file in version.files.values():
            if not file.location in extractlist: extractlist[file.location] = [file.name]
            else: extractlist[file.location].append(file.name)
        
        if to_zip: zfileobj = zipfile.ZipFile(dst, 'w', compression=zipfile.ZIP_DEFLATED)
        with tarfile.open(self.file) as t:
            for ver, files in extractlist.items():
                zfile = t.extractfile(self.versions[ver].data)      # Open version data zip
                with zipfile.ZipFile(zfile) as z:
                    for file in files: 
                        if to_zip: 
                            info = z.getinfo(file)
                            if info.file_size > 50000000:   # Extract to disk if file > 50MB
                                with tempfile.TemporaryDirectory() as tmpdir:
                                   z.extract(file, tmpdir)
                                   zfileobj.write(os.path.join(tmpdir, info.filename), info.filename)
                            else: zfileobj.writestr(info, z.read(file))
                        else: z.extract(file, dst)

        logging.info("Restored '{}' > '{}'".format(self.filename, dst))

    
    def trim(self, ver = None, file = None):
        if not ver: version = self.lastver      # Trim to newest version if none specified
        else: version = self.versions[ver]

        if not file: file = self.file
        working = '{}.tempfile'.format(file)        # Temporary file in case something goes wrong

        with tarfile.open(working, 'w') as newtar:
            with tempfile.SpooledTemporaryFile(256000000) as temp:
                # Use restore function to create new data.zip for version
                self.restore(temp, version.id, to_zip=True)
                temp.seek(0)
                tinfo = newtar.gettarinfo(arcname=version.data, fileobj=temp)
                newtar.addfile(tinfo, temp)

            verinfo = version.build_info()              # Convert version info to JSON
            verinfo['sizedelta'] = version.size         # Removing all versions older than specified
                                                        # version, so delta size == size
            for f in version.files.values(): 
                verinfo['files'][f.name] = { 'mod': f.mod, 'size': f.size,
                    'location': version.id }          # Change location to refer to specified version

            taraddstr(newtar, version.info, json.dumps(verinfo,sort_keys=True,indent=4)) # Info JSON
 
            with tarfile.open(self.file) as curtar:
                # Retrieve all newer versions from current backup
                remaining = [ v for v in self.versions.values() if v.time > version.time ]
                bakinfo = curtar.getmember('info.json')
                newtar.addfile(bakinfo, curtar.extractfile(bakinfo))

                for v in sorted(remaining, key=lambda v: v.time):
                    data = curtar.getmember(v.data)
                    newtar.addfile(data, curtar.extractfile(data))
                    verinfo = v.build_info()
                    # If file is located in version older than specified, change location
                    for f in verinfo['files'].values():  
                        if self.versions[f['location']].time < version.time: 
                            f['location'] = version.id  
                    taraddstr(newtar, v.info, json.dumps(verinfo,sort_keys=True,indent=4))

        if os.path.isfile(file): os.remove(file)  
        os.rename(working, file)

        logging.info("Trimmed backup '{}' to version {}".format(
                self.filename, version.id))

    def restorenum(self, num, dst):
        for ver in self.versions.values():
            if ver.num == int(num):
                version = ver
                break
        else: version = None
        if version: self.restore(dst, version.id)
        else: logging.error('Cannot restore - there is no version with the number {}'.format(num))

    def vertrim(self, num = 1, file = None):
        """Trim to NUM most recent versions"""       
        versions = sorted(self.versions.values(), key=lambda v: v.time)     # List from oldest > newest
        if num >= len(versions): return                  # We have fewer versions than specified
        self.trim(versions[len(versions)-num].id, file)

    def autotrim(self, minver = 1, maxver = 5, file = None):
        if len(self.versions) > maxver: self.vertrim(minver, file)

def main():
    args = docopt(__doc__, version='0.1.0')
    bak = Backup(args['<file>'])

    if args['build']:
        bak.build(args['<directory>'])
        bak.save(args['<file>'])
    if args['restore']: 
        if args['--ver']: bak.restore(args['<directory>'], ver=args['--ver'])
        elif args['--num']: bak.restorenum(args['--num'], args['<directory>'])
        else: bak.restore(args['<directory>'])

    if args['trim']: bak.vertrim(int(args['<num>']), args['--output'])

    if args['info']:
        print('Source:', bak.src, '\n')
        table = OrderedDict([('No.', []), ('Time', []), ('Files', []), ('Size', [])])
        width = {}
        space = 2
        for vid, version in sorted(bak.versions.items()):        
            table['No.'].append(str(version.num))
            date = time.localtime(version.time)
            table['Time'].append(time.strftime('%Y/%m/%d %H:%M:%S', date))
            table['Files'].append(str(len(version.files)))
            table['Size'].append(str(round(version.size/1000)))
        for column, data in table.items():
            colwidth = max([len(entry) for entry in data])
            if colwidth < len(column): colwidth = len(column)
            width[column] = colwidth
        print( (' '*space).join([h+' '*(width[h]-len(h)) for h in table]) )
        print( (' '*space).join(['-'*width[h] for h in table]) )
        htext = list(table)
        for row in zip(*table.values()):
            rowtext = ''
            for idx, item in enumerate(row):
                rowtext += '{}{}'.format(item, ' '*(width[htext[idx]]-len(item)+space))
            print(rowtext)       

if __name__ == '__main__':
    main()