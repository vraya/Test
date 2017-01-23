#!/usr/bin/env python

# Nagios Log Server log archive shipper assistant.
# Copyright 2014, Nagios Enterprises LLC.
# Released under the terms of the Nagios Software License:
# <https://assets.nagios.com/licenses/nagios_software_license.txt>
#
# Write log message lines read from stdin to stdout as JSON for import
# into Nagios Log Server (Logstash).

# encoding: utf-8

import bz2
import fnmatch
import glob
import gzip
import logging
import optparse
import os
import sys
import tarfile
import zipfile

try:
    import json
except ImportError:
    import simplejson as json


logging.basicConfig(level=logging.DEBUG)
#logging.basicConfig(level=logging.INFO, format='%(message)s')



def process_args():
    """Parse the input args of the script. """

    # Create and configure our argv parser.
    parser = optparse.OptionParser(version="%prog 1.0.0",
        usage="Usage: %prog [options] [field1:value1 ... ]",
        description="Encode log lines as JSON messages for import to Nagios Log Server.")

    # Add an option group and some options...
    fog = optparse.OptionGroup(parser, "File Selection",
        "Use these to select the input files to process. If no input files are specified with the -f, -d or -a options, or - is given as the argument to the -f option, input will be read from stdin.")
    fog.add_option("-f", metavar="FILE",
        help="Specify log file(s) to process. Plain text and gzip or bzip2 compressed files are supported. Multiple files can be specified with a shell glob pattern. Pass - to read from stdin.")
    fog.add_option("-d", metavar="DIR",
        help="Recursively process files in the given directory.")
    fog.add_option("-a", metavar="ARCHIVE",
        help="Process files in the given archive. Supported formats include: zip, uncompressed tar, and gzip or bzip2 compressed tar archives.")
    fog.add_option('-p', metavar="PATTERN",
        help="Only process file names that match this pattern when processing a directory or archive. Required with -d and -a options.")
    parser.add_option_group(fog)


    # Process our argv command line arguments.
    opts, args = parser.parse_args()

    logging.debug(opts)
    logging.debug(args)

    # Validate our file and directory options.
    if opts.f is not None:
        if opts.d is not None:
            parser.error("-f and -d options can not be used together.")
        if opts.a is not None:
            parser.error("-f and -a options can not be used together.")
        if not opts.f:
            parser.error("-f option requires a value.")

    if opts.d is not None:
        if opts.a is not None:
            parser.error("-d and -a options can not be used together.")
        if not opts.d:
            parser.error("-d option requires a value.")
        if not os.path.isdir(opts.d + '/.'):
            parser.error("'%s' is not an accessible directory." % opts.d)

    if opts.a is not None:
        if not opts.a:
            parser.error("-a option requires a value.")
        if not os.path.isfile(opts.a):
            parser.error("'%s' is not an accessible file." % opts.a)

    if opts.p is not None:
        if not opts.d and not opts.a:
            parser.error("-p option only makes sense with -d or -a options.")
        if not opts.p:
            parser.error("-p option requires a value.")
    # The 'null pattern' matches all files.
    if not opts.p and (opts.d or opts.a):
        parser.error("A pattern must be given with the -p option when processing directories or archives.")

    # Split the list of positional parameters after our option arguments.
    # These must be 'key:value' pairs to add to the JSON.
    args = [a.split(':', 1) for a in args]
    for a in args:
        if len(a) is not 2:
            parser.error("Argument '%s' is not a 'key:value' pair." % a[0])

    return opts, args



def find_files(directory, pattern):
    for root, directories, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename


def process_files(files, message):
    for f in files: process_file(f, message)


def process_file(path, message):
    s = None
    try:
        try:
            # These tests really shuold be done with magic (the file type DB).
            if path.lower().endswith(('.tar', '.tgz', '.tar.gz', '.tar.bz2', '.zip')):
                logging.info("Skipping archive file: '%s'" % path)

            elif fnmatch.fnmatch(path, '*.gz'):
                s = gzip.open(path, 'r')
                logging.info("Processing gzip file: '%s'" % path)

            elif fnmatch.fnmatch(path, '*.bz2'):
                s = bz2.BZ2File(path, 'r')
                logging.info("Processing bzip2 file: '%s'" % path)

            else:
                s = open(path, 'r')
                logging.info("Processing regular file: '%s'" % path)

            if s:
                process_stream(s, message)

        except UnicodeDecodeError, e:
            # We can't reliably continue on in the face of encoding errors.
            logging.info(e)
        except EnvironmentError, e:
            if e.strerror and e.filename: logging.info("%s: '%s'" % (e.strerror, e.filename))
            elif e.strerror: logging.info(e.strerror)
            else: logging.info(e)
        except Exception, e:
            logging.info(e)

    finally:
        if s: s.close()


def process_archive(path, pattern, message):
    a = None
    try:
        try:
            a = tarfile.open(path, 'r')
            logging.info("Processing tar archive: '%s'" % path)
            return process_tar(a, pattern, message)
        except tarfile.TarError, e:
            logging.debug('tar: ' + str(e))
    finally:
        if a: a.close()

    try:
        try:
            a = zipfile.ZipFile(path, 'r')
            logging.info("Processing zip archive: '%s'" % path)
            return process_zip(a, pattern, message)
        except Exception, e:
            logging.debug('zip: ' + str(e))
    finally:
        if a: a.close()

    logging.info("Unable to open archive: '%s'" % path)


def process_tar(t, pattern, message):
    try:
        for i in t.getmembers():
            if i.isfile() and fnmatch.fnmatch(i.name, pattern):
                logging.debug(i.name)
                s = t.extractfile(i)
                try:
                    try:
                        process_stream(s, message)
                    except UnicodeDecodeError, e:
                        logging.info(e)
                finally:
                    s.close()
    except Exception, e:
        logging.info(e)


def process_zip(z, pattern, message):
    try:
        for i in z.infolist():
            # Names with trailing slashes are directories...
            # ...until we get to Windows...
            if not i.filename.endswith('/') and fnmatch.fnmatch(i.filename, pattern):
                logging.debug(i.filename)
                s = z.open(i)
                try:
                    try:
                        process_stream(s, message)
                    except UnicodeDecodeError, e:
                        logging.info(e)
                finally:
                    s.close()
    except Exception, e:
        logging.info(e)


def process_stream(stream, message):
    for line in stream:
        message['message'] = line
        print json.dumps(message)



def main():
    opts, args = process_args()

    message = dict(args)

    if opts.f and opts.f is not '-':
        # Process files matching the glob pattern.
        process_files(glob.iglob(opts.f), message)
    elif opts.d:
        # Recursively process all files that match the name pattern.
        process_files(find_files(opts.d, opts.p), message)
    elif opts.a:
        # Process all matching files in the given archive.
        process_archive(opts.a, opts.p, message)
    else:
        # Otherwise we process log lines from stdin.
        process_stream(sys.stdin, message)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        logging.exception(e)
