#!/usr/bin/env python

import ConfigParser
import datetime
import gnupg
import hashlib
import json
import os.path
import requests
import shutil
import sys
import tarfile

from subprocess import call

# Globals
config = ConfigParser.SafeConfigParser()
config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uagent.conf'))
install_root = config.get('envs', 'install_root')
platform = config.get('envs', 'platform')
gnupg_home = os.path.join(install_root, '.gnupg')
download_path = os.path.join(install_root, 'download')
unpack_path = os.path.join(download_path, 'unpack')


class ExitCode:
    OK = 0

    USAGE = 100
    WARN_VERSION_SAME = 101
    WARN_OLD_VERSION_NOT_IN_RANGE = 102

    ERR_OLD_VERSION_MISSING = 1000
    ERR_NEW_VERSION_MISSING = 1001
    ERR_PARSING_OLD_VERSION = 1002
    ERR_PARSING_NEW_VERSION = 1003
    ERR_BAD_CHECKSUM = 1004
    ERR_UNPACK_TARFILE = 1005
    ERR_INSTALL_SCRIPT_MISSING = 1006
    ERR_INSTALL_SCRIPT_FAILED = 1007
    ERR_PACKAGE_FILE_MISSING = 1008
    ERR_BAD_SIGNATURE = 1009
    ERR_DOWNLOAD_FAILED = 1010
    ERR_PACKAGE_FILE_TOO_BIG = 1011
    ERR_BAD_SERVER_URL = 1012


class VersionNumber:
    """ This class represents a version number of the form <major>.<minor>.<patch>.<build> """

    def __init__(self, vstring):
        self._major = 0
        self._minor = 0
        self._patch = 0
        self._build = 0

        v = vstring.split('.')
        if len(v) > 0 and v[0]:
            self._major = int(v[0])
        if len(v) > 1 and v[1]:
            self._minor = int(v[1])
        else:
            self._minor = sys.maxsize
        if len(v) > 2 and v[2]:
            self._patch = int(v[2])
        else:
            self._patch = sys.maxsize
        if len(v) > 3 and v[3]:
            self._build = int(v[3])
        else:
            self._build = sys.maxsize

    def _get_major(self):
        return self._major

    def _set_major(self, value):
        if not isinstance(value, int):
            raise TypeError("major must be an integer")
        self._major = value

    def _get_minor(self):
        return self._minor

    def _set_minor(self, value):
        if not isinstance(value, int):
            raise TypeError("minor must be an integer")
        self._minor = value

    def _get_patch(self):
        return self._patch

    def _set_patch(self, value):
        if not isinstance(value, int):
            raise TypeError("patch must be an integer")
        self._patch = value

    def _get_build(self):
        return self._build

    def _set_build(self, value):
        if not isinstance(value, int):
            raise TypeError("build must be an integer")
        self._build = value

    major = property(_get_major, _set_major)
    minor = property(_get_minor, _set_minor)
    patch = property(_get_patch, _set_patch)
    build = property(_get_build, _set_build)

    def max_version_number(self):
        mv = VersionNumber('')
        mv._major = sys.maxsize
        mv._minor = sys.maxsize
        mv._patch = sys.maxsize
        mv._build = sys.maxsize
        return mv

    def in_range(self, v_range):
        r = v_range.split('-')
        f = VersionNumber(r[0])
        t = VersionNumber(r[1])
        return f <= self < t

    def beyond_range(self, v_range):
        r = v_range.split('-')
        t = VersionNumber(r[1])
        return self >= t

    def __str__(self):
        return '{0._major}.{0._minor}.{0._patch}.{0._build}'.format(self)

    # comparison operators
    def cmp(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        if self._major != other._major:
            return self._major - other._major
        elif self._minor != other._minor:
            return self._minor - other._minor
        elif self._patch != other._patch:
            return self._patch - other._patch
        else:
            return self._build - other._build

    def __gt__(self, other):
        return self.cmp(other) > 0

    def __lt__(self, other):
        return self.cmp(other) < 0

    def __eq__(self, other):
        return self.cmp(other) == 0

    def __ne__(self, other):
        return self.cmp(other) != 0

    def __ge__(self, other):
        return self.cmp(other) >= 0

    def __le__(self, other):
        return self.cmp(other) <= 0


class VersionFile:
    """ This class represents a version.json file. """

    def __init__(self, filename, logger):
        self._logger = logger
        try:
            with open(filename, 'r') as fh:
                version_json = json.loads(fh.read(int(config.get('limits', 'max_version_file_size'))))
                if version_json.get('version'):
                    self._version = VersionNumber(version_json.get('version'))
                if version_json.get('releaseDateUTC'):
                    self._release_date_utc = version_json.get('releaseDateUTC')
                if version_json.get('platform'):
                    self._platform = version_json.get('platform')
                if version_json.get('checksum'):
                    self._checksum = version_json.get('checksum')
                if version_json.get('upgradeFrom'):
                    r = version_json.get('upgradeFrom').split('-')
                    self._upgrade_min = VersionNumber(r[0])
                    self._upgrade_max = VersionNumber(r[1])
                if version_json.get('packageFile'):
                    self._package_file = version_json.get('packageFile')
        except ValueError:
            self._logger.log_exception("Corrupted file %s", filename)

    def _get_version(self):
        return self._version

    def _set_version(self, value):
        if not isinstance(value, VersionNumber):
            raise TypeError("version must be a VersionNumber")
        self._version = value

    def _get_release_date_utc(self):
        return self._release_date_utc

    def _set_release_date_utc(self, value):
        if not isinstance(value, str):
            raise TypeError("release_date_utc must be a str")
        self._release_date_utc = value

    def _get_platform(self):
        return self._platform

    def _set_platform(self, value):
        if not isinstance(value, str):
            raise TypeError("platform must be a str")
        self._platform = value

    def _get_checksum(self):
        return self._checksum

    def _set_checksum(self, value):
        if not isinstance(value, str):
            raise TypeError("checksum must be a str")
        self._checksum = value

    def _get_upgrade_min(self):
        return self._upgrade_min

    def _set_upgrade_min(self, value):
        if not isinstance(value, VersionNumber):
            raise TypeError("upgrade_min must be a version_number")
        self._upgrade_min = value

    def _get_upgrade_max(self):
        return self._upgrade_max

    def _set_upgrade_max(self, value):
        if not isinstance(value, VersionNumber):
            raise TypeError("upgrade_max must be a version_number")
        self._upgrade_max = value

    def _get_package_file(self):
        return self._package_file

    def _set_package_file(self, value):
        if not isinstance(value, str):
            raise TypeError("package_file must be a str")
        self._package_file = value

    version = property(_get_version, _set_version)
    release_date_utc = property(_get_release_date_utc, _set_release_date_utc)
    platform = property(_get_platform, _set_platform)
    checksum = property(_get_checksum, _set_checksum)
    upgrade_min = property(_get_upgrade_min, _set_upgrade_min)
    upgrade_max = property(_get_upgrade_max, _set_upgrade_max)
    package_file = property(_get_package_file, _set_package_file)

    def validate_attributes(self):
        return (hasattr(self, 'version') and
                hasattr(self, 'platform') and
                hasattr(self, 'checksum') and
                hasattr(self, 'upgrade_min') and
                hasattr(self, 'upgrade_max') and
                hasattr(self, 'package_file') and
                platform == self._platform)

    def validate_version_range(self):
        return self._upgrade_max <= self._version

    def validate_package_file(self):
        # Validate checksum
        pkg_file = os.path.join(download_path, self._package_file)
        if not os.path.isfile(pkg_file):
            return ExitCode.ERR_PACKAGE_FILE_MISSING
        actual_chksum = calc_checksum(pkg_file)
        if actual_chksum != self._checksum:
            return ExitCode.ERR_BAD_CHECKSUM
        # Validate signature
        if not verify_file(pkg_file, self._logger):
            return ExitCode.ERR_BAD_SIGNATURE
        return ExitCode.OK


def calc_checksum(filename, blocksize=65536):
    """ Calculate the checksum of the given file. """
    hasher = hashlib.sha256()
    with open(filename, 'rb') as fh:
        buf = fh.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = fh.read(blocksize)
        return hasher.hexdigest()


def verify_file(filename, logger):
    """ Verify the signature of the given file. We assume the signature file name
        is always in the form of <filename>.sig, under the same directory. """
    if not os.path.isdir(gnupg_home):
        logger.log_error("%s is not a directory", gnupg_home)
        return False
    keyring = os.path.join(gnupg_home, "pubring.gpg")
    if not os.path.isfile(keyring):
        logger.log_error("%s is not a file", keyring)
        return False
    sig_file = filename + ".sig"
    if not os.path.isfile(sig_file):
        logger.log_error("%s is not a file", sig_file)
        return False
    gpg = gnupg.GPG(gnupghome=gnupg_home, keyring=keyring)
    with open(sig_file, "rb") as fh:
        verified = gpg.verify_file(fh, filename)
    return verified.valid and verified.trust_level >= verified.TRUST_FULLY


class UAgent:
    def __init__(self, logger):
        self._logger = NullableLogger(logger)

    def download_file(self, url, limit):
        """ Download the file located at 'url'. The file size cannot be bigger than 'limit'. """

        # 'url' has to be HTTPS
        if not url.lower().startswith('https://'):
            return ExitCode.ERR_BAD_SERVER_URL

        # Download the header first, to make sure the size is reasonable
        #certs = os.path.join(gnupg_home, "server-certs.pem")
        #if not os.path.isfile(certs):
        #    self._logger.log_error("%s is not a file.", certs)
        #    return ExitCode.ERR_DOWNLOAD_FAILED
        components = url.split('/')
        filename = components[len(components) - 1]
        filename = os.path.join(download_path, filename)

        try:
            # make sure 'download_path' exists
            if not os.path.isdir(download_path):
                os.makedirs(download_path)

            # get header to check the size of the file
            #response = requests.head(url, verify=certs, stream=True, allow_redirects=True)
            response = requests.head(url, stream=True, allow_redirects=False)
            if response.status_code != 200:
                self._logger.log_error("requests.head: url=%s, status=%d", url, response.status_code)
                return ExitCode.ERR_DOWNLOAD_FAILED
            if ('Content-Length' in response.headers) and (int(response.headers['Content-Length']) > int(limit)):
                return ExitCode.ERR_PACKAGE_FILE_TOO_BIG

            # now download the file itself
            #response = requests.get(url, verify=certs, stream=True, allow_redirects=True)
            response = requests.get(url, stream=True, allow_redirects=False)
            if response.status_code != 200:
                self._logger.log_error("requests.get: url=%s, status=%d", url, response.status_code)
                return ExitCode.ERR_DOWNLOAD_FAILED

            # delete the file if already exist
            if os.path.isfile(filename):
                os.remove(filename)

            # save the downloaded file
            with open(filename, "wb") as fh:
                response.raw.decode_content = True
                total_size = 0
                chunk_size = 4096
                for chunk in response.iter_content(chunk_size):
                    fh.write(chunk)
                    total_size += chunk_size
                    if total_size > limit:
                        break
        except:
            self._logger.log_exception("failed to download file from %s, limit %s", url, limit)
            return ExitCode.ERR_DOWNLOAD_FAILED
        return ExitCode.OK

    def unpack_package_file(self, filename, dest_dir):
        """ Unpack the package file (.tar.gz) into the 'dest_dir' folder. """
        try:
            if os.path.isdir(dest_dir):
                self.clear_directory(dest_dir)
            else:
                os.makedirs(dest_dir)
            with tarfile.open(filename) as tar:
                tar.extractall(dest_dir)
        except Exception as ex:
            template = "An exception of type {0} occured. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            return ExitCode.ERR_UNPACK_TARFILE
        return ExitCode.OK

    def run_install_script(self, script):
        """ Run the install.py script that's in the package we just downloaded.
            Pass the install_root as argument to the script. """
        if not os.path.isfile(script):
            return ExitCode.ERR_INSTALL_SCRIPT_MISSING
        try:
            call([script, install_root], cwd=unpack_path)
        except:
            return ExitCode.ERR_INSTALL_SCRIPT_FAILED
        return ExitCode.OK

    def is_offhour(self):
        """ Off hour is defined as either weekends, or evening of weekdays. """
        day = datetime.datetime.today().weekday()
        if day >= 5:
            return True
        time = datetime.datetime.now().time()
        if ((time.hour < int(config.get('limits', 'offhour_morning'))) or
                (time.hour > int(config.get('limits', 'offhour_evening')))):
            return True
        return False

    def clear_directory(self, directory):
        """ Remove everything under the given directory. """
        for f in os.listdir(directory):
            fullpath = os.path.join(directory, f)
            if os.path.isfile(fullpath):
                os.remove(fullpath)
            else:
                shutil.rmtree(fullpath)

    def upgrade_once(self, url):
        """ Try to download agents from the given place and then upgrade. """

        if os.path.isdir(download_path):
            self.clear_directory(download_path)

        # Download version.json from server
        exit_code = self.download_file(url + "/version.json", config.get('limits', 'max_version_file_size'))
        if exit_code != ExitCode.OK:
            return exit_code

        # Download version.json.sig from server
        exit_code = self.download_file(url + "/version.json.sig", config.get('limits', 'max_signature_file_size'))
        if exit_code != ExitCode.OK:
            return exit_code

        old_version_filename = os.path.join(install_root, "version.json")
        new_version_filename = os.path.join(download_path, "version.json")

        # Do old and new version.json exist?
        if not os.path.isfile(old_version_filename):
            return ExitCode.ERR_OLD_VERSION_MISSING
        if not os.path.isfile(new_version_filename):
            return ExitCode.ERR_NEW_VERSION_MISSING

        # Verify signature of version.json
        if not verify_file(new_version_filename, self._logger):
            return ExitCode.ERR_BAD_SIGNATURE

        # Parse old and new version.json files.
        old_version_file = VersionFile(old_version_filename, self._logger)
        new_version_file = VersionFile(new_version_filename, self._logger)

        # Are old and new version.json valid?
        if not old_version_file.validate_attributes():
            return ExitCode.ERR_PARSING_OLD_VERSION
        if ((not new_version_file.validate_attributes()) or
                (not new_version_file.validate_version_range())):
            return ExitCode.ERR_PARSING_NEW_VERSION

        # Are old and new versions the same?
        if old_version_file.version >= new_version_file.version:
            return ExitCode.WARN_VERSION_SAME

        # See if old version falls in the 'upgradeFrom' range.
        if ((old_version_file.version < new_version_file.upgrade_min) or
                (new_version_file.upgrade_max <= old_version_file.version)):
            return ExitCode.WARN_OLD_VERSION_NOT_IN_RANGE

        # Download package.tar.gz from server
        pkg_file = new_version_file.package_file
        exit_code = self.download_file(url + "/" + pkg_file, config.get('limits', 'max_package_file_size'))
        if exit_code != ExitCode.OK:
            return exit_code

        # Download package.tar.gz.sig from server
        exit_code = self.download_file(url + "/" + pkg_file + ".sig", config.get('limits', 'max_signature_file_size'))
        if exit_code != ExitCode.OK:
            return exit_code

        # Validate new package file checksum.
        exit_code = new_version_file.validate_package_file()
        if exit_code != ExitCode.OK:
            return exit_code

        # Verify signature of the package file
        pkg_full_path = os.path.join(download_path, pkg_file)
        if not verify_file(pkg_full_path, self._logger):
            return ExitCode.ERR_BAD_SIGNATURE

        # Unpack and run install.py
        exit_code = self.unpack_package_file(pkg_full_path, unpack_path)
        if exit_code != ExitCode.OK:
            return exit_code

        exit_code = self.run_install_script(os.path.join(unpack_path, 'install.py'))
        if exit_code != ExitCode.OK:
            return exit_code

        # override old version.json with new version.json
        shutil.copyfile(new_version_filename, old_version_filename)

        return ExitCode.OK

    def run(self):
        """ Try to download updates from 3 different locations on the server.
                1. singles/<client-id>;
                2. offhour
                3. latest
            The script will quit upon finding a valid update and will not look in other locations.
        """

        base_urls = config.get("urls", "server_base")
        client_id = config.get('envs', 'client_id')

        for base_url in base_urls.split(','):
            # Try client specific URL
            client_specific_url = base_url + "/singles/" + client_id + "/" + platform
            self._logger.log_info("try client specific url %s", client_specific_url)
            exit_code = self.upgrade_once(client_specific_url)
            self._logger.log_info("finish connect to client specific url %s, return code %d", client_specific_url, exit_code)
            if exit_code == ExitCode.OK:
                return ExitCode.OK

            # Try off hour URL
            if self.is_offhour():
                offhour_url = base_url + "/offhour/" + platform
                self._logger.log_info("try offhour url %s", offhour_url)
                exit_code = self.upgrade_once(offhour_url)
                self._logger.log_info("finish connect to offhour url %s, return code %d", offhour_url, exit_code)
                if exit_code == ExitCode.OK:
                    return ExitCode.OK

            # Try latest URL
            latest_url = base_url + "/latest/" + platform
            self._logger.log_info("try latest url %s", latest_url)
            exit_code = self.upgrade_once(latest_url)
            self._logger.log_info("finish connect to latest url %s, return code %d", latest_url, exit_code)
            if exit_code == ExitCode.OK:
                return ExitCode.OK

        return ExitCode.ERR_DOWNLOAD_FAILED


class NullableLogger:
    def __init__(self, logger):
        self._logger_writter = logger

    def log_info(self, msg, *args, **kwargs):
        if self._logger_writter:
            self._logger_writter.info(msg+"\n", *args, **kwargs)
        else:
            sys.stdout.write(("INFO: " + msg + "\n") % args)

    def log_error(self, msg, *args, **kwargs):
        if self._logger_writter:
            self._logger_writter.error(msg+"\n", *args, **kwargs)
        else:
            sys.stderr.write(("ERROR: " + msg + "\n") % args)

    def log_exception(self, msg, *args, **kwargs):
        if self._logger_writter:
            self._logger_writter.exception(msg+"\n", *args, **kwargs)
        else:
            sys.stderr.write(("ERROR: " + msg + "\n") % args)

if __name__ == "__main__":
    uagent = UAgent(None)
    return_code = uagent.run()
    sys.exit(return_code)
