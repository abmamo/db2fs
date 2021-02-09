# io / fs
import os
import glob
import shutil
from pathlib import Path
# logging
import logging
# config logger
logger = logging.getLogger(__name__)


class IOFunctions:
    """
        common IO functions
    """
    def get_dir_files(self, dir_path, file_type=None):
        """
            get all files in a directory. if file_type
            specified will only get files with that
            extension

            params:
                - dir_path: path to directory
                - file_type: file types to search for e.g. csv, json
            returns:
                - list of paths or empty list
        """
        if file_type is None:
            # return all files
            return glob.glob(os.path.join(dir_path, "*.*"))
        else:
            # return filtered files
            return glob.glob(os.path.join(dir_path, "*" + file_type))

    def get_filename(self, file_path):
        """
            get filename from a given file path

            params:
                - file_path: location of file on disk
            returns:
                - name of file
        """
        # get name from filename
        return Path(file_path).name

    def remove_file(self, file_path):
        """
            remove file

            params:
                - file_path: location of file on disk
        """
        # remote temp csv file
        os.remove(file_path)

    def create_dir(self, dir_name):
        """
            create a local directory if it doesn't exist

            params:
                - dir_path: path of directory
        """
        # get base dir of current file
        current_dir = Path(__file__).parent.absolute()
        # get dir path
        created_dir = os.path.join(current_dir, dir_name)
        # check if dir exists
        if not os.path.exists(created_dir):
            # if it doesn't make it
            os.makedirs(created_dir)
        # return path of created dir
        return created_dir

    def empty_dir(self, dir_path):
        """
            deletes all files in a given directory but not
            the directory itself

            params:
                - dir_path: path of directory
        """
        # iteratively get all files in dir
        for filename in os.listdir(dir_path):
            # get file path for current file
            file_path = os.path.join(dir_path, filename)
            # if it's file
            if os.path.isfile(file_path) or os.path.islink(file_path):
                # remove file
                os.unlink(file_path)
            # if it's dir
            elif os.path.isdir(file_path):
                # remove dir
                shutil.rmtree(file_path)

    def delete_dir(self, dir_path):
        """
            delete directory entirely

            params:
                - dir_path: path of directory
        """
        shutil.rmtree(dir_path)
