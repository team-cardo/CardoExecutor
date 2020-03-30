import os
import re

IGNORE_DIRS = ['\\.git', '\\.idea']
EXTENSIONS_TO_WATCH = ['py', 'json']


class DirectoryUpdateWatcher(object):
	def __get_files_in_dir(self, root_dir, ignore_dirs=IGNORE_DIRS, extensions=EXTENSIONS_TO_WATCH):
		files_watchlist = []
		file_extensions_pattern = '.*\\.({})$'.format('|'.join(extensions))
		ignore_dirs_pattern = '/({})/'.format('|'.join(ignore_dirs))
		for root, directories, filenames in os.walk(root_dir):
			full_paths = self.__get_full_pathes(root, filenames, file_extensions_pattern, ignore_dirs_pattern)
			files_watchlist.extend(full_paths)
		return files_watchlist

	def __get_full_pathes(self, root, filenames, file_extensions_pattern, ignore_dirs_pattern):
		files = []
		if re.search(ignore_dirs_pattern, root) is None:
			for filename in filenames:
				if re.match(file_extensions_pattern, filename) is not None:
					files.append(os.path.join(root, filename))
		return files

	def get_dir_modified_time(self, root_dir):
		files = self.__get_files_in_dir(os.getcwd())
		files_modified_time = [os.path.getmtime(filename) for filename in files]
		last_modified_time = max(files_modified_time)
		return last_modified_time
