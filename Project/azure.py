import os
import sys

from flask import jsonify
from azure.storage.file import FileService

from .logger import config_dic,logger
from .models import DB

if config_dic["Envirolment"] == "Staging" or config_dic["Envirolment"] == "Development":
	account_name = config_dic["StgAccountName"]
	account_key = config_dic["StgAccountKey"]
elif config_dic["Envirolment"] == "Production" : 
	account_name = config_dic["ProAccountName"]
	account_key = config_dic["ProAccountKey"]

def get_azure_keys(envirolment):
	if envirolment == "Staging" or  envirolment == "Development":
		account_name = config_dic["StgAccountName"]
		account_key = config_dic["StgAccountKey"]
	elif envirolment == "Production":
		account_name = config_dic["ProAccountName"]
		account_key = config_dic["ProAccountKey"]
	else:
		account_name = None
		account_key = None
	return (account_name,account_key)

share_name = config_dic["ShareName"]

# -------------------- Azure functions ----------------------------

class AZURE():
	def __init__(self, account_name = account_name, account_key = account_key):
		"""
		Initialization of Azure service object.

		Args:
			account_name (str): Azure account name
			account_key (str): Azure account key
		"""
		self.file_service = None
		self.ACCOUNT_NAME = account_name
		self.ACCOUNT_KEY = account_key
		self.azure_connect()
		self.final_path_list = []
		self.status = True

	def azure_connect(self,account_name = None, account_key = None):
		"""
		creates a FileService object using the storage account name and account key
		"""
		try:
			if account_name == None:
				account_name = self.ACCOUNT_NAME
			if account_key == None:
				account_key = self.ACCOUNT_KEY
			logger.info(f"Making azure service connection to {account_name}")
			self.file_service = FileService(account_name, account_key)
			logger.info(f"Connection established for Azure service")
			
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(azure_connect). Check Logs...')
			logger.warning('Error while connecting: {}'.format(str(e)))
			self.status =False
			return False

	def insert_file_azure(self, share_name, directory_name, file_name,file_data):
		"""
		share_name (str): Name of the share. The share must exist.\n
		directory_name (str): Path of the directory. The directory must exist. \n
		file_name (str): Name of the destination file. If the destination file exists, it will be overwritten. Otherwise, it will be created. \n
		file_data: Content of file as an array of bytes.\n
		description: Creates a new file from an array of bytes, or updates the content of an existing file, with automatic chunking and progress notifications.\n
		"""
		try:
			self.file_service.create_file_from_bytes(share_name, directory_name, file_name, file_data)
			return True
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(insert_file_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def create_directory_azure(self, share_name, directory_name):
		"""
		share_name (str): Name of the share. The share must exist.\n
		directory_name (str): Name of the directory to create.\n
		description: Creates a new directory under the specified share or parent directory. If the directory with the same name already exists, the operation fails on the service.\n
		returns: True if directory is created, False if directory already exists.
		"""
		try:
			status = self.file_service.exists(share_name, directory_name)
			if status == False:
				status = self.file_service.create_directory(share_name, directory_name)
			return status 
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(create_directory_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def list_files_and_dir_azure(self, share_name, dir_name):
		"""
		share_name (str): Name of the share. The share must exist.\n
		directory_name (str): Path of the directory. The directory must exist. \n
		description: Returns a generator to list the directories and files under the specified share. The generator will lazily follow the continuation tokens returned by the service and stop when all directories and files have been returned or num_results is reached.\n
		"""
		try:
			generator = self.file_service.list_directories_and_files(share_name =share_name,directory_name = dir_name)
			dir_list = []
			for file_or_dir in generator:
				dir_list.append(file_or_dir.name)
			return dir_list
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(list_files_and_dir_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def download_file_azure(self, share_name, directory_name, file_name, destination_file_path):
		"""
		share_name (str): Name of the share. The share must exist.\n
		directory_name (str): Path of the directory. The directory must exist. \n
		file_name (str): Name of the file \n
		destination_file_path (str): Path of file to write to \n
		description: Downloads a file to a file path, with automatic chunking and progress notifications. Returns an instance of File with properties and metadata.\n
		resturns (File): A File with properties and metadata. 
		"""
		try:
			destination_file = destination_file_path +'\\'+ file_name
			self.file_service.get_file_to_path(share_name, directory_name, file_name, destination_file)
			return {"status":True}

		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(download_file_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def file_url_in_azure(self, share_name, directory_name, file_name):
		"""
		share_name (str): Name of the share. The share must exist.\n
		directory_name (str): Path of the directory. The directory must exist. \n
		file_name (str): Name of the file.  \n
		returns (str): Url representing file
		description: Creates the url to access a file.
		"""
		try:
			sourcefile = self.file_service.make_file_url(share_name, directory_name, file_name)
			return {"sourcefile":sourcefile}
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(file_url_in_azure). Check Logs...')
			logger.warning('Error: {}'.format(str(e)))
			self.status =False
			return False

	def copy_file_azure(self, share_name, directory_name, file_name, copy_source):
		""" 
		share_name (str): Name of the destination share. The share must exist.\n
		directory_name (str): Path of the destination directory. The directory must exist. \n
		file_name (str): Name of the destination file. If the destination file exists, it will be overwritten. Otherwise, it will be created. \n
		copy_source (str): A URL of up to 2 KB in length that specifies an Azure file or blob. The value should be URL-encoded as it would appear in a request URI. If the source is in another account, the source must either be public or must be authenticated via a shared access signature. If the source is public, no authentication is required.\n
		description: Copies a file asynchronously. This operation returns a copy operation properties object, including a copy ID you can use to check or abort the copy operation. The File service copies files on a best-effort basis.\n
				If the destination file exists, it will be overwritten. The destination file cannot be modified while the copy operation is in progress.
		"""
		try:
			copy = self.file_service.copy_file(share_name, directory_name, file_name, copy_source)
			if(copy.status == 'pending'):
				self.file_service.abort_copy_file(share_name, None, 'file1copy', copy.id)
			return {"status":True}
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(copy_file_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def delete_file_azure(self, share_name, directory_name, file_name):
		"""
		share_name (str): Name of the destination share. The share must exist.\n
		directory_name (str): Path of the destination directory. The directory must exist. \n
		file_name (str): Name of the destination file.\n
		description: Marks the specified file for deletion. The file is later deleted during garbage collection.
		"""
		try:
			self.file_service.delete_file(share_name, directory_name, file_name)
			return {"status":True}
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(delete_file_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def move_file_in_azure(self,share_name, source_directory_name, destination_directory_name, file_name):
		"""
		share_name (str): Name of the destination share. The share must exist.\n
		source_directory_name (str): Name of the source directory. The directory must exist. \n
		destination_directory_name (str):Path of the  destination directory. The directory must exist. \n
		file_name (str): Name of the destination file. If the destination file exists, it will be overwritten. Otherwise, it will be created. \n
		Description : helps to move files from one location to another in azure dir deletes the old file \n
		Output: object consist of status
		"""
		try:
			result = self.file_url_in_azure(share_name, source_directory_name, file_name)
			if "message" in result:
				return result
			
			""" Checking file path exist or not """
			file_status = self.file_service.exists(share_name, destination_directory_name + '/')
			azure_file_path_list = destination_directory_name.split('/')
			""" If not exist create """
			if file_status == False:
				for i in  azure_file_path_list:
					dir_=dir_+i+'/'
					result = self.create_directory_azure(share_name, dir_)
					if result == False:
						return result

			result = self.copy_file_azure(share_name, destination_directory_name, file_name,result["sourcefile"])
			if "message" in result:
				return result

			result = self.delete_file_azure(share_name, source_directory_name, file_name)
			if "message" in result:
				return result
			return True
			
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(move_file_in_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def copy_file_in_azure(self, share_name, source_directory_name,destination_directory_name, file_name,old_file_name=None):
		"""
		share_name (str): Name of the destination share. The share must exist.\n
		source_directory_name (str): Name of the source directory. The directory must exist. \n
		destination_directory_name (str):Path of the  destination directory. The directory must exist. \n
		file_name (str): Name of the destination file. If the destination file exists, it will be overwritten. Otherwise, it will be created. \n
		Description : helps to move files from one location to another in azure dir deletes the old file \n
		Output : object consist of status
		"""
		try:
			if old_file_name != None:
				result = self.file_url_in_azure(share_name, source_directory_name, old_file_name)
			else:
				result = self.file_url_in_azure(share_name, source_directory_name, file_name)

			copy_source = result["sourcefile"]
			result = self.copy_file_azure(share_name, destination_directory_name, file_name,copy_source)
			
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(copy_file_in_azure). Check Logs...')
			logger.warning('Error : {}'.format(str(e)))
			self.status =False
			return False

	def path_file_generator(self, generate_path):
		"""
		generate_path (str): folder structure(path) need to created in Azure storage drives  
		description: Returns a generator file path in azure by creating folders as per path provided , skips if path exist.\n
		"""
		try:
			for file in self.file_service.list_directories_and_files(generate_path):
				self.final_path_list.append(generate_path + '/'+ file.name)
				self.path_file_generator(generate_path + '/' + file.name)
		except :
			pass
		return set(self.final_path_list)

	def path_conf_helper(self, fileformat, userinfo_obj, sourcefilepath):
		"""
		Input : PlanFilePath:file path format,result: Scheduleinfo

		Description : will create a folder structure with the path provided

		Output : returns created path
		"""
		
		try:
			path_format = fileformat.replace('<','').replace('>', '')
			if 'Planner' in userinfo_obj:
				if userinfo_obj['Planner'] :
					try:
						userinfo_obj['Planner'] = userinfo_obj['Planner'][:userinfo_obj['Planner'].index('@')].replace('.', ' ') # if planner is an emali
					except:
						pass
				else:
					userinfo_obj['Planner'] = 'Sample_User'
			if 'Brand' in userinfo_obj:
				userinfo_obj['Brand'] = userinfo_obj['Brand'].replace('/',' or')
			for placeholder in userinfo_obj :
				if path_format.find(placeholder) != -1:
					path_format = path_format.replace(str(placeholder),str(userinfo_obj[placeholder]))
				else:
					pass
			final_file_path = sourcefilepath+path_format
			azure_file_path = final_file_path.replace('\\','/')
			azure_file_path_list = list(azure_file_path.split('/'))
			
			dir_ = ''
			file_status = self.file_service.exists(share_name, azure_file_path)
			file_status = False
			if file_status == False:
				for i in  azure_file_path_list:
					dir_=dir_+i+'/'
					result = self.create_directory_azure(share_name, dir_)
					if result == False:
						return result
			return final_file_path
			
		except Exception as e:
			logger.warning(f'Something went wrong while connecting to Azure(path_conf_helper). Check Logs...')
			logger.warning('Error while connecting: {}'.format(str(e)))
			self.status =False
			return False
							
# ------------------- Azure Supporting Functions ----------------------------

def getListOfFiles(dirName):
	"""
	Input : dir name

	Description : helps to list all files in given dir name 

	Output : list consist of file names 
	"""

	data=locals()
	try:
		listOfFile = os.listdir(dirName)
		allFiles = list()
		for entry in listOfFile:
			fullPath = os.path.join(dirName, entry)
			if os.path.isdir(fullPath):
				allFiles = allFiles + getListOfFiles(fullPath)
			else:
				allFiles.append(fullPath)			
		return allFiles
	
	except Exception as e:
		logger.exception(sys._getframe().f_code.co_name,os.path.basename(__file__),data,Error=e)
		return({"message":"fail"})
		
def create_directory_local(folder_path):
	"""
	Input : folder path

	Description : will create a folder structure in local system 

	Output : created folder path
	"""
	data=locals()
	try:
		if not os.path.exists(folder_path):  
				os.makedirs(folder_path)
		return folder_path

	except Exception as e:
		logger.exception(sys._getframe().f_code.co_name,os.path.basename(__file__),data,Error=e)
		return({"message":"fail"})
